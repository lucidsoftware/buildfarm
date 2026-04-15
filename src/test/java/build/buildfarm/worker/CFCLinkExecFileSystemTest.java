// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.CASFileCache.Entry;
import build.buildfarm.cas.cfc.DirectoryEntryCFC;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.util.LinkedInputExclusions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CFCLinkExecFileSystemTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);
  private static final Directory EMPTY_DIR = Directory.getDefaultInstance();
  private static final Digest EMPTY_DIR_DIGEST = DIGEST_UTIL.compute(EMPTY_DIR);
  private static final build.bazel.remote.execution.v2.Digest EMPTY_DIR_DIGEST_V2 =
      DigestUtil.toDigest(EMPTY_DIR_DIGEST);

  private Path root;
  private CASFileCache fileCache;
  private CFCLinkExecFileSystem execFileSystem;
  private ExecutorService service;

  @Before
  public void setUp() throws Exception {
    root =
        Iterables.getFirst(
            Jimfs.newFileSystem(
                    Configuration.unix().toBuilder()
                        .setAttributeViews("basic", "owner", "posix", "unix")
                        .build())
                .getRootDirectories(),
            null);

    Path cacheRoot = root.resolve("cache");
    Path execRoot = root.resolve("exec");
    Files.createDirectories(cacheRoot);
    Files.createDirectories(execRoot);

    service = newSingleThreadExecutor();

    ContentAddressableStorage delegate = mock(ContentAddressableStorage.class);
    when(delegate.getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(new NullWrite());
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(delegate)
        .findMissingBlobs(any(Iterable.class), any(DigestFunction.Value.class));

    ConcurrentMap<String, Entry> storage = Maps.newConcurrentMap();
    fileCache =
        new DirectoryEntryCFC(
            cacheRoot,
            /* maxSizeInBytes= */ 1024 * 1024,
            /* maxEntrySizeInBytes= */ 1024 * 1024,
            /* hexBucketLevels= */ 0,
            service,
            /* accessRecorder= */ directExecutor(),
            storage,
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> ByteString.EMPTY.newInput());
    fileCache.initializeRootDirectory();

    execFileSystem =
        new CFCLinkExecFileSystem(
            execRoot,
            fileCache,
            ImmutableMap.of(),
            /* linkInputDirectories= */ true,
            ImmutableList.of(), // deprecated — LinkedInputExclusions replaces regex patterns
            /* linkedInputExclusionPatterns= */ ImmutableList.of(),
            /* allowSymlinkTargetAbsolute= */ false,
            service,
            /* accessRecorder= */ service,
            /* fetchService= */ service);
    // start() sets fileStore (needed for destroyExecDir) and initializes the cache
    execFileSystem.start(digests -> {}, /* skipLoad= */ true, /* writable= */ true).get();
  }

  @After
  public void tearDown() throws Exception {
    shutdownAndAwaitTermination(service, 1, SECONDS);
    // Clean up children of root, not root itself (Jimfs can't delete /)
    try (var stream = Files.list(root)) {
      var fileStore = Files.getFileStore(root);
      stream.forEach(
          child -> {
            try {
              Directories.remove(child, fileStore);
            } catch (Exception e) {
              // swallow — a failed cleanup shouldn't mask a real assertion failure
            }
          });
    }
  }

  private Path runCreateExecDir(
      String operationName,
      Digest inputRootDigest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Command command)
      throws Exception {
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(inputRootDigest)).build();
    return execFileSystem.createExecDir(
        operationName,
        directoriesIndex,
        DigestFunction.Value.SHA256,
        action,
        command,
        /* owner= */ null,
        WorkerExecutedMetadata.newBuilder());
  }

  /**
   * Builds a standalone {@link CFCLinkExecFileSystem} backed by its own {@link DirectoryEntryCFC},
   * rooted under unique {@code cache-<name>} / {@code exec-<name>} directories. Used by tests that
   * need a configuration the shared {@code @Before} fixture doesn't provide (a different {@code
   * linkInputDirectories} flag or non-empty exclusion patterns).
   */
  private CFCLinkExecFileSystem newLinkExecFileSystem(
      String name, boolean linkInputDirectories, ImmutableList<String> linkedInputExclusionPatterns)
      throws Exception {
    Path cacheRoot = root.resolve("cache-" + name);
    Path execRoot = root.resolve("exec-" + name);
    Files.createDirectories(cacheRoot);
    Files.createDirectories(execRoot);

    ContentAddressableStorage delegate = mock(ContentAddressableStorage.class);
    when(delegate.getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(new NullWrite());
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(delegate)
        .findMissingBlobs(any(Iterable.class), any(DigestFunction.Value.class));

    CASFileCache fileCache =
        new DirectoryEntryCFC(
            cacheRoot,
            /* maxSizeInBytes= */ 1024 * 1024,
            /* maxEntrySizeInBytes= */ 1024 * 1024,
            /* hexBucketLevels= */ 0,
            service,
            /* accessRecorder= */ directExecutor(),
            Maps.newConcurrentMap(),
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> ByteString.EMPTY.newInput());
    fileCache.initializeRootDirectory();

    CFCLinkExecFileSystem fileSystem =
        new CFCLinkExecFileSystem(
            execRoot,
            fileCache,
            ImmutableMap.of(),
            linkInputDirectories,
            /* linkedInputDirectories= */ ImmutableList.of(),
            linkedInputExclusionPatterns,
            /* allowSymlinkTargetAbsolute= */ false,
            service,
            /* accessRecorder= */ service,
            /* fetchService= */ service);
    fileSystem.start(digests -> {}, /* skipLoad= */ true, /* writable= */ true).get();
    return fileSystem;
  }

  /**
   * Verifies that directories listed in output_paths are not symlinked to the read-only CAS cache.
   * Output path ancestors are in the linkedInputExclusions set, so they must be created as real
   * writable directories.
   */
  @Test
  public void outputPathDirectoryIsNotSymlinked() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("outdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir, EMPTY_DIR_DIGEST_V2, EMPTY_DIR);

    Command command = Command.newBuilder().addOutputPaths("outdir").build();
    Path execDir = runCreateExecDir("test-op", rootDirDigest, directoriesIndex, command);

    try {
      Path outdir = execDir.resolve("outdir");
      assertThat(Files.exists(outdir)).isTrue();
      assertThat(Files.isSymbolicLink(outdir)).isFalse();
      assertThat(Files.isDirectory(outdir)).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  /**
   * Verifies that a declared {@code output_paths} directory keeps existing descendants as real
   * directories too. The server does not know whether an {@code output_paths} entry is a file or
   * directory before execution, so the path must be treated as a recursive writable root while
   * creating the exec tree.
   */
  @Test
  public void outputPathDirectoryDescendantsAreNotSymlinked() throws Exception {
    Directory outDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("existing").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest outDirDigest = DIGEST_UTIL.compute(outDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("outdir")
                    .setDigest(DigestUtil.toDigest(outDirDigest)))
            .addDirectories(
                DirectoryNode.newBuilder().setName("inputdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest),
            rootDir,
            DigestUtil.toDigest(outDirDigest),
            outDir,
            EMPTY_DIR_DIGEST_V2,
            EMPTY_DIR);

    Command command = Command.newBuilder().addOutputPaths("outdir").build();
    Path execDir =
        runCreateExecDir(
            "test-op-output-dir-descendants", rootDirDigest, directoriesIndex, command);

    try {
      assertThat(Files.isDirectory(execDir.resolve("outdir"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("outdir"))).isFalse();
      assertThat(Files.isDirectory(execDir.resolve("outdir/existing"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("outdir/existing"))).isFalse();
      assertThat(Files.isSymbolicLink(execDir.resolve("inputdir"))).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  /**
   * Verifies output path exclusion works when the command has a non-empty working directory. Output
   * paths are relative to the working directory, so the resolution must account for
   * execDir.resolve(workingDirectory).resolve(outputPath).
   */
  @Test
  public void outputPathWithWorkingDirectoryIsNotSymlinked() throws Exception {
    Directory buildDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("outdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest buildDirDigest = DIGEST_UTIL.compute(buildDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("build")
                    .setDigest(DigestUtil.toDigest(buildDirDigest)))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest),
            rootDir,
            DigestUtil.toDigest(buildDirDigest),
            buildDir,
            EMPTY_DIR_DIGEST_V2,
            EMPTY_DIR);

    Command command =
        Command.newBuilder().setWorkingDirectory("build").addOutputPaths("outdir").build();
    Path execDir = runCreateExecDir("test-op-workdir", rootDirDigest, directoriesIndex, command);

    try {
      Path outdir = execDir.resolve("build/outdir");
      assertThat(Files.exists(outdir)).isTrue();
      assertThat(Files.isSymbolicLink(outdir)).isFalse();
      assertThat(Files.isDirectory(outdir)).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  /**
   * Verifies that multiple output_paths entries are all excluded from linking, while non-output
   * directories in the same tree are still symlinked.
   */
  @Test
  public void multipleOutputPathsAreNotSymlinked() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("classes").setDigest(EMPTY_DIR_DIGEST_V2))
            .addDirectories(
                DirectoryNode.newBuilder().setName("libdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .addDirectories(
                DirectoryNode.newBuilder().setName("resources").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir, EMPTY_DIR_DIGEST_V2, EMPTY_DIR);

    Command command =
        Command.newBuilder().addOutputPaths("classes").addOutputPaths("resources").build();
    Path execDir = runCreateExecDir("test-op-multi", rootDirDigest, directoriesIndex, command);

    try {
      assertThat(Files.isSymbolicLink(execDir.resolve("classes"))).isFalse();
      assertThat(Files.isDirectory(execDir.resolve("classes"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("resources"))).isFalse();
      assertThat(Files.isDirectory(execDir.resolve("resources"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("libdir"))).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  /**
   * Verifies that directories NOT in output_paths are still symlinked normally. This ensures the
   * fix doesn't break the linking optimization for regular input directories.
   */
  @Test
  public void nonOutputPathDirectoryIsStillSymlinked() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("inputdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir, EMPTY_DIR_DIGEST_V2, EMPTY_DIR);

    Command command = Command.newBuilder().addOutputPaths("something_else").build();
    Path execDir = runCreateExecDir("test-op-non-output", rootDirDigest, directoriesIndex, command);

    try {
      Path inputdir = execDir.resolve("inputdir");
      assertThat(Files.exists(inputdir)).isTrue();
      assertThat(Files.isSymbolicLink(inputdir)).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  // --- linkedInputExclusions unit tests ---

  @Test
  public void linkedInputExclusions_fromOutputPaths() {
    Command command = Command.newBuilder().addOutputPaths("a/b/c/output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // output_paths includes the path itself ("a/b/c/output.jar") because under REAPI >= 2.1
    // output_paths entries can be files OR directories — we don't know the type, so we
    // conservatively treat each as a potential directory that must remain real.
    assertThat(result).containsExactly("a", "a/b", "a/b/c", "a/b/c/output.jar");
  }

  @Test
  public void linkedInputExclusions_fromOutputFiles() {
    Command command = Command.newBuilder().addOutputFiles("a/b/c/output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // For output_files, add parent dir + ancestors (not the file itself)
    assertThat(result).containsExactly("a", "a/b", "a/b/c");
  }

  @Test
  public void linkedInputExclusions_fromOutputDirectories() {
    Command command = Command.newBuilder().addOutputDirectories("a/b/outdir").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // For output_directories, add the directory itself + ancestors
    assertThat(result).containsExactly("a", "a/b", "a/b/outdir");
  }

  @Test
  public void linkedInputExclusions_fromMultipleOutputs() {
    Command command =
        Command.newBuilder()
            .addOutputPaths("a/b/output1.jar")
            .addOutputPaths("x/y/z/output2.jar")
            .build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    assertThat(result)
        .containsExactly("a", "a/b", "a/b/output1.jar", "x", "x/y", "x/y/z", "x/y/z/output2.jar");
  }

  @Test
  public void linkedInputExclusions_withWorkingDirectory() {
    Command command =
        Command.newBuilder().setWorkingDirectory("build").addOutputPaths("out/output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    assertThat(result).containsExactly("build", "build/out", "build/out/output.jar");
  }

  @Test
  public void linkedInputExclusions_rootLevelOutput() {
    Command command = Command.newBuilder().addOutputFiles("output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // Root-level file has no parent directory to add
    assertThat(result).isEmpty();
  }

  @Test
  public void linkedInputExclusions_emptyOutputs() {
    Command command = Command.getDefaultInstance();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    assertThat(result).isEmpty();
  }

  @Test
  public void linkedInputExclusions_outputFilesWithWorkingDirectory() {
    Command command =
        Command.newBuilder().setWorkingDirectory("build").addOutputFiles("out/output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // Parent of build/out/output.jar is build/out, ancestors are build
    assertThat(result).containsExactly("build", "build/out");
  }

  @Test
  public void linkedInputExclusions_outputDirectoriesWithWorkingDirectory() {
    Command command =
        Command.newBuilder().setWorkingDirectory("build").addOutputDirectories("classes").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    assertThat(result).containsExactly("build", "build/classes");
  }

  @Test
  public void linkedInputExclusions_rootLevelOutputPath() {
    // Single-component output_path — the path itself is added but has no ancestors
    Command command = Command.newBuilder().addOutputPaths("outdir").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    assertThat(result).containsExactly("outdir");
  }

  @Test
  public void linkedInputExclusions_outputPathRootsExcludeDescendants() {
    Command command = Command.newBuilder().addOutputPaths("outdir").build();
    LinkedInputExclusions.ExclusionSet result =
        LinkedInputExclusions.computeExclusionSet(command, ImmutableSet.of());

    assertThat(result.paths()).containsExactly("outdir");
    assertThat(result.recursivePaths()).containsExactly("outdir");
    assertThat(result.excludes("outdir")).isTrue();
    assertThat(result.excludes("outdir/existing")).isTrue();
    assertThat(result.excludes("outdirectory/existing")).isFalse();
  }

  @Test
  public void linkedInputExclusions_mixedOutputFilesAndDirectories() {
    Command command =
        Command.newBuilder()
            .addOutputFiles("a/b/output.jar")
            .addOutputDirectories("a/c/classes")
            .build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // output_files: parent a/b + ancestor a
    // output_directories: a/c/classes itself + ancestors a/c, a
    assertThat(result).containsExactly("a", "a/b", "a/c", "a/c/classes");
  }

  @Test
  public void linkedInputExclusions_outputPathsShortCircuitOutputFilesAndDirectories() {
    // REAPI >= 2.1: when output_paths is present, output_files and output_directories are ignored.
    Command command =
        Command.newBuilder()
            .addOutputPaths("a/out.jar")
            .addOutputFiles("ignored/file.txt")
            .addOutputDirectories("ignored/dir")
            .build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // Only the output_paths entry and its ancestor contribute; the ignored/* entries do not.
    assertThat(result).containsExactly("a", "a/out.jar");
  }

  @Test
  public void linkedInputExclusions_withExcludePaths() {
    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    ImmutableSet<String> result =
        LinkedInputExclusions.compute(command, ImmutableSet.of("tools/bin/javac", "libs/dep.jar"));
    // Output ancestors + exclude path ancestors (exclude paths are files, not directories,
    // so only their ancestors are added — the file path itself is irrelevant to directory linking)
    assertThat(result).containsExactly("out", "out/output.jar", "tools", "tools/bin", "libs");
  }

  @Test
  public void linkedInputExclusions_excludePathsAddOnlyAncestors() {
    // Exclude paths are file paths (tool inputs). Only their ancestor directories need
    // to remain real — the file path itself is never checked during directory traversal.
    Command command = Command.getDefaultInstance();
    ImmutableSet<String> result =
        LinkedInputExclusions.compute(command, ImmutableSet.of("tools/compiler"));
    assertThat(result).containsExactly("tools");
  }

  @Test
  public void linkedInputExclusions_trailingSlashNormalized() {
    Command command = Command.newBuilder().addOutputPaths("out/output.jar/").build();
    ImmutableSet<String> result =
        LinkedInputExclusions.compute(command, ImmutableSet.of("tools/bin/"));
    // Trailing slashes stripped — paths should be clean.
    // "tools/bin/" is a file exclude path (trailing slash stripped to "tools/bin"),
    // so only its ancestor "tools" is added.
    assertThat(result).containsExactly("out", "out/output.jar", "tools");
  }

  @Test
  public void linkedInputExclusions_workingDirectoryWithTrailingSlash() {
    Command command =
        Command.newBuilder().setWorkingDirectory("build/").addOutputPaths("out/output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // Trailing slash on workDir should be stripped — no double-slash paths
    assertThat(result).containsExactly("build", "build/out", "build/out/output.jar");
  }

  @Test
  public void linkedInputExclusions_dotDotSegmentsNormalized() {
    Command command = Command.newBuilder().addOutputPaths("a/b/../c/output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // "a/b/../c/output.jar" normalizes to "a/c/output.jar"
    assertThat(result).containsExactly("a", "a/c", "a/c/output.jar");
  }

  @Test
  public void linkedInputExclusions_pathToRelativeStringUsesForwardSlashesOnWindowsFileSystem()
      throws Exception {
    try (FileSystem fileSystem = Jimfs.newFileSystem(Configuration.windows())) {
      Path root = fileSystem.getPath("C:\\exec");
      Path dir = root.resolve("a").resolve("b").resolve("out");

      assertThat(LinkedInputExclusions.pathToRelativeString(root, dir)).isEqualTo("a/b/out");
    }
  }

  @Test
  public void linkedInputExclusions_dotSegmentsNormalized() {
    Command command = Command.newBuilder().addOutputFiles("a/./b/output.jar").build();
    ImmutableSet<String> result = LinkedInputExclusions.compute(command, ImmutableSet.of());
    // "a/./b/output.jar" normalizes to "a/b/output.jar"; parent is "a/b"
    assertThat(result).containsExactly("a", "a/b");
  }

  @Test
  public void linkedInputExclusions_excludePathWithDotDotNormalized() {
    Command command = Command.getDefaultInstance();
    ImmutableSet<String> result =
        LinkedInputExclusions.compute(command, ImmutableSet.of("tools/./bin/../bin/javac"));
    // "tools/./bin/../bin/javac" normalizes to "tools/bin/javac"; ancestors are "tools",
    // "tools/bin"
    assertThat(result).containsExactly("tools", "tools/bin");
  }

  @Test
  public void linkedInputExclusions_overlappingExcludePathsAndOutputPaths() {
    // Output at a/b/out.jar and tool input at a/c/javac share ancestor "a".
    // Verify that set deduplication produces the correct merged result.
    Command command = Command.newBuilder().addOutputPaths("a/b/out.jar").build();
    ImmutableSet<String> result =
        LinkedInputExclusions.compute(command, ImmutableSet.of("a/c/javac"));
    // "a" appears from both output ancestors and exclude ancestors — deduplicated.
    // Output: a, a/b, a/b/out.jar (conservative — output_paths path itself included)
    // Exclude: a, a/c (ancestors of a/c/javac)
    assertThat(result).containsExactly("a", "a/b", "a/b/out.jar", "a/c");
  }

  @Test
  public void linkedInputExclusions_absoluteOutputPathThrows() {
    Command command = Command.newBuilder().addOutputPaths("/absolute/path").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> LinkedInputExclusions.compute(command, ImmutableSet.of()));
  }

  @Test
  public void linkedInputExclusions_absoluteExcludePathThrows() {
    Command command = Command.getDefaultInstance();
    assertThrows(
        IllegalArgumentException.class,
        () -> LinkedInputExclusions.compute(command, ImmutableSet.of("/absolute/tool")));
  }

  @Test
  public void invalidExclusionComputationDoesNotCreateExecDir() {
    String operationName = "test-invalid-exclusion-computation";
    Command command = Command.newBuilder().addOutputPaths("a/\0/../b").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(EMPTY_DIR_DIGEST)).build();
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(EMPTY_DIR_DIGEST_V2, EMPTY_DIR);

    assertThrows(
        RuntimeException.class,
        () ->
            execFileSystem.createExecDir(
                operationName,
                directoriesIndex,
                DigestFunction.Value.SHA256,
                action,
                command,
                null,
                WorkerExecutedMetadata.newBuilder()));

    assertThat(Files.exists(execFileSystem.root().resolve(operationName))).isFalse();
  }

  // --- linkedInputExclusions tree walk integration tests ---

  @Test
  public void linkedInputExclusions_symlinksAtShallowDepthWhenOutputIsDeep() throws Exception {
    // Build tree: root -> a -> b -> {src (with file), out (with file)}
    // Output is at a/b/out/output.jar
    // linkedInputExclusions = {a, a/b, a/b/out} — so a/b/src should be symlinked
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory bDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("out")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest bDirDigest = DIGEST_UTIL.compute(bDir);

    Directory aDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("b")
                    .setDigest(DigestUtil.toDigest(bDirDigest))
                    .build())
            .build();
    Digest aDirDigest = DIGEST_UTIL.compute(aDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("a")
                    .setDigest(DigestUtil.toDigest(aDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(aDirDigest), aDir,
            DigestUtil.toDigest(bDirDigest), bDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    Command command = Command.newBuilder().addOutputPaths("a/b/out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    Path execDir =
        execFileSystem.createExecDir(
            "test-deep-output",
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            null,
            WorkerExecutedMetadata.newBuilder());

    try {
      // a, a/b are in linkedInputExclusions — real dirs
      assertThat(Files.isDirectory(execDir.resolve("a"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("a"))).isFalse();
      assertThat(Files.isDirectory(execDir.resolve("a/b"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b"))).isFalse();

      // a/b/src is NOT in linkedInputExclusions — should be symlinked
      assertThat(Files.exists(execDir.resolve("a/b/src"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b/src"))).isTrue();

      // a/b/out is in linkedInputExclusions — should be real dir
      assertThat(Files.isDirectory(execDir.resolve("a/b/out"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b/out"))).isFalse();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  @Test
  public void linkedInputExclusions_rootLevelDirectoryWithNoOutputs() throws Exception {
    // A first-level directory with no outputs anywhere should be symlinked at root level
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    // Output at a different path — src/ is completely safe to symlink
    Command command = Command.newBuilder().addOutputPaths("build/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    Path execDir =
        execFileSystem.createExecDir(
            "test-root-dir",
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            null,
            WorkerExecutedMetadata.newBuilder());

    try {
      assertThat(Files.exists(execDir.resolve("src"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("src"))).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  @Test
  public void linkedInputExclusions_noOutputs_everythingSymlinked() throws Exception {
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("lib")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    // No outputs — everything should be symlinked
    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    Path execDir =
        execFileSystem.createExecDir(
            "test-no-outputs",
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            null,
            WorkerExecutedMetadata.newBuilder());

    try {
      assertThat(Files.isSymbolicLink(execDir.resolve("src"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("lib"))).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  @Test
  public void linkedInputExclusions_withLinkInputDirectoriesDisabled() throws Exception {
    // With linkInputDirectories=false, input directories are materialized as real directories
    // rather than symlinked, regardless of the exclusion computation.
    CFCLinkExecFileSystem noLinkFs =
        newLinkExecFileSystem(
            "nolink", /* linkInputDirectories= */ false, /* patterns= */ ImmutableList.of());
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    Path execDir =
        noLinkFs.createExecDir(
            "test-no-link",
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            null,
            WorkerExecutedMetadata.newBuilder());

    try {
      // With linkInputDirectories=false, directories should NOT be symlinked
      assertThat(Files.isDirectory(execDir.resolve("src"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("src"))).isFalse();
    } finally {
      noLinkFs.destroyExecDir(execDir);
    }
  }

  @Test
  public void linkedInputExclusionPatterns_keepMatchingTopLevelDirectoryRealAnchored()
      throws Exception {
    // Operator-supplied exclusion patterns merge with the auto-computed exclusion set: a directory
    // matching a pattern is kept real even when no output forces it to be. This reproduces the
    // migration story for the old inclusion regex "^(?!external$).*$" — rewriting it as the
    // exclusion pattern "external" keeps the external/ tree from being symlinked. The pattern is a
    // full-string (anchored) match, so a sibling "externalfoo" is NOT kept real.
    CFCLinkExecFileSystem patternFs =
        newLinkExecFileSystem(
            "anchored", /* linkInputDirectories= */ true, ImmutableList.of("external"));
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("external")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("externalfoo")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    // No outputs touch any directory, so without the pattern all three would be symlinked.
    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    Path execDir =
        patternFs.createExecDir(
            "test-anchored-pattern",
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            null,
            WorkerExecutedMetadata.newBuilder());

    try {
      // external matches the exclusion pattern exactly — kept as a real directory.
      assertThat(Files.isDirectory(execDir.resolve("external"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("external"))).isFalse();

      // externalfoo is not a full-string match for "external" — symlinked (proves anchoring).
      assertThat(Files.isSymbolicLink(execDir.resolve("externalfoo"))).isTrue();

      // src matches no pattern and no output — symlinked to its CAS tree.
      assertThat(Files.isSymbolicLink(execDir.resolve("src"))).isTrue();
    } finally {
      patternFs.destroyExecDir(execDir);
    }
  }

  @Test
  public void linkedInputExclusionPatterns_keepNestedDirectoryRealAlongsideOutputAncestors()
      throws Exception {
    // A pattern is matched against each directory's path relative to the input root, so it can keep
    // a deep directory real. Here the output path forces a and a/b real via the auto-computed set,
    // the pattern "a/b/keep" keeps that nested directory real, and a/b/drop — matched by neither
    // mechanism — is symlinked. Exercises depth matching and the union of both mechanisms.
    CFCLinkExecFileSystem patternFs =
        newLinkExecFileSystem(
            "nested", /* linkInputDirectories= */ true, ImmutableList.of("a/b/keep"));
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory bDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("keep")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("drop")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest bDirDigest = DIGEST_UTIL.compute(bDir);

    Directory aDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("b")
                    .setDigest(DigestUtil.toDigest(bDirDigest))
                    .build())
            .build();
    Digest aDirDigest = DIGEST_UTIL.compute(aDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("a")
                    .setDigest(DigestUtil.toDigest(aDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(aDirDigest), aDir,
            DigestUtil.toDigest(bDirDigest), bDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    // Output under a/b/out forces a and a/b into the auto-computed exclusion set.
    Command command = Command.newBuilder().addOutputPaths("a/b/out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    Path execDir =
        patternFs.createExecDir(
            "test-nested-pattern",
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            null,
            WorkerExecutedMetadata.newBuilder());

    try {
      // a and a/b are output-path ancestors (auto-computed) — real, so the walk descends into them.
      assertThat(Files.isSymbolicLink(execDir.resolve("a"))).isFalse();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b"))).isFalse();

      // a/b/keep matches the pattern at depth — kept real.
      assertThat(Files.isDirectory(execDir.resolve("a/b/keep"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b/keep"))).isFalse();

      // a/b/drop matches neither the computed set nor a pattern — symlinked.
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b/drop"))).isTrue();
    } finally {
      patternFs.destroyExecDir(execDir);
    }
  }

  @Test
  public void linkedInputExclusionPatterns_keepNestedDirectoryRealWithoutOutputAncestors()
      throws Exception {
    // The pattern match itself must keep ancestors real. Otherwise the walk would symlink a/ and
    // skip the subtree before it ever reaches a/b/keep.
    CFCLinkExecFileSystem patternFs =
        newLinkExecFileSystem(
            "nested-no-output", /* linkInputDirectories= */ true, ImmutableList.of("a/b/keep"));
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory bDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("keep")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("drop")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest bDirDigest = DIGEST_UTIL.compute(bDir);

    Directory aDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("b")
                    .setDigest(DigestUtil.toDigest(bDirDigest))
                    .build())
            .build();
    Digest aDirDigest = DIGEST_UTIL.compute(aDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("a")
                    .setDigest(DigestUtil.toDigest(aDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(aDirDigest), aDir,
            DigestUtil.toDigest(bDirDigest), bDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    Path execDir =
        patternFs.createExecDir(
            "test-nested-pattern-no-output",
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            null,
            WorkerExecutedMetadata.newBuilder());

    try {
      assertThat(Files.isSymbolicLink(execDir.resolve("a"))).isFalse();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b"))).isFalse();
      assertThat(Files.isDirectory(execDir.resolve("a/b/keep"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b/keep"))).isFalse();
      assertThat(Files.isSymbolicLink(execDir.resolve("a/b/drop"))).isTrue();
    } finally {
      patternFs.destroyExecDir(execDir);
    }
  }

  @Test
  public void constructorRejectsInvalidExclusionPattern() {
    // An unparseable exclusion regex fails fast at construction with a message naming the config
    // field, rather than surfacing an opaque PatternSyntaxException deep in worker startup.
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                newLinkExecFileSystem(
                    "badregex", /* linkInputDirectories= */ true, ImmutableList.of("[unclosed")));
    assertThat(thrown).hasMessageThat().contains("linkedInputExclusionPatterns");
  }

  // --- createLightweightExecDir tests ---

  @Test
  public void createLightweightExecDir_createsOutputDirsOnly() throws Exception {
    Command command = Command.newBuilder().addOutputPaths("bazel-out/output.jar").build();

    Path execDir = execFileSystem.createLightweightExecDir("test-lightweight", command);

    try {
      assertThat(Files.isDirectory(execDir)).isTrue();
      // Output parent directory should be stamped
      assertThat(Files.isDirectory(execDir.resolve("bazel-out"))).isTrue();
      // No symlinks — lightweight exec dir has no input links
      try (var stream = Files.walk(execDir)) {
        long symlinkCount = stream.filter(Files::isSymbolicLink).count();
        assertThat(symlinkCount).isEqualTo(0);
      }
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  @Test
  public void createLightweightExecDir_destroyIsInexpensive() throws Exception {
    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();

    Path execDir = execFileSystem.createLightweightExecDir("test-lightweight-destroy", command);
    // destroyExecDir on lightweight dir should not try to decrement CAS refs
    // (no refs tracked in rootInputFiles/rootInputDirectories maps)
    execFileSystem.destroyExecDir(execDir);

    assertThat(Files.exists(execDir)).isFalse();
  }

  @Test
  public void createLightweightExecDir_cleansPartialDirWhenStampFails() {
    String operationName = "test-lightweight-stamp-failure";
    Command command = Command.newBuilder().addOutputPaths("bad\0path/output.jar").build();

    assertThrows(
        RuntimeException.class,
        () -> execFileSystem.createLightweightExecDir(operationName, command));

    assertThat(Files.exists(root.resolve("exec").resolve(operationName))).isFalse();
  }

  // --- fetchAndRefInputs tests ---

  @Test
  public void fetchAndRefInputs_returnsNonNullResult() throws Exception {
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());

    try {
      assertThat(fetchResult).isNotNull();
      assertThat(fetchResult.entries()).isNotEmpty();
      // src/ is not in linkedInputExclusions (output is at out/), so it should be a DIRECTORY entry
      assertThat(fetchResult.entries()).hasSize(1);
      assertThat(fetchResult.entries().get(0).type())
          .isEqualTo(build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(fetchResult.entries().get(0).relativePath()).isEqualTo("src");
      assertThat(fetchResult.entries().get(0).casPath()).isNotNull();
    } finally {
      fetchResult.close();
    }
  }

  @Test
  public void fetchAndRefInputs_directoryMissAddsFetchedBytes() throws Exception {
    ByteString content = ByteString.copyFromUtf8("directory input content");
    fileCache.put(new ContentAddressableStorage.Blob(content, DIGEST_UTIL));
    Digest fileDigest = DIGEST_UTIL.compute(content);

    Directory srcDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("Input.java")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest srcDirDigest = DIGEST_UTIL.compute(srcDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(srcDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(srcDirDigest), srcDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();
    WorkerExecutedMetadata.Builder workerExecutedMetadata = WorkerExecutedMetadata.newBuilder();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            workerExecutedMetadata);

    try {
      assertThat(workerExecutedMetadata.getFetchedBytes()).isEqualTo(content.size());
    } finally {
      fetchResult.close();
    }
  }

  @Test
  public void fetchAndRefInputs_doesNotCreateAnyLinks() throws Exception {
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("lib")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // Record the exec root state before fetchAndRefInputs
    Path execRoot = root.resolve("exec");

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());

    try {
      // No links should be created anywhere in the exec root
      // No symlinks in exec root
      try (var stream = Files.walk(execRoot)) {
        long symlinkCount = stream.filter(Files::isSymbolicLink).count();
        assertThat(symlinkCount).isEqualTo(0);
      }
    } finally {
      fetchResult.close();
    }
  }

  @Test
  public void fetchAndRefInputs_linkedInputExclusionsDeterminesGranularity() throws Exception {
    // Build tree: root -> {src (safe), out (has output)}
    // src should be a DIRECTORY entry, files in out should be individual FILE entries
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("out")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    // Output is under out/ — so out is in linkedInputExclusions, src is not
    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());

    try {
      // src/ is safe to symlink → DIRECTORY entry
      // out/ is in linkedInputExclusions → descended into (but empty, so no file entries)
      boolean hasSrcDir =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("src")
                          && e.type()
                              == build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(hasSrcDir).isTrue();

      // out/ should NOT appear as a DIRECTORY entry (it's in linkedInputExclusions)
      boolean hasOutDir =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("out")
                          && e.type()
                              == build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(hasOutDir).isFalse();

      // out/ was descended into (excluded as an output-path ancestor), so it is recorded for
      // cleanup
      assertThat(fetchResult.descendedDirectories()).contains("out");
    } finally {
      fetchResult.close();
    }
  }

  /**
   * Key regression test for FetchRefVisitor. Tool inputs should be fetched into CAS and appear in
   * toolInputCasPaths(), but NOT in entries() (they are managed through the tool root mechanism).
   * Without the FetchRefVisitor fix, tool inputs were completely skipped — never fetched, never
   * recorded — causing copyToolInputsIntoWorkerToolRoot to fail.
   */
  @Test
  public void fetchAndRefInputs_toolInputsFetchedButNotInEntries() throws Exception {
    // Pre-populate CAS with tool input content
    ByteString toolContent = ByteString.copyFromUtf8("tool binary content");
    ContentAddressableStorage.Blob toolBlob =
        new ContentAddressableStorage.Blob(toolContent, DIGEST_UTIL);
    fileCache.put(toolBlob);
    Digest toolDigest = DIGEST_UTIL.compute(toolContent);
    build.bazel.remote.execution.v2.Digest toolReapiDigest = DigestUtil.toDigest(toolDigest);

    // Build tree: root -> {src/ (directory), tools/tool_binary (file)}
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory toolsDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("tool_binary")
                    .setDigest(toolReapiDigest)
                    .setIsExecutable(true)
                    .build())
            .build();
    Digest toolsDirDigest = DIGEST_UTIL.compute(toolsDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("tools")
                    .setDigest(DigestUtil.toDigest(toolsDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir,
            DigestUtil.toDigest(toolsDirDigest), toolsDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // "tools/tool_binary" is a tool input — should be fetched but not in entries
    ImmutableSet<String> toolInputPaths = ImmutableSet.of("tools/tool_binary");

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            toolInputPaths,
            WorkerExecutedMetadata.newBuilder());

    try {
      // (a) Tool input should be in toolInputCasPaths with a non-null CAS path
      assertThat(fetchResult.toolInputCasPaths()).containsKey("tools/tool_binary");
      assertThat(fetchResult.toolInputCasPaths().get("tools/tool_binary")).isNotNull();

      // (b) Tool input should NOT appear in entries (managed via tool root)
      boolean hasToolInEntries =
          fetchResult.entries().stream()
              .anyMatch(e -> e.relativePath().equals("tools/tool_binary"));
      assertThat(hasToolInEntries).isFalse();

      // (c) Non-tool directory (src/) should still be in entries as a DIRECTORY entry
      boolean hasSrcDir =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("src")
                          && e.type()
                              == build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(hasSrcDir).isTrue();
    } finally {
      fetchResult.close();
    }
  }

  /**
   * Regression test: zero-size tool inputs (e.g. _repo_mapping in .runfiles) have no CAS entry.
   * They must be tracked in zeroSizeToolInputPaths so copyToolInputsIntoWorkerToolRoot can create
   * them as empty files. Without the fix, zero-size tool inputs were silently dropped — not in
   * toolInputCasPaths, not in entries, not tracked anywhere.
   */
  @Test
  public void fetchAndRefInputs_zeroSizeToolInputTrackedInFetchResult() throws Exception {
    // Zero-size digest
    ByteString emptyContent = ByteString.EMPTY;
    Digest zeroDigest = DIGEST_UTIL.compute(emptyContent);
    build.bazel.remote.execution.v2.Digest zeroReapiDigest = DigestUtil.toDigest(zeroDigest);

    // Non-zero tool input (to verify both paths work together)
    ByteString toolContent = ByteString.copyFromUtf8("tool binary content");
    ContentAddressableStorage.Blob toolBlob =
        new ContentAddressableStorage.Blob(toolContent, DIGEST_UTIL);
    fileCache.put(toolBlob);
    Digest toolDigest = DIGEST_UTIL.compute(toolContent);
    build.bazel.remote.execution.v2.Digest toolReapiDigest = DigestUtil.toDigest(toolDigest);

    // Build tree: root -> tools/{tool_binary (non-zero), zero_file (zero-size)}
    Directory toolsDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("tool_binary")
                    .setDigest(toolReapiDigest)
                    .setIsExecutable(true)
                    .build())
            .addFiles(FileNode.newBuilder().setName("zero_file").setDigest(zeroReapiDigest).build())
            .build();
    Digest toolsDirDigest = DIGEST_UTIL.compute(toolsDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("tools")
                    .setDigest(DigestUtil.toDigest(toolsDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(toolsDirDigest), toolsDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // Both files are tool inputs
    ImmutableSet<String> toolInputPaths = ImmutableSet.of("tools/tool_binary", "tools/zero_file");

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            toolInputPaths,
            WorkerExecutedMetadata.newBuilder());

    try {
      // Non-zero tool input in toolInputCasPaths
      assertThat(fetchResult.toolInputCasPaths()).containsKey("tools/tool_binary");

      // Zero-size tool input NOT in toolInputCasPaths (no CAS entry)
      assertThat(fetchResult.toolInputCasPaths()).doesNotContainKey("tools/zero_file");

      // Zero-size tool input tracked in zeroSizeToolInputPaths
      assertThat(fetchResult.zeroSizeToolInputPaths()).contains("tools/zero_file");

      // Neither tool input should appear in entries
      boolean hasToolInEntries =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("tools/tool_binary")
                          || e.relativePath().equals("tools/zero_file"));
      assertThat(hasToolInEntries).isFalse();
    } finally {
      fetchResult.close();
    }
  }

  // Operator linkedInputExclusionPatterns must apply on the persistent fetch path too: a directory
  // matched by a pattern stays real (descended into) rather than being symlinked as a unit.
  @Test
  public void fetchAndRefInputs_operatorExclusionPatternKeepsDirectoryReal() throws Exception {
    CFCLinkExecFileSystem patternFs =
        newLinkExecFileSystem(
            "fetch-pattern", /* linkInputDirectories= */ true, ImmutableList.of("external"));

    // root -> {external/ (empty, pattern-matched), lib/ (empty, linkable)}
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("external")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("lib")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    // No outputs — only the operator pattern excludes anything.
    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        patternFs.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());
    try {
      // lib/ is not matched → linked as a DIRECTORY entry
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          e.relativePath().equals("lib")
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType
                                      .DIRECTORY))
          .isTrue();
      // external/ matched the pattern → kept real (descended), not a DIRECTORY entry
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          e.relativePath().equals("external")
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType
                                      .DIRECTORY))
          .isFalse();
      assertThat(fetchResult.descendedDirectories()).contains("external");
    } finally {
      fetchResult.close();
    }
  }

  // Recursive output-root exclusion must apply on the fetch path: a directory nested under a
  // declared output directory is kept real (descended into) and a real file under it is emitted as
  // an individual FILE entry. The nested dir is excluded only via the recursive root — absent from
  // ExclusionSet.paths() — so it must still be recorded in descendedDirectories for cleanup.
  @Test
  public void fetchAndRefInputs_recursiveOutputRootDescendantKeptReal() throws Exception {
    ByteString content = ByteString.copyFromUtf8("nested input under output root");
    fileCache.put(new ContentAddressableStorage.Blob(content, DIGEST_UTIL));
    Digest fileDigest = DIGEST_UTIL.compute(content);

    // out/ declares an output dir and contains gen/, which holds a real input file.
    Directory genDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("input.txt")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest genDirDigest = DIGEST_UTIL.compute(genDir);
    Directory outDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("gen")
                    .setDigest(DigestUtil.toDigest(genDirDigest))
                    .build())
            .build();
    Digest outDirDigest = DIGEST_UTIL.compute(outDir);
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("out")
                    .setDigest(DigestUtil.toDigest(outDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(outDirDigest), outDir,
            DigestUtil.toDigest(genDirDigest), genDir);

    // out is a recursive output root: out and out/gen must stay real; the file must be fetched.
    Command command = Command.newBuilder().addOutputPaths("out").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());
    try {
      // Neither out nor out/gen is symlinked as a unit.
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          (e.relativePath().equals("out") || e.relativePath().equals("out/gen"))
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType
                                      .DIRECTORY))
          .isFalse();
      // The file under the kept-real subtree is fetched as an individual FILE entry.
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          e.relativePath().equals("out/gen/input.txt")
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType.FILE))
          .isTrue();
      // out/gen is the recursive-only descendant (not in ExclusionSet.paths()); it must still be
      // recorded so cleanup removes it from the reused worker exec root.
      assertThat(fetchResult.descendedDirectories()).containsAtLeast("out", "out/gen");
      // The FILE entry must carry the CAS path and key that linkFromFetchResult relies on; these
      // are populated in the async fetch callback, and a null casPath would NPE at hardlink time.
      build.buildfarm.worker.persistent.FetchResult.Entry fileEntry =
          fetchResult.entries().stream()
              .filter(e -> e.relativePath().equals("out/gen/input.txt"))
              .findFirst()
              .orElseThrow();
      assertThat(fileEntry.casPath()).isNotNull();
      assertThat(fileEntry.key()).isNotNull();
    } finally {
      fetchResult.close();
    }
  }

  // The fetch path must enforce the same absolute-symlink-target guard as createExecDir.
  @Test
  public void fetchAndRefInputs_absoluteSymlinkTargetRejected() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addSymlinks(
                SymlinkNode.newBuilder().setName("escape").setTarget("/etc/passwd").build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(rootDirDigest), rootDir);
    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // Shared fixture is configured allowSymlinkTargetAbsolute=false, so the absolute target
    // rejects.
    assertThrows(
        IOException.class,
        () ->
            execFileSystem.fetchAndRefInputs(
                directoriesIndex,
                DigestFunction.Value.SHA256,
                action,
                command,
                ImmutableSet.of(),
                WorkerExecutedMetadata.newBuilder()));
  }

  // FetchRefVisitor must emit ZERO_SIZE_FILE and SYMLINK_NODE entries for non-tool size-0 files and
  // protobuf symlinks (producer side; the consumer side is covered in ProtoCoordinatorTest).
  @Test
  public void fetchAndRefInputs_emitsZeroSizeAndSymlinkEntries() throws Exception {
    Digest zeroDigest = DIGEST_UTIL.compute(ByteString.EMPTY);
    Directory rootDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("empty.txt")
                    .setDigest(DigestUtil.toDigest(zeroDigest))
                    .build())
            .addSymlinks(SymlinkNode.newBuilder().setName("link").setTarget("empty.txt").build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(rootDirDigest), rootDir);
    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());
    try {
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          e.relativePath().equals("empty.txt")
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType
                                      .ZERO_SIZE_FILE))
          .isTrue();
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          e.relativePath().equals("link")
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType
                                      .SYMLINK_NODE
                              && "empty.txt".equals(e.symlinkTarget())))
          .isTrue();
    } finally {
      fetchResult.close();
    }
  }
}
