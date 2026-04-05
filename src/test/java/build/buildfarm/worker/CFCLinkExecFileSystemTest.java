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
import build.bazel.remote.execution.v2.RequestMetadata;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
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
            ImmutableList.of(".*"),
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
   * Verifies that directories listed in output_paths are not symlinked to the read-only CAS cache,
   * even when they match the linkedInputDirectories patterns. Before the fix, output_paths entries
   * were indistinguishable from regular input directories, causing them to be symlinked to
   * read-only CAS cache and making the action fail with AccessDeniedException.
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
}
