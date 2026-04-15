// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Collections.synchronizedList;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.CASFileCache.PathResult;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.ExecDirException.ViolationException;
import build.buildfarm.worker.persistent.FetchResult;
import build.buildfarm.worker.util.LinkedInputExclusions;
import build.buildfarm.worker.util.LinkedInputExclusions.ExclusionSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.extern.java.Log;
import org.jspecify.annotations.Nullable;

@Log
public class CFCLinkExecFileSystem extends CFCExecFileSystem {
  // perform first-available non-output symlinking and retain directories in cache
  private final boolean linkInputDirectories;

  // operator-supplied regex patterns; any input directory whose path-relative-to-the-input-root
  // matches is kept as a real directory rather than symlinked to its CAS tree. These merge with the
  // auto-computed LinkedInputExclusions set — a directory is excluded from linking if it appears in
  // the computed set OR matches one of these patterns.
  private final ImmutableList<Pattern> linkedInputExclusionPatterns;

  /**
   * Determines whether a directory should be linked as a unit (symlink to CAS tree) rather than
   * descended into and materialized file-by-file. Shared by LinkExecFileVisitor and FetchRefVisitor
   * to keep the decision logic in one place.
   */
  static boolean shouldLinkDirectory(
      boolean linkInputDirectories,
      @Nullable OutputDirectory outputDirectory,
      ExclusionSet linkedInputExclusions,
      String relativePath) {
    return linkInputDirectories
        && outputDirectory == null
        && !linkedInputExclusions.excludes(relativePath);
  }

  private final Map<Path, DigestFunction.Value> rootInputDigestFunction = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<String>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<build.bazel.remote.execution.v2.Digest>> rootInputDirectories =
      new ConcurrentHashMap<>();

  public CFCLinkExecFileSystem(
      Path root,
      CASFileCache fileCache,
      ImmutableMap<String, UserPrincipal> owners,
      boolean linkInputDirectories,
      Iterable<String> linkedInputDirectories,
      Iterable<String> linkedInputExclusionPatterns,
      boolean allowSymlinkTargetAbsolute,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ExecutorService fetchService) {
    super(
        root,
        fileCache,
        owners,
        allowSymlinkTargetAbsolute,
        removeDirectoryService,
        accessRecorder,
        fetchService);
    this.linkInputDirectories = linkInputDirectories;
    this.linkedInputExclusionPatterns = compileExclusionPatterns(linkedInputExclusionPatterns);
    if (linkedInputDirectories.iterator().hasNext()) {
      log.warning(
          "linkedInputDirectories is deprecated and ignored; the LinkedInputExclusions computation"
              + " automatically determines which directories can be symlinked based on output"
              + " paths. To force specific directories to remain real, use"
              + " linkedInputExclusionPatterns.");
    }
  }

  private static ImmutableList<Pattern> compileExclusionPatterns(Iterable<String> patterns) {
    ImmutableList.Builder<Pattern> compiled = ImmutableList.builder();
    for (String pattern : patterns) {
      try {
        compiled.add(Pattern.compile(pattern));
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException(
            "linkedInputExclusionPatterns contains an invalid regex: " + pattern, e);
      }
    }
    return compiled.build();
  }

  private static final class DirectoryFrame {
    private final String path;
    private final Directory directory;

    private DirectoryFrame(String path, Directory directory) {
      this.path = path;
      this.directory = directory;
    }
  }

  @SuppressWarnings("ConstantConditions")
  private ListenableFuture<Void> put(
      Digest digest, Path path, boolean isExecutable, Consumer<String> onKey) {
    if (digest.getSize() == 0) {
      return listeningDecorator(fetchService)
          .submit(
              () -> {
                Files.createFile(path);
                // ignore executable
                return null;
              });
    }
    String key = fileCache.getKey(digest, isExecutable);
    return transformAsync(
        fileCache.put(digest, isExecutable, fetchService),
        pathResult -> {
          checkNotNull(key);
          // we saw null entries in the built immutable list without synchronization
          onKey.accept(key);
          if (digest.getSize() != 0) {
            try {
              Files.createLink(path, pathResult.path());
            } catch (IOException e) {
              return immediateFailedFuture(e);
            }
          }
          return immediateFuture(null);
        },
        fetchService);
  }

  private ListenableFuture<Void> catchingPut(
      Digest digest, Path root, Path path, boolean isExecutable, Consumer<String> onKey) {
    return catching(
        put(digest, path, isExecutable, onKey),
        e -> new ViolationException(digest, root.relativize(path), isExecutable, e));
  }

  @SuppressWarnings("ConstantConditions")
  private ListenableFuture<PathResult> linkDirectory(
      Path execPath,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex) {
    return transformAsync(
        fileCache.putDirectory(digest, directoriesIndex, fetchService),
        pathResult -> {
          Path path = pathResult.path();
          if (pathResult.isMissed()) {
            log.finer(
                String.format(
                    "putDirectory(%s, %s) created", execPath, DigestUtil.toString(digest)));
          }
          Files.createSymbolicLink(execPath, path);
          return immediateFuture(pathResult);
        },
        fetchService);
  }

  private static void checkExecErrors(Path path, List<Throwable> errors) throws ExecDirException {
    if (!errors.isEmpty()) {
      throw new ExecDirException(path, errors);
    }
  }

  @VisibleForTesting
  static OutputDirectory createOutputDirectory(Command command) {
    Iterable<String> files;
    Iterable<String> dirs;
    if (command.getOutputPathsCount() != 0) {
      files = command.getOutputPathsList();
      dirs = ImmutableList.of(); // output paths require the action to create their own directory
    } else {
      files = command.getOutputFilesList();
      dirs = command.getOutputDirectoriesList();
    }
    if (!command.getWorkingDirectory().isEmpty()) {
      files = Iterables.transform(files, file -> command.getWorkingDirectory() + "/" + file);
      dirs = Iterables.transform(dirs, dir -> command.getWorkingDirectory() + "/" + dir);
    }
    return OutputDirectory.parse(files, dirs, command.getEnvironmentVariablesList());
  }

  private boolean matchesLinkedInputExclusionPattern(String relativePath) {
    for (Pattern pattern : linkedInputExclusionPatterns) {
      if (pattern.matcher(relativePath).matches()) {
        return true;
      }
    }
    return false;
  }

  private ImmutableSet<String> matchedLinkedInputExclusionDirectories(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      build.bazel.remote.execution.v2.Digest rootDigest) {
    if (linkedInputExclusionPatterns.isEmpty()) {
      return ImmutableSet.of();
    }

    HashSet<String> matches = new HashSet<>();
    ArrayDeque<DirectoryFrame> remaining = new ArrayDeque<>();
    remaining.add(new DirectoryFrame("", directoriesIndex.get(rootDigest)));
    while (!remaining.isEmpty()) {
      DirectoryFrame frame = remaining.removeLast();
      for (DirectoryNode directoryNode : frame.directory.getDirectoriesList()) {
        String path =
            frame.path.isEmpty()
                ? directoryNode.getName()
                : frame.path + "/" + directoryNode.getName();
        if (matchesLinkedInputExclusionPattern(path)) {
          matches.add(path);
        }
        if (directoryNode.getDigest().getSizeBytes() != 0) {
          remaining.add(new DirectoryFrame(path, directoriesIndex.get(directoryNode.getDigest())));
        }
      }
    }
    return ImmutableSet.copyOf(matches);
  }

  private static long sumDirectorySize(
      build.bazel.remote.execution.v2.Digest root,
      Map<build.bazel.remote.execution.v2.Digest, Directory> index) {
    long size = 0;
    List<build.bazel.remote.execution.v2.Digest> digests = new ArrayList<>();
    digests.add(root);
    while (!digests.isEmpty()) {
      Directory directory = index.get(digests.removeFirst());
      for (FileNode fileNode : directory.getFilesList()) {
        size += fileNode.getDigest().getSizeBytes();
      }
      Iterables.addAll(
          digests,
          Iterables.transform(directory.getDirectoriesList(), dirNode -> dirNode.getDigest()));
    }
    return size;
  }

  class LinkExecFileVisitor extends ExecFileVisitor {
    private final Path root;
    private final ExclusionSet linkedInputExclusions;
    private final Map<build.bazel.remote.execution.v2.Digest, Directory>
        index; // only need retrieve
    private final OutputDirectory outputDirectoryRoot;
    private final List<OutputDirectory> outputDirectories = new ArrayList<>();
    private final List<String> inputFiles = synchronizedList(new ArrayList<>());
    private final List<build.bazel.remote.execution.v2.Digest> inputDirectories =
        synchronizedList(new ArrayList<>());

    LinkExecFileVisitor(
        WorkerExecutedMetadata.Builder workerExecutedMetadata,
        Path root,
        ExclusionSet linkedInputExclusions,
        Map<build.bazel.remote.execution.v2.Digest, Directory> index,
        OutputDirectory outputDirectoryRoot) {
      super(workerExecutedMetadata);
      this.root = root;
      this.linkedInputExclusions = linkedInputExclusions;
      this.index = index;
      this.outputDirectoryRoot = outputDirectoryRoot;
    }

    List<String> inputFiles() {
      return inputFiles;
    }

    List<build.bazel.remote.execution.v2.Digest> inputDirectories() {
      return inputDirectories;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      OutputDirectory outputDirectory;
      if (outputDirectories.isEmpty()) {
        outputDirectory = outputDirectoryRoot;
      } else {
        OutputDirectory parent = outputDirectories.get(outputDirectories.size() - 1);
        outputDirectory = resolveChildOutputDirectory(parent, dir.getFileName().toString());
      }
      String relativePath = LinkedInputExclusions.pathToRelativeString(root, dir);
      if (shouldLinkDirectory(
          linkInputDirectories, outputDirectory, linkedInputExclusions, relativePath)) {
        Digest digest = (Digest) attrs.fileKey();
        build.bazel.remote.execution.v2.Digest reapiDigest = DigestUtil.toDigest(digest);
        workerExecutedMetadata.addLinkedInputDirectories(relativePath);
        futures.add(
            transform(
                linkDirectory(dir, digest, index),
                pathResult -> {
                  inputDirectories.add(reapiDigest);
                  if (pathResult.isMissed()) {
                    fetchedBytes(sumDirectorySize(reapiDigest, index));
                  }
                  return null;
                },
                fetchService));
        return FileVisitResult.SKIP_SUBTREE;
      }

      FileVisitResult result = super.preVisitDirectory(dir, attrs);
      if (result == FileVisitResult.CONTINUE) {
        outputDirectories.add(outputDirectory);
      }
      return result;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      // this is only called when we've continued and placed onto stack
      outputDirectories.remove(outputDirectories.size() - 1);
      return super.postVisitDirectory(dir, exc);
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      ListenableFuture<Void> populate;
      boolean terminate = false;
      if (attrs.isSymbolicLink()) {
        ExecSymlinkAttributes symlinkAttrs = (ExecSymlinkAttributes) attrs;
        populate = putSymlink(file, symlinkAttrs.target());
      } else if (attrs.isRegularFile()) {
        Digest digest = (Digest) attrs.fileKey();
        ExecFileAttributes fileAttrs = (ExecFileAttributes) attrs;
        // mild risk here with inputFiles missing a key that was referenced...
        populate = catchingPut(digest, root, file, fileAttrs.isExecutable(), inputFiles::add);
      } else {
        populate = immediateFailedFuture(new IOException("unknown file type for " + file));
        terminate = true;
      }
      futures.add(populate);
      return terminate ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
    }
  }

  @Override
  public Path createExecDir(
      String operationName,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      DigestFunction.Value digestFunction,
      Action action,
      Command command,
      @Nullable UserPrincipal owner,
      WorkerExecutedMetadata.Builder workerExecutedMetadata)
      throws IOException, InterruptedException {
    Digest inputRootDigest = DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction);
    OutputDirectory outputDirectory = createOutputDirectory(command);

    ExclusionSet linkedInputExclusions =
        linkInputDirectories
            ? LinkedInputExclusions.computeExclusionSet(
                command,
                ImmutableSet.of(),
                matchedLinkedInputExclusionDirectories(
                    directoriesIndex, DigestUtil.toDigest(inputRootDigest)))
            : ExclusionSet.empty();

    Path execDir = root().resolve(operationName);
    if (Files.exists(execDir)) {
      Directories.remove(execDir, fileStore);
    }
    Files.createDirectories(execDir);

    log.log(Level.FINER, operationName + " walking execTree");
    ExecTree execTree = new ExecTree(directoriesIndex);
    LinkExecFileVisitor visitor =
        new LinkExecFileVisitor(
            workerExecutedMetadata,
            execDir,
            linkedInputExclusions,
            directoriesIndex,
            outputDirectory);
    execTree.walk(execDir, inputRootDigest, visitor);
    Iterable<ListenableFuture<Void>> fetchedFutures = visitor.futures();
    boolean success = false;
    try {
      ImmutableList.Builder<Throwable> exceptions = ImmutableList.builder();
      drainFutures(fetchedFutures, exceptions);
      checkExecErrors(execDir, exceptions.build());
      success = true;
    } finally {
      if (!success) {
        fileCache.decrementReferences(
            visitor.inputFiles(), visitor.inputDirectories(), digestFunction);
        Directories.remove(execDir, fileStore);
      }
    }

    rootInputDigestFunction.put(execDir, digestFunction);
    rootInputFiles.put(execDir, visitor.inputFiles());
    rootInputDirectories.put(execDir, visitor.inputDirectories());

    log.log(Level.FINER, operationName + " stamping output directories");
    boolean stamped = false;
    try {
      outputDirectory.stamp(execDir);
      stamped = true;
    } finally {
      if (!stamped) {
        destroyExecDir(execDir);
      }
    }
    if (owner != null) {
      Directories.setAllOwner(execDir, owner);
    }
    return execDir;
  }

  /**
   * Creates a lightweight exec dir with output directory stubs only — no input links, no CAS refs.
   * Used for persistent workers where inputs are materialized directly into the worker exec root.
   */
  @Override
  public Path createLightweightExecDir(String operationName, Command command) throws IOException {
    Path execDir = root().resolve(operationName);
    if (Files.exists(execDir)) {
      Directories.remove(execDir, fileStore);
    }
    try {
      Files.createDirectories(execDir);
      OutputDirectory outputDirectory = createOutputDirectory(command);
      outputDirectory.stamp(execDir);
      return execDir;
    } catch (IOException | RuntimeException e) {
      try {
        if (Files.exists(execDir)) {
          Directories.remove(execDir, fileStore);
        }
      } catch (IOException cleanupException) {
        e.addSuppressed(cleanupException);
      }
      throw e;
    }
  }

  /**
   * Fetches all non-tool inputs into local CAS and increments references, without creating any
   * links. Returns a FetchResult carrying CAS paths and ref keys for deferred linking by
   * ProtoCoordinator.
   *
   * <p>Uses the linkedInputExclusions set (with tool input ancestors as exclude paths) to determine
   * which entries to fetch as directories (one putDirectory call) vs individual files (one put call
   * per file). This granularity must match what ProtoCoordinator will use for linking.
   *
   * <p>Self-cleaning on failure: decrements all partially-incremented refs in a finally block.
   */
  @Override
  public FetchResult fetchAndRefInputs(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      DigestFunction.Value digestFunction,
      Action action,
      Command command,
      Set<String> toolInputPaths,
      WorkerExecutedMetadata.Builder workerExecutedMetadata)
      throws IOException, InterruptedException {
    build.buildfarm.v1test.Digest inputRootDigest =
        DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction);

    // Tool input ancestors are passed as exclude paths so they stay real directories — tool inputs
    // are materialized into the shared worker tool root, not linked into the exec root, so their
    // ancestor directories must remain real to hold the non-tool inputs. Operator-supplied patterns
    // merge in via excludeDirectories, matching createExecDir's computation.
    ExclusionSet linkedInputExclusions =
        linkInputDirectories
            ? LinkedInputExclusions.computeExclusionSet(
                command,
                toolInputPaths,
                matchedLinkedInputExclusionDirectories(
                    directoriesIndex, DigestUtil.toDigest(inputRootDigest)))
            : ExclusionSet.empty();

    // Walk the protobuf input tree (same as createExecDir but without creating links)
    OutputDirectory outputDirectory = createOutputDirectory(command);
    ExecTree execTree = new ExecTree(directoriesIndex);

    // Walk the tree with a visitor that collects CAS paths without creating links.
    // We use root() as the walk root for relativize(). No files are created at this path —
    // the visitor only calls fileCache.put/putDirectory, not filesystem operations.
    Path walkRoot = root();
    FetchRefVisitor visitor =
        new FetchRefVisitor(
            workerExecutedMetadata,
            walkRoot,
            directoriesIndex,
            linkInputDirectories,
            linkedInputExclusions,
            toolInputPaths,
            outputDirectory);
    execTree.walk(walkRoot, inputRootDigest, visitor);
    Iterable<ListenableFuture<Void>> fetchedFutures = visitor.futures();

    // Wait for all CAS futures, self-cleaning on failure
    boolean success = false;
    try {
      ImmutableList.Builder<Throwable> exceptions = ImmutableList.builder();
      drainFutures(fetchedFutures, exceptions);
      checkExecErrors(root(), exceptions.build());
      success = true;
    } finally {
      if (!success) {
        // Decrement all refs that were successfully incremented
        List<String> refKeys = visitor.refKeys();
        List<build.bazel.remote.execution.v2.Digest> refDigests = visitor.refDigests();
        if (!refKeys.isEmpty() || !refDigests.isEmpty()) {
          fileCache.decrementReferences(refKeys, refDigests, digestFunction);
        }
      }
    }

    return new FetchResult(
        ImmutableList.copyOf(visitor.entries()),
        ImmutableSet.copyOf(visitor.descendedDirectories()),
        ImmutableList.copyOf(visitor.refKeys()),
        ImmutableList.copyOf(visitor.refDigests()),
        digestFunction,
        fileCache,
        ImmutableMap.copyOf(visitor.toolInputCasPaths()),
        ImmutableSet.copyOf(visitor.zeroSizeToolInputPaths()));
  }

  /**
   * Visitor that walks the protobuf input tree and collects CAS paths for deferred linking, without
   * creating any filesystem links. Each input is fetched into local CAS with a reference held, and
   * an entry is recorded in the provided lists for later use by ProtoCoordinator.
   */
  private class FetchRefVisitor extends ExecFileVisitor {
    private final Path walkRoot;
    private final Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex;
    private final boolean linkInputDirectories;
    private final ExclusionSet linkedInputExclusions;
    private final Set<String> toolInputPaths;
    private final List<OutputDirectory> outputDirectoryStack = new ArrayList<>();
    private final List<String> refKeys = synchronizedList(new ArrayList<>());
    private final List<build.bazel.remote.execution.v2.Digest> refDigests =
        synchronizedList(new ArrayList<>());
    private final List<FetchResult.Entry> entries = synchronizedList(new ArrayList<>());
    // Relative paths of directories the walk kept real (descended into) rather than linking as a
    // unit. linkFromFetchResult materializes these as real directories (implicitly, as link
    // parents); cleanup removes them bottom-up once their links are gone.
    private final List<String> descendedDirectories = synchronizedList(new ArrayList<>());
    private final ConcurrentHashMap<String, Path> toolInputCasPaths = new ConcurrentHashMap<>();
    private final Set<String> zeroSizeToolInputPaths = ConcurrentHashMap.newKeySet();
    private final OutputDirectory rootOutputDirectory;

    FetchRefVisitor(
        WorkerExecutedMetadata.Builder workerExecutedMetadata,
        Path walkRoot,
        Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
        boolean linkInputDirectories,
        ExclusionSet linkedInputExclusions,
        Set<String> toolInputPaths,
        OutputDirectory rootOutputDirectory) {
      super(workerExecutedMetadata);
      this.walkRoot = walkRoot;
      this.directoriesIndex = directoriesIndex;
      this.linkInputDirectories = linkInputDirectories;
      this.linkedInputExclusions = linkedInputExclusions;
      this.toolInputPaths = toolInputPaths;
      this.rootOutputDirectory = rootOutputDirectory;
    }

    List<String> refKeys() {
      return refKeys;
    }

    List<build.bazel.remote.execution.v2.Digest> refDigests() {
      return refDigests;
    }

    List<FetchResult.Entry> entries() {
      return entries;
    }

    List<String> descendedDirectories() {
      return descendedDirectories;
    }

    ConcurrentHashMap<String, Path> toolInputCasPaths() {
      return toolInputCasPaths;
    }

    Set<String> zeroSizeToolInputPaths() {
      return zeroSizeToolInputPaths;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      // Check before dir.getFileName() which could be null for root paths.
      if (outputDirectoryStack.isEmpty()) {
        outputDirectoryStack.add(rootOutputDirectory);
        return FileVisitResult.CONTINUE;
      }

      OutputDirectory parent = outputDirectoryStack.get(outputDirectoryStack.size() - 1);
      OutputDirectory outputDirectory =
          resolveChildOutputDirectory(parent, dir.getFileName().toString());

      String relativePath = LinkedInputExclusions.pathToRelativeString(walkRoot, dir);

      if (shouldLinkDirectory(
          linkInputDirectories, outputDirectory, linkedInputExclusions, relativePath)) {
        workerExecutedMetadata.addLinkedInputDirectories(relativePath);
        build.buildfarm.v1test.Digest digest = (build.buildfarm.v1test.Digest) attrs.fileKey();
        build.bazel.remote.execution.v2.Digest reapiDigest = DigestUtil.toDigest(digest);
        futures.add(
            transform(
                fileCache.putDirectory(digest, directoriesIndex, fetchService),
                pathResult -> {
                  refDigests.add(reapiDigest);
                  if (pathResult.isMissed()) {
                    fetchedBytes(sumDirectorySize(reapiDigest, directoriesIndex));
                  }
                  entries.add(
                      new FetchResult.Entry(
                          relativePath,
                          FetchResult.EntryType.DIRECTORY,
                          pathResult.path(),
                          null,
                          reapiDigest,
                          null));
                  return null;
                },
                directExecutor()));
        return FileVisitResult.SKIP_SUBTREE;
      }

      // Kept real (descended into rather than linked as a unit): record it so cleanup can remove
      // the real directory once empty. Covers directories excluded only via a recursive output
      // root, which are absent from ExclusionSet.paths().
      descendedDirectories.add(relativePath);
      outputDirectoryStack.add(outputDirectory);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      String relativePath = LinkedInputExclusions.pathToRelativeString(walkRoot, file);

      // Tool inputs are managed through the shared tool root mechanism
      // (see ProtoCoordinator#copyToolInputsIntoWorkerToolRoot), so they are fetched+ref'd
      // into CAS but kept out of entries() — they must not be linked directly into the
      // worker exec root.
      if (toolInputPaths.contains(relativePath)) {
        if (attrs.isRegularFile()) {
          ExecFileSystem.ExecFileAttributes fileAttrs = (ExecFileSystem.ExecFileAttributes) attrs;
          build.buildfarm.v1test.Digest digest = (build.buildfarm.v1test.Digest) attrs.fileKey();
          if (digest.getSize() > 0) {
            String key = fileCache.getKey(digest, fileAttrs.isExecutable());
            futures.add(
                transform(
                    fileCache.put(digest, fileAttrs.isExecutable(), fetchService),
                    pathResult -> {
                      refKeys.add(key);
                      toolInputCasPaths.put(relativePath, pathResult.path());
                      return null;
                    },
                    directExecutor()));
          } else {
            // Zero-size tool input (e.g. _repo_mapping in .runfiles): no CAS entry.
            // Track separately for copyToolInputsIntoWorkerToolRoot to create as empty file.
            zeroSizeToolInputPaths.add(relativePath);
          }
        }
        return FileVisitResult.CONTINUE;
      }

      if (attrs.isSymbolicLink()) {
        // SymlinkNode: no CAS ref, just track for later creation.
        // Validate absolute symlink target against config (matching putSymlink behavior).
        ExecFileSystem.ExecSymlinkAttributes symlinkAttrs =
            (ExecFileSystem.ExecSymlinkAttributes) attrs;
        String target = symlinkAttrs.target();
        Path targetPath = file.getFileSystem().getPath(target);
        if (targetPath.isAbsolute() && !allowSymlinkTargetAbsolute) {
          futures.add(
              immediateFailedFuture(
                  new IOException(
                      "absolute symlink target not allowed: " + target + " for " + file)));
          return FileVisitResult.TERMINATE;
        }
        entries.add(
            new FetchResult.Entry(
                relativePath, FetchResult.EntryType.SYMLINK_NODE, null, null, null, target));
        return FileVisitResult.CONTINUE;
      }

      if (!attrs.isRegularFile()) {
        futures.add(immediateFailedFuture(new IOException("unknown file type for " + file)));
        return FileVisitResult.TERMINATE;
      }

      ExecFileSystem.ExecFileAttributes fileAttrs = (ExecFileSystem.ExecFileAttributes) attrs;
      build.buildfarm.v1test.Digest digest = (build.buildfarm.v1test.Digest) attrs.fileKey();

      if (digest.getSize() == 0) {
        // Zero-size file: no CAS entry, track for creation
        entries.add(
            new FetchResult.Entry(
                relativePath, FetchResult.EntryType.ZERO_SIZE_FILE, null, null, null, null));
        return FileVisitResult.CONTINUE;
      }

      // Regular file: fetch+ref, track CAS path and key
      String key = fileCache.getKey(digest, fileAttrs.isExecutable());
      futures.add(
          transform(
              fileCache.put(digest, fileAttrs.isExecutable(), fetchService),
              pathResult -> {
                refKeys.add(key);
                entries.add(
                    new FetchResult.Entry(
                        relativePath,
                        FetchResult.EntryType.FILE,
                        pathResult.path(),
                        key,
                        null,
                        null));
                return null;
              },
              directExecutor()));
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      outputDirectoryStack.remove(outputDirectoryStack.size() - 1);
      return FileVisitResult.CONTINUE;
    }
  }

  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    DigestFunction.Value digestFunction = rootInputDigestFunction.remove(execDir);
    Iterable<String> inputFiles = rootInputFiles.remove(execDir);
    Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories =
        rootInputDirectories.remove(execDir);
    if (inputFiles != null || inputDirectories != null) {
      fileCache.decrementReferences(
          inputFiles == null ? ImmutableList.of() : inputFiles,
          inputDirectories == null ? ImmutableList.of() : inputDirectories,
          digestFunction);
    }
    super.destroyExecDir(execDir);
  }
}
