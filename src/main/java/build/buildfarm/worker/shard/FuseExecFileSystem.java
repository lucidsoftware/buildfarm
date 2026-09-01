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

package build.buildfarm.worker.shard;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.CFCExecFileSystem;
import build.buildfarm.worker.ExecFileSystem;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.OutputDirectory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/** A FUSE execroot backed by eagerly populated, referenced files in the local CFC. */
class FuseExecFileSystem implements ExecFileSystem {
  private record InputFile(Digest digest, boolean executable) {}

  private final Path root;
  private final Path scratchRoot;
  private final FuseCAS fuseCAS;
  private final CASFileCache fileCache;
  private final ExecutorService fetchService;
  private final ExecutorService removeDirectoryService;
  private final ExecutorService accessRecorder;
  private final Map<Path, List<String>> execRootReferences = new LinkedHashMap<>();

  FuseExecFileSystem(
      Path root,
      Path scratchRoot,
      FuseCAS fuseCAS,
      CASFileCache fileCache,
      ExecutorService fetchService,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder) {
    this.root = root;
    this.scratchRoot = scratchRoot;
    this.fuseCAS = fuseCAS;
    this.fileCache = fileCache;
    this.fetchService = fetchService;
    this.removeDirectoryService = removeDirectoryService;
    this.accessRecorder = accessRecorder;
  }

  @Override
  public UserPrincipal getOwner(String name) {
    return null;
  }

  @Override
  public ListenableFuture<Void> start(
      Consumer<List<Digest>> onDigests, boolean skipLoad, boolean writable)
      throws IOException, InterruptedException {
    Files.createDirectories(root);
    if (Files.exists(scratchRoot)) {
      Directories.remove(scratchRoot, Files.getFileStore(scratchRoot));
    }
    Files.createDirectories(scratchRoot);
    List<Digest> digests = Collections.synchronizedList(new ArrayList<>());
    ListenableFuture<Void> started =
        fileCache.start(digests::add, removeDirectoryService, skipLoad, writable);
    return Futures.transform(
        started,
        ignored -> {
          synchronized (digests) {
            onDigests.accept(List.copyOf(digests));
          }
          return null;
        },
        directExecutor());
  }

  @Override
  public void stop() throws IOException, InterruptedException {
    IOException ioFailure = null;
    InterruptedException interrupted = null;
    List<Path> activeRoots;
    synchronized (execRootReferences) {
      activeRoots = List.copyOf(execRootReferences.keySet());
    }
    for (Path execRoot : activeRoots) {
      try {
        destroyExecDir(execRoot);
      } catch (IOException e) {
        ioFailure = accumulate(ioFailure, e);
      } catch (InterruptedException e) {
        if (interrupted == null) {
          interrupted = e;
        } else {
          interrupted.addSuppressed(e);
        }
      }
    }
    fuseCAS.stop();
    try {
      fileCache.stop();
    } catch (IOException e) {
      ioFailure = accumulate(ioFailure, e);
    } catch (InterruptedException e) {
      if (interrupted == null) {
        interrupted = e;
      } else {
        interrupted.addSuppressed(e);
      }
    } finally {
      MoreExecutors.shutdownAndAwaitTermination(fetchService, 1, TimeUnit.MINUTES);
      MoreExecutors.shutdownAndAwaitTermination(removeDirectoryService, 1, TimeUnit.MINUTES);
      MoreExecutors.shutdownAndAwaitTermination(accessRecorder, 1, TimeUnit.MINUTES);
    }
    if (interrupted != null) {
      if (ioFailure != null) {
        interrupted.addSuppressed(ioFailure);
      }
      throw interrupted;
    }
    if (ioFailure != null) {
      throw ioFailure;
    }
  }

  private static IOException accumulate(@Nullable IOException current, IOException next) {
    if (current == null) {
      return next;
    }
    current.addSuppressed(next);
    return current;
  }

  @Override
  public Path root() {
    return root;
  }

  @Override
  public ContentAddressableStorage getStorage() {
    return fileCache;
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    return fileCache.newInput(compressor, digest, offset);
  }

  @Override
  public Path createExecDir(
      String operationName,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Digest inputRootDigest,
      Command command,
      @Nullable UserPrincipal owner,
      WorkerExecutedMetadata.Builder workerExecutedMetadata)
      throws IOException, InterruptedException {
    if (owner != null) {
      throw new IOException("FUSE execroots do not yet support per-operation owners");
    }
    Path execDir = root.resolve(operationName);
    synchronized (execRootReferences) {
      if (execRootReferences.containsKey(execDir)) {
        throw new IOException("FUSE execroot already exists: " + operationName);
      }
    }
    List<String> references = new ArrayList<>();
    boolean installed = false;
    boolean complete = false;
    try {
      Map<InputFile, Path> localInputs =
          stageInputs(directoriesIndex, inputRootDigest, references, workerExecutedMetadata);
      fuseCAS.createInputRoot(
          operationName,
          inputRootDigest,
          directoriesIndex,
          (digest, executable) -> localInputs.get(new InputFile(digest, executable)));
      installed = true;

      OutputDirectory outputDirectory = CFCExecFileSystem.createOutputDirectory(command);
      outputDirectory.stamp(execDir);
      synchronized (execRootReferences) {
        execRootReferences.put(execDir, ImmutableList.copyOf(references));
      }
      complete = true;
      return execDir;
    } finally {
      if (!complete && installed) {
        try {
          fuseCAS.destroyInputRoot(operationName);
        } finally {
          decrementReferences(references);
        }
      } else if (!complete) {
        decrementReferences(references);
      }
    }
  }

  private Map<InputFile, Path> stageInputs(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Digest inputRootDigest,
      List<String> references,
      WorkerExecutedMetadata.Builder metadata)
      throws IOException, InterruptedException {
    Set<InputFile> inputs = new LinkedHashSet<>();
    collectInputs(inputRootDigest, directoriesIndex, inputs, new HashSet<>());
    Map<InputFile, Path> paths = new LinkedHashMap<>();
    Map<InputFile, ListenableFuture<CASFileCache.PathResult>> staging = new LinkedHashMap<>();
    for (InputFile input : inputs) {
      if (input.digest().getSize() == 0) {
        continue;
      }
      staging.put(input, fileCache.put(input.digest(), input.executable(), fetchService));
    }

    IOException failure = null;
    InterruptedException interrupted = null;
    for (Map.Entry<InputFile, ListenableFuture<CASFileCache.PathResult>> entry :
        staging.entrySet()) {
      InputFile input = entry.getKey();
      String key = CASFileCache.getKey(input.digest(), input.executable());
      try {
        CASFileCache.PathResult result = Uninterruptibles.getUninterruptibly(entry.getValue());
        references.add(key);
        paths.put(input, result.path());
        if (result.isMissed()) {
          synchronized (metadata) {
            metadata.setFetchedBytes(metadata.getFetchedBytes() + input.digest().getSize());
          }
        }
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException ioException) {
          failure = accumulate(failure, ioException);
        } else if (cause instanceof InterruptedException interruptedException) {
          if (interrupted == null) {
            interrupted = interruptedException;
          } else {
            interrupted.addSuppressed(interruptedException);
          }
        } else {
          failure =
              accumulate(
                  failure,
                  new IOException("failed to stage " + DigestUtil.toString(input.digest()), cause));
        }
      }
    }
    if (Thread.interrupted() && interrupted == null) {
      interrupted = new InterruptedException("interrupted while staging FUSE inputs");
    }
    if (interrupted != null) {
      if (failure != null) {
        interrupted.addSuppressed(failure);
      }
      throw interrupted;
    }
    if (failure != null) {
      throw failure;
    }
    return paths;
  }

  private static void collectInputs(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Set<InputFile> inputs,
      Set<build.bazel.remote.execution.v2.Digest> ancestors)
      throws IOException {
    build.bazel.remote.execution.v2.Digest reapiDigest = DigestUtil.toDigest(digest);
    Directory directory =
        digest.getSize() == 0 ? Directory.getDefaultInstance() : directoriesIndex.get(reapiDigest);
    if (directory == null) {
      throw new IOException("directory missing from input index: " + DigestUtil.toString(digest));
    }
    if (!ancestors.add(reapiDigest)) {
      throw new IOException("directory cycle in input index: " + DigestUtil.toString(digest));
    }
    try {
      for (FileNode file : directory.getFilesList()) {
        inputs.add(
            new InputFile(
                DigestUtil.fromDigest(file.getDigest(), digest.getDigestFunction()),
                file.getIsExecutable()));
      }
      for (DirectoryNode child : directory.getDirectoriesList()) {
        collectInputs(
            DigestUtil.fromDigest(child.getDigest(), digest.getDigestFunction()),
            directoriesIndex,
            inputs,
            ancestors);
      }
    } finally {
      ancestors.remove(reapiDigest);
    }
  }

  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    String topdir = root.relativize(execDir).toString();
    List<String> references;
    synchronized (execRootReferences) {
      references = execRootReferences.remove(execDir);
    }
    try {
      fuseCAS.destroyInputRoot(topdir);
    } finally {
      decrementReferences(references == null ? List.of() : references);
    }
  }

  private void decrementReferences(Iterable<String> references) throws IOException {
    IOException failure = null;
    for (String key : references) {
      try {
        fileCache.decrementReference(key);
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
