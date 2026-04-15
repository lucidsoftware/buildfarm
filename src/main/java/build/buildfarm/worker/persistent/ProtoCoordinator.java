// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.persistent;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;
import persistent.bazel.client.CommonsWorkerPool;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkCoordinator;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerSupervisor;

/**
 * Responsible for:
 *
 * <ol>
 *   <li>Initializing a new Worker's file environment correctly
 *   <li>pre-request requirements, e.g. ensuring tool input files
 *   <li>post-response requirements, i.e. putting output files in the right place
 * </ol>
 */
@Log
public class ProtoCoordinator extends WorkCoordinator<RequestCtx, ResponseCtx, CommonsWorkerPool> {
  private static final String WORKER_INIT_LOG_SUFFIX = ".initargs.log";

  private record PendingRequest(PersistentWorker worker, RequestTimeoutHandler task) {
    private PendingRequest {
      Objects.requireNonNull(worker);
      Objects.requireNonNull(task);
    }
  }

  private static final ConcurrentHashMap<RequestCtx, PendingRequest> pendingReqs =
      new ConcurrentHashMap<>();

  @VisibleForTesting
  static boolean hasPendingRequest(RequestCtx request) {
    return pendingReqs.containsKey(request);
  }

  @VisibleForTesting final ScheduledExecutorService timeoutScheduler = createTimeoutScheduler();

  private static ScheduledExecutorService createTimeoutScheduler() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "persistent-worker-timeout");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  // Synchronize writes to the tool input directory per WorkerKey
  // TODO: We only need a Set of WorkerKeys to synchronize on, but no ConcurrentHashSet
  private static final ConcurrentHashMap<WorkerKey, WorkerKey> toolInputSyncs =
      new ConcurrentHashMap<>();

  // Enforces locking on the same object given the same WorkerKey
  private static WorkerKey keyLock(WorkerKey key) {
    return toolInputSyncs.computeIfAbsent(key, k -> k);
  }

  public ProtoCoordinator(CommonsWorkerPool workerPool) {
    super(workerPool);
  }

  private ProtoCoordinator(WorkerSupervisor supervisor, int maxWorkersPerKey) {
    super(new CommonsWorkerPool(supervisor, maxWorkersPerKey));
  }

  // We copy tool inputs from the shared WorkerKey tools directory into our worker exec root,
  //    since there are multiple workers per key,
  //    and presumably there might be writes to tool inputs?
  // Tool inputs which are absolute-paths (e.g. /usr/bin/...) are not affected
  public static ProtoCoordinator ofCommonsPool(int maxWorkersPerKey) {
    WorkerSupervisor loadToolsOnCreate =
        new WorkerSupervisor() {
          @Override
          public PersistentWorker create(WorkerKey workerKey) throws Exception {
            Path keyExecRoot = workerKey.getExecRoot();
            String workerExecDir = getUniqueSubdir(keyExecRoot);
            Path workerExecRoot = keyExecRoot.resolve(workerExecDir);
            copyToolsIntoWorkerExecRoot(workerKey, workerExecRoot);

            Path initArgsLogFile = workerExecRoot.resolve(workerExecDir + WORKER_INIT_LOG_SUFFIX);
            if (!Files.exists(initArgsLogFile)) {
              StringBuilder initArgs = new StringBuilder();
              for (String s : workerKey.getCmd()) {
                initArgs.append(s).append('\n');
              }
              for (String s : workerKey.getArgs()) {
                initArgs.append(s).append('\n');
              }

              Files.write(initArgsLogFile, initArgs.toString().getBytes());
            }
            return new PersistentWorker(workerKey, workerExecDir);
          }
        };
    return new ProtoCoordinator(loadToolsOnCreate, maxWorkersPerKey);
  }

  public void copyToolInputsIntoWorkerToolRoot(
      WorkerKey key, WorkerInputs workerFiles, @Nullable FetchResult fetchResult)
      throws IOException {
    WorkerKey lock = keyLock(key);
    synchronized (lock) {
      try {
        // Copy tool inputs as needed
        Path workToolRoot = key.getToolRoot();
        for (Path opToolPath : workerFiles.opToolInputs) {
          Path workToolPath = workerFiles.relativizeInput(workToolRoot, opToolPath);
          if (!Files.exists(workToolPath)) {
            if (fetchResult != null) {
              // FetchResult path: tool inputs were fetched into CAS by FetchRefVisitor.
              // Copy from CAS path instead of opRoot (which is a lightweight exec dir
              // with no input files).
              String relPath = workerFiles.opRoot.relativize(opToolPath).toString();
              Path casPath = fetchResult.toolInputCasPaths().get(relPath);
              if (casPath == null) {
                if (fetchResult.zeroSizeToolInputPaths().contains(relPath)) {
                  // Zero-size tool input (e.g. _repo_mapping): no CAS entry exists.
                  // Create empty file directly, matching how createExecDir handles size-0
                  // FileNodes.
                  Files.createDirectories(workToolPath.getParent());
                  Files.createFile(workToolPath);
                } else {
                  throw new IOException(
                      "Tool input not found in FetchResult toolInputCasPaths: " + relPath);
                }
              } else {
                FileAccessUtils.copyFile(casPath, workToolPath);
              }
            } else {
              workerFiles.copyInputFile(opToolPath, workToolPath);
            }
          }
        }
      } finally {
        toolInputSyncs.remove(key);
      }
    }
  }

  private static String getUniqueSubdir(Path workRoot) {
    String uuid = UUID.randomUUID().toString();
    while (Files.exists(workRoot.resolve(uuid))) {
      uuid = UUID.randomUUID().toString();
    }
    return uuid;
  }

  // copyToolInputsIntoWorkerToolRoot() should have been called before this.
  private static void copyToolsIntoWorkerExecRoot(WorkerKey key, Path workerExecRoot)
      throws IOException {
    log.log(Level.FINE, "loadToolsIntoWorkerRoot() into: " + workerExecRoot);

    Path toolInputRoot = key.getToolRoot();
    for (Path relPath : key.getWorkerFilesWithHashes().keySet()) {
      Path toolInputPath = toolInputRoot.resolve(relPath);
      Path execRootPath = workerExecRoot.resolve(relPath);

      FileAccessUtils.copyFile(toolInputPath, execRootPath);
    }
  }

  @Override
  public WorkRequest preWorkInit(WorkerKey key, RequestCtx request, PersistentWorker worker)
      throws IOException {
    checkNotNull(request.timeout);
    RequestTimeoutHandler task = new RequestTimeoutHandler(request);
    PendingRequest pendingRequest = new PendingRequest(worker, task);
    PendingRequest alreadyPendingRequest = pendingReqs.putIfAbsent(request, pendingRequest);
    // null means that this request was not in pendingReqs (the expected case)
    if (alreadyPendingRequest != null) {
      if (alreadyPendingRequest.worker != worker) {
        throw new IllegalArgumentException(
            "Already have a persistent worker on the job: " + request.request);
      } else {
        throw new IllegalArgumentException(
            "Got the same request for the same worker while it's running: " + request.request);
      }
    }
    try {
      task.future =
          timeoutScheduler.schedule(
              task, Durations.toMillis(request.timeout), TimeUnit.MILLISECONDS);

      if (request.fetchResult != null) {
        // Link from FetchResult CAS paths (local sym/hardlinks only, no CAS operations)
        log.log(
            Level.FINE,
            () -> "Persistent worker: linking from FetchResult into " + worker.getExecRoot());
        linkFromFetchResult(request, worker.getExecRoot());
      } else {
        log.log(
            Level.FINE,
            () -> "Persistent worker: copying non-tool inputs into " + worker.getExecRoot());
        copyNontoolInputs(request.workerInputs, worker.getExecRoot());
      }
    } catch (Exception e) {
      pendingReqs.remove(request);
      if (task.future != null) {
        task.future.cancel(false);
      }
      throw e;
    }

    return request.request;
  }

  // After the worker has finished, output files need to be visible in the operation directory
  @Override
  public ResponseCtx postWorkCleanup(
      WorkResponse response, PersistentWorker worker, RequestCtx request) throws IOException {
    PendingRequest pendingRequest = pendingReqs.remove(request);

    if (pendingRequest != null && pendingRequest.task.future != null) {
      pendingRequest.task.future.cancel(false);
    }

    // When doWork or preWorkInit throws, Coordinator calls postWorkCleanup(null, ...) for any
    // cleanup that needs to happen despite the failure. Clean up partial links and FetchResult
    // refs.
    if (response == null) {
      try {
        cleanUpLinksAndEmptyDirs(request, worker.getExecRoot());
      } catch (IOException e) {
        log.log(Level.SEVERE, "error cleaning up partial state after failure", e);
      }
      return null;
    }

    try {
      Path workerExecRoot = worker.getExecRoot();
      // Always move outputs and clean up non-tool inputs, regardless of exit code. This matches the
      // REAPI spec as well as what Buildfarm and Bazel do elsewhere.
      moveOutputsToOperationRoot(request.filesContext, workerExecRoot);
      if (request.fetchResult != null) {
        cleanUpLinksAndEmptyDirs(request, workerExecRoot);
      } else {
        cleanUpNontoolInputs(request.workerInputs, workerExecRoot);
      }
    } catch (IOException e) {
      // Ensure FetchResult refs are decremented even if cleanup failed (idempotent)
      if (request.fetchResult != null) {
        request.fetchResult.close();
      }
      throw logBadCleanup(request, e);
    }

    return new ResponseCtx(response, worker.flushStdErr());
  }

  private IOException logBadCleanup(RequestCtx request, IOException e) {
    WorkFilesContext context = request.filesContext;

    StringBuilder sb = new StringBuilder(122);
    sb.append("Output files failure debug for request with args<")
        .append(request.request.getArgumentsList())
        .append(">:\ngetOutputPathsList:\n")
        .append(context.outputPaths)
        .append("getOutputFilesList:\n")
        .append(context.outputFiles)
        .append("getOutputDirectoriesList:\n")
        .append(context.outputDirectories);

    log.log(Level.SEVERE, sb.toString(), e);

    return new IOException("Failed during postWorkCleanup", e);
  }

  private void copyNontoolInputs(WorkerInputs workerInputs, Path workerExecRoot)
      throws IOException {
    for (Path opPath : workerInputs.allInputs.keySet()) {
      if (!workerInputs.allToolInputs.contains(opPath)) {
        Path execPath = workerInputs.relativizeInput(workerExecRoot, opPath);
        workerInputs.copyInputFile(opPath, execPath);
      }
    }
  }

  // Make outputs visible to the rest of Worker machinery
  // see DockerExecutor::copyOutputsOutOfContainer
  void moveOutputsToOperationRoot(WorkFilesContext context, Path workerExecRoot)
      throws IOException {
    Path opRoot = context.opRoot;

    // REAPI >= 2.1: output_paths supersedes output_files and output_directories.
    if (!context.outputPaths.isEmpty()) {
      for (String relOutput : context.outputPaths) {
        Path execOutputPath = workerExecRoot.resolve(relOutput);
        Path opOutputPath = opRoot.resolve(relOutput);
        if (Files.isDirectory(execOutputPath)) {
          FileAccessUtils.moveDirectory(execOutputPath, opOutputPath);
        } else if (Files.exists(execOutputPath)) {
          FileAccessUtils.moveFile(execOutputPath, opOutputPath);
        }
      }
    } else {
      for (String outputDir : context.outputDirectories) {
        Path execOutputPath = workerExecRoot.resolve(outputDir);
        Path opOutputPath = opRoot.resolve(outputDir);
        if (Files.exists(execOutputPath) && Files.isDirectory(execOutputPath)) {
          FileAccessUtils.moveDirectory(execOutputPath, opOutputPath);
        }
      }

      for (String relOutput : context.outputFiles) {
        Path execOutputPath = workerExecRoot.resolve(relOutput);
        Path opOutputPath = opRoot.resolve(relOutput);
        // Don't fail here if the action failed to produce a file.
        // The missing file will be handled just like it is for non-worker actions.
        if (Files.exists(execOutputPath)) {
          FileAccessUtils.moveFile(execOutputPath, opOutputPath);
        }
      }
    }
  }

  private void cleanUpNontoolInputs(WorkerInputs workerInputs, Path workerExecRoot)
      throws IOException {
    for (Path opPath : workerInputs.allInputs.keySet()) {
      if (!workerInputs.allToolInputs.contains(opPath)) {
        workerInputs.deleteInputFileIfExists(workerExecRoot, opPath);
      }
    }
  }

  /**
   * Links non-tool inputs from FetchResult CAS paths into the worker exec root. Each FetchResult
   * entry corresponds 1:1 with a link to create; parent directories are created as needed. An entry
   * whose path is already occupied in the reused worker exec root replaces the leftover — see
   * {@link FileAccessUtils#createSymlink}/{@link FileAccessUtils#createHardlink} for files and
   * directories, and {@link FileAccessUtils#createReplacingOnConflict} for the directly-created
   * zero-size files and symlink nodes.
   */
  private void linkFromFetchResult(RequestCtx request, Path workerExecRoot) throws IOException {
    FetchResult fetchResult = request.fetchResult;
    for (FetchResult.Entry entry : fetchResult.entries()) {
      Path execPath = workerExecRoot.resolve(entry.relativePath());
      switch (entry.type()) {
        case DIRECTORY:
          FileAccessUtils.createSymlink(entry.casPath(), execPath);
          break;
        case FILE:
          FileAccessUtils.createHardlink(entry.casPath(), execPath);
          break;
        case ZERO_SIZE_FILE:
          // Zero-size file: no CAS entry. Ignore the executable bit, matching
          // CFCLinkExecFileSystem.put's size-0 handling.
          FileAccessUtils.createReplacingOnConflict(execPath, Files::createFile);
          break;
        case SYMLINK_NODE:
          FileAccessUtils.createReplacingOnConflict(
              execPath,
              link ->
                  Files.createSymbolicLink(
                      link, link.getFileSystem().getPath(entry.symlinkTarget())));
          break;
      }
      request.trackedLinks.add(execPath);
    }
  }

  /**
   * Deletes tracked links, removes the now-empty real directories the fetch walk descended into
   * (bottom-up), and closes FetchResult refs. Shared by both success and failure cleanup paths.
   */
  private void cleanUpLinksAndEmptyDirs(RequestCtx request, Path workerExecRoot)
      throws IOException {
    IOException cleanupException = null;
    for (Path link : request.trackedLinks) {
      try {
        FileAccessUtils.deleteFileIfExists(link);
      } catch (IOException e) {
        if (cleanupException == null) {
          cleanupException = e;
        } else {
          cleanupException.addSuppressed(e);
        }
      }
    }

    if (request.fetchResult != null) {
      try {
        ImmutableSet<String> descendedDirectories = request.fetchResult.descendedDirectories();
        // Sort by path depth descending (deepest first): a parent always has fewer segments than
        // its
        // child, so descending depth guarantees children are removed before parents — a valid
        // bottom-up order.
        List<String> sortedPaths = new ArrayList<>(descendedDirectories);
        sortedPaths.sort(Comparator.comparingInt(ProtoCoordinator::pathDepth).reversed());
        for (String relativePath : sortedPaths) {
          Path directory = workerExecRoot.resolve(relativePath);
          try {
            if (Files.isDirectory(directory)) {
              try (var entries = Files.list(directory)) {
                if (entries.findFirst().isEmpty()) {
                  Files.delete(directory);
                }
              }
            }
          } catch (IOException e) {
            // Best-effort: the directory may have been removed already, or still hold leftover
            // content. Log rather than swallow so a stuck/undeletable descended directory is
            // visible (e.g. when diagnosing a later directory-symlink collision in the reused
            // worker exec root).
            log.log(
                Level.WARNING,
                "could not remove empty descended directory during cleanup: " + directory,
                e);
          }
        }
      } finally {
        // Decrement all CAS refs (idempotent after a successful close; retryable after failure).
        request.fetchResult.close();
      }
    }

    if (cleanupException != null) {
      throw cleanupException;
    }
  }

  /** Counts path segments: 1 + number of '/' characters. */
  private static int pathDepth(String relativePath) {
    int count = 1;
    for (int i = 0; i < relativePath.length(); i++) {
      if (relativePath.charAt(i) == '/') {
        count++;
      }
    }
    return count;
  }

  @VisibleForTesting
  final class RequestTimeoutHandler implements Runnable {
    private final RequestCtx request;
    volatile ScheduledFuture<?> future;

    @VisibleForTesting
    RequestTimeoutHandler(RequestCtx request) {
      this.request = request;
    }

    @Override
    public void run() {
      try {
        PendingRequest pendingRequest = pendingReqs.remove(this.request);
        if (pendingRequest != null) {
          onTimeout(this.request, pendingRequest.worker);
        }
      } catch (Throwable t) {
        log.log(
            Level.SEVERE,
            "Exception in persistent worker timeout handler for request: " + this.request.request,
            t);
      }
    }
  }

  private void onTimeout(RequestCtx request, PersistentWorker worker) {
    if (worker != null) {
      log.severe("Persistent Worker timed out on request: " + request.request);
      try {
        this.workerPool.invalidateObject(worker.getKey(), worker);
      } catch (Exception e) {
        log.severe(
            "Tried to invalidate worker for request:\n"
                + request
                + "\n\tbut got: "
                + e
                + "\n\nCalling worker.destroy() and moving on.");
        worker.destroy();
      }
    }
  }
}
