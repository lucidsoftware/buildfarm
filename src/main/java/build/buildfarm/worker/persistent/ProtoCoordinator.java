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

import build.buildfarm.common.Time;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 5;

  private record PendingRequest(PersistentWorker worker, RequestTimeoutHandler task) {
    private PendingRequest {
      Objects.requireNonNull(worker);
      Objects.requireNonNull(task);
    }
  }

  private final ConcurrentHashMap<RequestCtx, PendingRequest> pendingReqs =
      new ConcurrentHashMap<>();

  // Per-worker materialization state for incremental input updates.
  // Keyed by worker exec root path (stable across requests for the same worker). A state carries
  // CAS refs for entries that are still linked in that worker's exec root. State lifetime is tied
  // to the worker lifetime and request transitions, not an independent eviction policy.
  private final ConcurrentHashMap<Path, MaterializationState> workerStates =
      new ConcurrentHashMap<>();

  // Tracks in-flight requests for graceful shutdown. Incremented in preWorkInit, decremented in
  // postWorkCleanup. shutdown() waits for this to reach zero before closing workerStates.
  private final AtomicInteger inFlightRequests = new AtomicInteger(0);
  private final Set<RequestCtx> countedInFlightRequests = ConcurrentHashMap.newKeySet();
  private final Object shutdownMonitor = new Object();
  private boolean shuttingDown = false;

  @VisibleForTesting
  boolean hasPendingRequest(RequestCtx request) {
    return pendingReqs.containsKey(request);
  }

  /** Releases all worker materialization refs before CASFileCache.stop(). */
  public void shutdown() {
    shutdown(DEFAULT_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Releases all worker materialization refs, waiting up to {@code timeout} for active requests.
   */
  public void shutdown(long timeout, TimeUnit unit) {
    long deadline = timeoutDeadlineNanos(timeout, unit);
    synchronized (shutdownMonitor) {
      shuttingDown = true;
      while (inFlightRequests.get() > 0) {
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
          break;
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(shutdownMonitor, remainingNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (inFlightRequests.get() > 0) {
      log.warning(
          "Persistent worker coordinator shutdown timed out with "
              + inFlightRequests.get()
              + " request(s) still in flight; invalidating workers before releasing state");
      invalidatePendingWorkersForShutdown();
    }

    for (Path workerExecRoot : ImmutableList.copyOf(workerStates.keySet())) {
      closeWorkerState(workerExecRoot);
    }
  }

  private static long timeoutDeadlineNanos(long timeout, TimeUnit unit) {
    return Time.deadlineNanosFromNow(checkNotNull(unit).toNanos(timeout));
  }

  private void invalidatePendingWorkersForShutdown() {
    for (RequestCtx request : ImmutableList.copyOf(pendingReqs.keySet())) {
      PendingRequest pendingRequest = pendingReqs.remove(request);
      if (pendingRequest != null) {
        if (pendingRequest.task.future != null) {
          pendingRequest.task.future.cancel(false);
        }
        invalidateWorker(request, pendingRequest.worker);
      }
    }
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
  private final ConcurrentHashMap<WorkerKey, WorkerKey> toolInputSyncs = new ConcurrentHashMap<>();

  // Enforces locking on the same object given the same WorkerKey
  private WorkerKey keyLock(WorkerKey key) {
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
    // AtomicReference provides JMM visibility guarantees for the coordinator reference
    // accessed by the anonymous WorkerSupervisor from pool threads.
    final AtomicReference<ProtoCoordinator> coordinatorRef = new AtomicReference<>();
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

          @Override
          public void destroyObject(
              WorkerKey key, org.apache.commons.pool2.PooledObject<PersistentWorker> p) {
            PersistentWorker worker = p.getObject();
            try {
              super.destroyObject(key, p);
            } finally {
              ProtoCoordinator coordinator = coordinatorRef.get();
              if (coordinator != null) {
                coordinator.closeWorkerState(worker.getExecRoot());
              }
            }
          }
        };
    ProtoCoordinator coordinator = new ProtoCoordinator(loadToolsOnCreate, maxWorkersPerKey);
    coordinatorRef.set(coordinator);
    return coordinator;
  }

  private void closeWorkerState(Path workerExecRoot) {
    MaterializationState state = workerStates.remove(workerExecRoot);
    if (state != null) {
      state.close();
    }
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
              String relativePath = workerFiles.opRoot.relativize(opToolPath).toString();
              Path casPath = fetchResult.toolInputCasPaths().get(relativePath);
              if (casPath == null) {
                if (fetchResult.zeroSizeToolInputPaths().contains(relativePath)) {
                  // Zero-size tool input (e.g. _repo_mapping): no CAS entry exists.
                  // Create empty file directly, matching how createExecDir handles size-0
                  // FileNodes.
                  Files.createDirectories(workToolPath.getParent());
                  Files.createFile(workToolPath);
                } else {
                  throw new IOException(
                      "Tool input not found in FetchResult toolInputCasPaths: " + relativePath);
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
    registerInFlightRequest(request);
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
        // Phase 2/3 path: link from FetchResult CAS paths (local sym/hardlinks only, no CAS
        // operations). On a reused worker, diff against the previously materialized state and
        // apply only the delta; otherwise materialize from scratch.
        Path workerExecRoot = worker.getExecRoot();
        MaterializationState previousState = workerStates.get(workerExecRoot);
        if (previousState != null) {
          // Incremental update: diff against previous state and apply delta
          MaterializationState.DiffResult diffResult = previousState.diff(request.fetchResult);
          if (diffResult.descendedDirectoriesChanged()) {
            // descendedDirectories changed — directory structure is different, full
            // re-materialization needed
            log.log(
                Level.FINE,
                () -> "Persistent worker: full re-materialization into " + workerExecRoot);
            cleanUpPreviousState(previousState, workerExecRoot);
            linkFromFetchResult(request, workerExecRoot);
          } else {
            log.log(
                Level.FINE,
                () ->
                    "Persistent worker: incremental update into "
                        + workerExecRoot
                        + " (added="
                        + diffResult.added().size()
                        + " removed="
                        + diffResult.removed().size()
                        + " changed="
                        + diffResult.changed().size()
                        + ")");
            applyDelta(diffResult, request, workerExecRoot);
          }
        } else {
          // First request to this worker — full materialization
          log.log(Level.FINE, () -> "Persistent worker: first-time linking into " + workerExecRoot);
          linkFromFetchResult(request, workerExecRoot);
        }
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

  private void registerInFlightRequest(RequestCtx request) throws IOException {
    synchronized (shutdownMonitor) {
      if (shuttingDown) {
        throw new IOException("Persistent worker coordinator is shutting down");
      }
      if (countedInFlightRequests.add(request)) {
        inFlightRequests.incrementAndGet();
      }
    }
  }

  private void unregisterInFlightRequest(RequestCtx request) {
    if (!countedInFlightRequests.remove(request)) {
      return;
    }
    int remaining = inFlightRequests.decrementAndGet();
    if (remaining == 0) {
      synchronized (shutdownMonitor) {
        shutdownMonitor.notifyAll();
      }
    }
  }

  private boolean isShuttingDown() {
    synchronized (shutdownMonitor) {
      return shuttingDown;
    }
  }

  // After the worker has finished, output files need to be visible in the operation directory
  @Override
  public ResponseCtx postWorkCleanup(
      WorkResponse response, PersistentWorker worker, RequestCtx request) throws IOException {
    try {
      PendingRequest pendingRequest = pendingReqs.remove(request);

      if (pendingRequest != null && pendingRequest.task.future != null) {
        pendingRequest.task.future.cancel(false);
      }

      // When doWork or preWorkInit throws, Coordinator calls postWorkCleanup(null, ...) for any
      // cleanup that needs to happen despite the failure. Clean up partial links and FetchResult
      // refs.
      if (response == null) {
        // Capture previous state — needed to clean up unchanged entries that persist in the
        // exec root from the previous request's incremental reuse.
        MaterializationState previousState = workerStates.get(worker.getExecRoot());
        try {
          cleanUpPartialState(request, worker, previousState);
        } catch (IOException e) {
          log.log(Level.SEVERE, "error cleaning up partial state after failure", e);
        }
        // Close after links are deleted so CAS refs are not decremented while links to those CAS
        // entries still exist on disk.
        closeWorkerState(worker.getExecRoot());
        return null;
      }

      try {
        Path workerExecRoot = worker.getExecRoot();
        // Always move outputs and clean up non-tool inputs, regardless of exit code. This matches
        // the REAPI spec as well as what Buildfarm and Bazel do elsewhere.
        moveOutputsToOperationRoot(request.filesContext, workerExecRoot);
        if (request.fetchResult != null) {
          if (pendingRequest == null || isShuttingDown()) {
            // Timeout/shutdown means this worker will not be reused. Do not publish fresh state
            // after shutdown may already have drained the state map.
            request.fetchResult.close();
          } else {
            transferFetchResultToState(request, workerExecRoot);
          }
        } else {
          cleanUpNontoolInputs(request.workerInputs, workerExecRoot);
        }
      } catch (IOException e) {
        // A failure here (e.g. moveOutputsToOperationRoot) can leave the exec root holding this
        // request's applied delta while workerStates still holds the previous request's state — a
        // stale baseline for the next diff. Close it so the next request re-materializes from
        // scratch. The worker is also invalidated by the Coordinator loop on this throw, but only
        // if pool invalidation succeeds; closing here covers that gap.
        closeWorkerState(worker.getExecRoot());
        // Ensure FetchResult refs are decremented even if cleanup failed (idempotent)
        if (request.fetchResult != null) {
          request.fetchResult.close();
        }
        throw logBadCleanup(request, e);
      }

      return new ResponseCtx(response, worker.flushStdErr());
    } finally {
      unregisterInFlightRequest(request);
    }
  }

  /**
   * On a successful request, snapshots the FetchResult into a MaterializationState that takes over
   * its CAS refs and stores it as the baseline for the next request's incremental diff. Entries
   * whose paths {@link #moveOutputsToOperationRoot} just moved out of the exec root as declared
   * outputs are omitted from the snapshot — their on-disk links are gone, so the next diff must
   * re-link them rather than treat them as unchanged.
   *
   * <p>The snapshot is built before {@link FetchResult#markTransferred()} takes ref ownership: if
   * construction fails (e.g. an unexpected duplicate input path), the FetchResult is still OPEN, so
   * its refs are released by close() rather than orphaned.
   */
  private void transferFetchResultToState(RequestCtx request, Path workerExecRoot) {
    ImmutableSet<String> movedOutputPaths =
        movedOutputEntryPaths(request.filesContext, request.fetchResult.entries());
    MaterializationState newState;
    try {
      newState =
          MaterializationState.fromFetchResultWithRefs(request.fetchResult, movedOutputPaths);
    } catch (RuntimeException e) {
      // Snapshot construction failed before ref ownership transferred. The FetchResult is still
      // OPEN — release its refs and drop the stale baseline, then rethrow.
      closeWorkerState(workerExecRoot);
      request.fetchResult.close();
      throw e;
    }
    if (!request.fetchResult.markTransferred()) {
      // close() already ran — refs were already decremented. Don't store a state that would
      // double-decrement the same refs.
      log.log(
          Level.WARNING,
          "FetchResult already closed before transfer — skipping state storage for "
              + workerExecRoot);
      return;
    }
    // markTransferred() succeeded, so close() is now a no-op on the FetchResult.
    // Since new refs were incremented before this request (fetchAndRefInputs), and old refs are
    // decremented only after the replacement, no CAS entry ever drops to 0 refs while in use.
    MaterializationState previousState;
    try {
      previousState = workerStates.put(workerExecRoot, newState);
    } catch (Throwable t) {
      // put() failed (e.g., OOM). newState owns refs but was never stored — compensate by
      // releasing them. This compensation covers the put() ALONE: once newState is published it
      // owns its refs unconditionally, so no later failure may decrement them.
      newState.close();
      throw t;
    }
    // newState is now published and owns its refs. Releasing the PREVIOUS state's refs is kept out
    // of the compensation try above: a failure here must not touch newState. decrementReferences
    // can throw (including unchecked), but previousState.close() swallows that, logs it, and leaves
    // the OLD refs un-decremented — leaking the old refs is far safer than decrementing newState's
    // refs while its links are still live in the exec root.
    if (previousState != null) {
      previousState.close();
    }
  }

  /**
   * Relative paths of input entries that {@link #moveOutputsToOperationRoot} moved out of the exec
   * root as declared outputs, so they must be omitted from the next MaterializationState. Mirrors
   * moveOutputsToOperationRoot's branching exactly: {@code outputPaths} supersedes {@code
   * outputFiles}/{@code outputDirectories}. An entry matches if its path equals an output (file or
   * path) or is a descendant of an output directory / output path.
   *
   * <p>Keyed off declared outputs, not disk existence: an input link placed at an output path
   * always exists, so the move always relocates it. Compared in the same coordinate space (raw
   * output strings, no working_directory offset) that moveOutputsToOperationRoot resolves against,
   * so the exclusion matches exactly what was moved.
   */
  private static ImmutableSet<String> movedOutputEntryPaths(
      WorkFilesContext context, ImmutableList<FetchResult.Entry> entries) {
    if (context.outputPaths.isEmpty()
        && context.outputFiles.isEmpty()
        && context.outputDirectories.isEmpty()) {
      return ImmutableSet.of();
    }

    Set<String> exactOutputs = new HashSet<>();
    List<String> outputDirPrefixes = new ArrayList<>();
    if (!context.outputPaths.isEmpty()) {
      // REAPI >= 2.1: output_paths supersedes output_files/output_directories. Each may be a file
      // or a directory, so treat every one as both an exact match and a directory ancestor.
      for (String outputPath : context.outputPaths) {
        exactOutputs.add(outputPath);
        outputDirPrefixes.add(outputPath + "/");
      }
    } else {
      for (String outputDirectory : context.outputDirectories) {
        exactOutputs.add(outputDirectory);
        outputDirPrefixes.add(outputDirectory + "/");
      }
      exactOutputs.addAll(context.outputFiles);
    }

    ImmutableSet.Builder<String> moved = ImmutableSet.builder();
    for (FetchResult.Entry entry : entries) {
      String relativePath = entry.relativePath();
      if (exactOutputs.contains(relativePath) || hasPrefixIn(relativePath, outputDirPrefixes)) {
        moved.add(relativePath);
      }
    }
    return moved.build();
  }

  private static boolean hasPrefixIn(String path, List<String> prefixes) {
    for (String prefix : prefixes) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
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

  /** Removes all links from a previous materialization state before a full re-materialization. */
  private void cleanUpPreviousState(MaterializationState previousState, Path workerExecRoot)
      throws IOException {
    for (FetchResult.Entry entry : previousState.entriesByPath().values()) {
      Path execPath = workerExecRoot.resolve(entry.relativePath());
      FileAccessUtils.deleteExistingPath(execPath);
    }
    removeEmptyDescendedDirs(previousState.descendedDirectories(), workerExecRoot);
    // Previous state's CAS refs (if any) are released when the state is replaced or the worker is
    // destroyed. This method only removes filesystem links; ref management is handled by
    // MaterializationState.close().
  }

  /**
   * Applies an incremental delta to the worker exec root. Unchanged entries are left in place,
   * preserving the in-place links that back the previous MaterializationState's carried CAS refs.
   * CAS refs for added/changed entries were already incremented by InputFetcher's fetchAndRefInputs
   * before this request; the previous state's refs are decremented only after this request succeeds
   * and its FetchResult refs are transferred to a new MaterializationState. That
   * increment-before-decrement ordering guarantees no CAS entry drops to zero refs while it is
   * still linked into the exec root.
   */
  private void applyDelta(
      MaterializationState.DiffResult diff, RequestCtx request, Path workerExecRoot)
      throws IOException {
    for (FetchResult.Entry entry : diff.removed()) {
      Path execPath = workerExecRoot.resolve(entry.relativePath());
      FileAccessUtils.deleteExistingPath(execPath);
    }

    for (FetchResult.Entry entry : diff.added()) {
      linkEntry(entry, request, workerExecRoot);
    }

    // Delete then re-create is safe because the persistent worker is idle between requests
    // (waiting for WorkRequest), so no concurrent reader exists. A Buildfarm crash between
    // delete and create loses the in-memory workerStates, forcing full re-materialization.
    for (FetchResult.Entry entry : diff.changed()) {
      Path execPath = workerExecRoot.resolve(entry.relativePath());
      // A changed entry's path is always occupied. Pre-delete recursively (NOFOLLOW) so a type
      // change whose prior on-disk form is a real directory — or a leftover symlink — is removed
      // rather than followed into the shared CAS tree, then re-link onto the now-clear path.
      FileAccessUtils.deleteExistingPath(execPath);
      linkEntry(entry, request, workerExecRoot);
    }

    // Walk up from each removed entry to workerExecRoot, deleting empty dirs.
    // Runs after added entries are created so non-empty dirs are preserved.
    // Changed entries are excluded — they always have a replacement at the same path.
    if (!diff.removed().isEmpty()) {
      for (FetchResult.Entry entry : diff.removed()) {
        removeEmptyParents(workerExecRoot.resolve(entry.relativePath()), workerExecRoot);
      }
      // Also clean descended dirs (may now be empty after removals).
      // Use the new request's descended set for clarity — guaranteed identical to previousState's
      // since applyDelta is only called when !descendedDirectoriesChanged.
      removeEmptyDescendedDirs(request.fetchResult.descendedDirectories(), workerExecRoot);
    }
  }

  /**
   * Creates a single link entry in the worker exec root. Each entry corresponds 1:1 with a link to
   * create; parent directories are created as needed. An entry whose path is already occupied in
   * the reused worker exec root replaces the leftover — see {@link
   * FileAccessUtils#createSymlink}/{@link FileAccessUtils#createHardlink} for directories and
   * files, and {@link FileAccessUtils#createReplacingOnConflict} for the directly-created zero-size
   * files and symlink nodes. The replace cost is paid only on a genuine collision; the common
   * clear-path create is a single syscall.
   */
  private void linkEntry(FetchResult.Entry entry, RequestCtx request, Path workerExecRoot)
      throws IOException {
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

  private void linkFromFetchResult(RequestCtx request, Path workerExecRoot) throws IOException {
    for (FetchResult.Entry entry : request.fetchResult.entries()) {
      linkEntry(entry, request, workerExecRoot);
    }
  }

  /**
   * Cleans up partial state after a failure (null response path). Deletes links created during this
   * request, any unchanged entries from the previous request's incremental reuse, removes the
   * now-empty real directories the fetch walk descended into (bottom-up), and closes FetchResult
   * refs. Deletion is best-effort: a failure on one entry is recorded and rethrown after the rest
   * have been attempted, so a single stuck path does not strand other links or skip the ref close.
   */
  private void cleanUpPartialState(
      RequestCtx request, PersistentWorker worker, @Nullable MaterializationState previousState)
      throws IOException {
    Path workerExecRoot = worker.getExecRoot();
    IOException cleanupException = null;
    // Delete any links created during this request
    for (Path link : request.trackedLinks) {
      try {
        FileAccessUtils.deleteExistingPath(link);
      } catch (IOException e) {
        if (cleanupException == null) {
          cleanupException = e;
        } else {
          cleanupException.addSuppressed(e);
        }
      }
    }
    // Delete unchanged entries that persisted from the previous request's incremental reuse
    if (previousState != null) {
      for (FetchResult.Entry entry : previousState.entriesByPath().values()) {
        Path execPath = workerExecRoot.resolve(entry.relativePath());
        try {
          FileAccessUtils.deleteExistingPath(execPath);
        } catch (IOException e) {
          if (cleanupException == null) {
            cleanupException = e;
          } else {
            cleanupException.addSuppressed(e);
          }
        }
      }
      removeEmptyDescendedDirs(previousState.descendedDirectories(), workerExecRoot);
    }
    // Clean empty descended dirs from the current request. writeFileSafe creates intermediate
    // directories during linking; after deleting tracked links those dirs may be empty.
    if (request.fetchResult != null) {
      removeEmptyDescendedDirs(request.fetchResult.descendedDirectories(), workerExecRoot);
      // Close FetchResult to decrement CAS refs (idempotent)
      request.fetchResult.close();
    }

    if (cleanupException != null) {
      throw cleanupException;
    }
  }

  /**
   * Walks up from the given path's parent to stopAt, removing empty non-symlink directories. Stops
   * at the first non-empty directory or at stopAt.
   */
  private static void removeEmptyParents(Path path, Path stopAt) {
    Path parent = path.getParent();
    if (parent == null || !parent.startsWith(stopAt)) {
      return;
    }
    while (parent != null && !parent.equals(stopAt)) {
      try {
        if (Files.isDirectory(parent) && !Files.isSymbolicLink(parent)) {
          try (var entries = Files.list(parent)) {
            if (entries.findFirst().isEmpty()) {
              Files.delete(parent);
            } else {
              break; // non-empty, stop walking up
            }
          }
        } else {
          break;
        }
      } catch (IOException e) {
        // Best-effort: the directory may have been removed already, or still hold leftover
        // content. Log rather than swallow so a stuck/undeletable directory in the reused worker
        // exec root stays visible (e.g. when diagnosing a later directory-symlink collision).
        log.log(
            Level.WARNING, "could not remove empty parent directory during cleanup: " + parent, e);
        break;
      }
      parent = parent.getParent();
    }
  }

  private static void removeEmptyDescendedDirs(
      ImmutableSet<String> descendedDirectories, Path workerExecRoot) {
    // Sort by path depth descending (deepest first): a parent always has fewer segments than its
    // child, so descending depth guarantees children are removed before parents — a valid
    // bottom-up order.
    List<String> sortedPaths = new ArrayList<>(descendedDirectories);
    sortedPaths.sort(Comparator.comparingInt(ProtoCoordinator::pathDepth).reversed());
    for (String relativePath : sortedPaths) {
      Path directory = workerExecRoot.resolve(relativePath);
      try {
        // Guard against symlinks — a symlink to a CAS directory should not be treated as a
        // real directory here. isDirectory follows symlinks, so check !isSymbolicLink first.
        if (Files.isDirectory(directory) && !Files.isSymbolicLink(directory)) {
          try (var entries = Files.list(directory)) {
            if (entries.findFirst().isEmpty()) {
              Files.delete(directory);
            }
          }
        }
      } catch (IOException e) {
        // Best-effort: the directory may have been removed already, or still hold leftover
        // content. Log rather than swallow so a stuck/undeletable descended directory stays
        // visible (e.g. when diagnosing a later directory-symlink collision in the reused worker
        // exec root).
        log.log(
            Level.WARNING,
            "could not remove empty descended directory during cleanup: " + directory,
            e);
      }
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
      invalidateWorker(request, worker);
    }
  }

  private void invalidateWorker(RequestCtx request, PersistentWorker worker) {
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
    } finally {
      // Clear incremental state only after the worker has been destroyed/invalidated.
      closeWorkerState(worker.getExecRoot());
    }
  }
}
