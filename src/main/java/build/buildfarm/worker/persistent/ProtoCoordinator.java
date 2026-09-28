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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import build.buildfarm.common.config.PersistentWorkers;
import build.buildfarm.common.io.Directories;
import com.google.common.annotations.VisibleForTesting;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.apache.commons.pool2.PooledObject;
import persistent.bazel.client.CommonsWorkerPool;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkCoordinator;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerResources;
import persistent.bazel.client.WorkerSupervisor;
import persistent.common.PoolExhaustedException;

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
public class ProtoCoordinator extends WorkCoordinator<RequestCtx, ResponseCtx, CommonsWorkerPool>
    implements AutoCloseable {
  private static final String WORKER_INIT_LOG_SUFFIX = ".initargs.log";

  @VisibleForTesting final PersistentWorkerLifecycle lifecycle;
  private final PersistentWorkerIdleMonitor idleMonitor;

  private record PendingRequest(PersistentWorkerLifecycle.Lease lease, RequestTimeoutHandler task) {
    private PendingRequest {
      Objects.requireNonNull(lease);
      Objects.requireNonNull(task);
    }
  }

  private final ConcurrentHashMap<RequestCtx, PendingRequest> pendingReqs =
      new ConcurrentHashMap<>();

  @VisibleForTesting
  boolean hasPendingRequest(RequestCtx request) {
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

  private Duration poolWaitTimeout = Duration.ofMillis(1000);

  public ProtoCoordinator(CommonsWorkerPool workerPool) {
    super(workerPool);
    lifecycle = new PersistentWorkerLifecycle();
    idleMonitor = PersistentWorkerIdleMonitor.disabled(lifecycle);
  }

  private ProtoCoordinator(
      WorkerSupervisor supervisor,
      int maxWorkersPerKey,
      PersistentWorkers settings,
      PersistentWorkerLifecycle lifecycle,
      PersistentWorkerIdleMonitor idleMonitor) {
    super(
        new CommonsWorkerPool(
            supervisor,
            maxWorkersPerKey,
            settings.getMaxWorkersTotal(),
            settings.getWarmIdleWorkersPerKey(),
            settings.getIdleRetirementMode() == PersistentWorkers.IdleRetirementMode.ENABLED
                ? Duration.ofSeconds(settings.getIdleCheckIntervalSeconds())
                : Duration.ofMillis(-1),
            new PersistentWorkerEvictionPolicy(
                lifecycle,
                Duration.ofSeconds(settings.getIdleTimeoutSeconds()),
                settings.getWarmIdleWorkersPerKey())));
    this.poolWaitTimeout = Duration.ofMillis(settings.getPoolWaitTimeoutMillis());
    this.lifecycle = lifecycle;
    this.idleMonitor = idleMonitor;
  }

  // We copy tool inputs from the shared WorkerKey tools directory into our worker exec root,
  //    since there are multiple workers per key,
  //    and presumably there might be writes to tool inputs?
  // Tool inputs which are absolute-paths (e.g. /usr/bin/...) are not affected
  public static ProtoCoordinator ofCommonsPool(int maxWorkersPerKey) {
    return ofCommonsPool(maxWorkersPerKey, new PersistentWorkers());
  }

  public static ProtoCoordinator ofCommonsPool(int maxWorkersPerKey, PersistentWorkers settings) {
    validateSettings(maxWorkersPerKey, settings);
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorkerIdleMonitor idleMonitor = PersistentWorkerIdleMonitor.from(settings, lifecycle);
    WorkerSupervisor loadToolsOnCreate =
        new WorkerSupervisor() {
          @Override
          public PersistentWorker create(WorkerKey workerKey) throws Exception {
            Path keyExecRoot = workerKey.getExecRoot();
            String workerExecDir = getUniqueSubdir(keyExecRoot);
            Path workerExecRoot = keyExecRoot.resolve(workerExecDir);
            Files.createDirectories(workerExecRoot);
            long toolSetupStarted = PersistentWorkerMetrics.startTimer();
            try {
              copyToolsIntoWorkerExecRoot(workerKey, workerExecRoot);
            } finally {
              PersistentWorkerMetrics.observePhase(
                  PersistentWorkerMetrics.PHASE_TOOL_SETUP, toolSetupStarted);
            }

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
            long workerStartStarted = PersistentWorkerMetrics.startTimer();
            try {
              PersistentWorker worker = new PersistentWorker(workerKey, workerExecDir);
              lifecycle.register(worker);
              idleMonitor.onIdle(worker);
              PersistentWorkerMetrics.workerStarted(worker);
              return worker;
            } finally {
              PersistentWorkerMetrics.observePhase(
                  PersistentWorkerMetrics.PHASE_WORKER_START, workerStartStarted);
            }
          }

          @Override
          public boolean validateObject(WorkerKey key, PooledObject<PersistentWorker> pooled) {
            boolean valid = super.validateObject(key, pooled);
            if (!valid) {
              PersistentWorkerMetrics.markDestroyReason(
                  pooled.getObject(), PersistentWorkerMetrics.DESTROY_UNEXPECTED_EXIT);
            }
            return valid;
          }

          @Override
          public void destroyObject(WorkerKey key, PooledObject<PersistentWorker> pooled) {
            PersistentWorker worker = pooled.getObject();
            idleMonitor.onUnavailable(worker);
            lifecycle.beginRetiring(worker);
            try {
              boolean terminated =
                  worker.terminate(Duration.ofSeconds(settings.getGracefulTerminationSeconds()));
              if (terminated) {
                removeWorkerExecRoot(key, worker);
              } else {
                PersistentWorkerMetrics.terminationFailure();
                log.severe(
                    "Persistent worker process tree did not terminate; preserving exec root: "
                        + worker.getExecRoot());
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              worker.destroy();
              PersistentWorkerMetrics.terminationFailure();
              log.log(Level.WARNING, "Interrupted while terminating persistent worker", e);
            } finally {
              lifecycle.terminated(worker);
              PersistentWorkerMetrics.workerDestroyed(worker);
            }
          }
        };
    return new ProtoCoordinator(
        loadToolsOnCreate, maxWorkersPerKey, settings, lifecycle, idleMonitor);
  }

  private static void validateSettings(int maxWorkersPerKey, PersistentWorkers settings) {
    checkNotNull(settings);
    checkNotNull(settings.getIdleRetirementMode());
    checkArgument(
        settings.getPoolWaitTimeoutMillis() >= 0, "poolWaitTimeoutMillis must not be negative");
    checkArgument(maxWorkersPerKey > 0, "maxWorkersPerKey must be positive");
    checkArgument(
        settings.getMaxWorkersTotal() == -1 || settings.getMaxWorkersTotal() > 0,
        "maxWorkersTotal must be positive or -1");
    checkArgument(
        settings.getWarmIdleWorkersPerKey() >= 0
            && settings.getWarmIdleWorkersPerKey() <= maxWorkersPerKey,
        "warmIdleWorkersPerKey must be between zero and maxWorkersPerKey");
    checkArgument(settings.getIdleTimeoutSeconds() > 0, "idleTimeoutSeconds must be positive");
    if (settings.getIdleRetirementMode() == PersistentWorkers.IdleRetirementMode.ENABLED) {
      checkArgument(
          settings.getIdleCheckIntervalSeconds() > 0,
          "idleCheckIntervalSeconds must be positive when idle retirement is enabled");
    }
    checkArgument(
        settings.getGracefulTerminationSeconds() >= 0,
        "gracefulTerminationSeconds must not be negative");
  }

  private static void removeWorkerExecRoot(WorkerKey key, PersistentWorker worker) {
    Path workerExecRoot = worker.getExecRoot().toAbsolutePath().normalize();
    Path keyExecRoot = key.getExecRoot().toAbsolutePath().normalize();
    if (!keyExecRoot.equals(workerExecRoot.getParent())) {
      log.severe(
          "Refusing to remove persistent worker exec root outside its key root: " + workerExecRoot);
      return;
    }
    if (!Files.exists(workerExecRoot)) {
      return;
    }
    try {
      Directories.remove(workerExecRoot, Files.getFileStore(workerExecRoot));
    } catch (IOException e) {
      log.log(Level.WARNING, "Could not remove persistent worker exec root " + workerExecRoot, e);
    }
  }

  @Override
  public void close() {
    idleMonitor.close();
    for (PendingRequest pendingRequest : pendingReqs.values()) {
      if (pendingRequest.task().future != null) {
        pendingRequest.task().future.cancel(false);
      }
    }
    pendingReqs.clear();
    timeoutScheduler.shutdownNow();
    workerPool.close();
  }

  @Override
  public ResponseCtx runRequest(WorkerKey workerKey, RequestCtx request) throws Exception {
    PersistentWorkerMetrics.requestStarted();
    try {
      return runRequestTimed(workerKey, request);
    } finally {
      PersistentWorkerMetrics.observeRequest(request.outcome(), request.metricsStartedNanos);
      PersistentWorkerMetrics.requestFinished();
    }
  }

  private ResponseCtx runRequestTimed(WorkerKey workerKey, RequestCtx request) throws Exception {
    PersistentWorker worker;
    long poolWaitStarted = PersistentWorkerMetrics.startTimer();
    PersistentWorkerMetrics.poolWaitStarted();
    try {
      Duration requestBudget = Duration.ofNanos(Math.max(0, Durations.toNanos(request.timeout)));
      worker =
          workerPool.obtain(
              workerKey,
              poolWaitTimeout.compareTo(requestBudget) < 0 ? poolWaitTimeout : requestBudget);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        request.setOutcome(PersistentWorkerMetrics.OUTCOME_INTERRUPTED);
      } else if (e instanceof PoolExhaustedException) {
        request.setOutcome(PersistentWorkerMetrics.OUTCOME_POOL_TIMEOUT);
      } else {
        request.setOutcome(PersistentWorkerMetrics.OUTCOME_WORKER_ERROR);
      }
      throw e;
    } finally {
      PersistentWorkerMetrics.observePhase(
          PersistentWorkerMetrics.PHASE_POOL_WAIT, poolWaitStarted);
      PersistentWorkerMetrics.poolWaitFinished();
    }

    PersistentWorkerMetrics.workerBorrowed(worker);
    PersistentWorkerLifecycle.Lease lease = lifecycle.lease(worker, requestId(request));
    idleMonitor.onUnavailable(worker);
    boolean postWorkCleanupCalled = false;
    try {
      request.setOutcome(PersistentWorkerMetrics.OUTCOME_INPUT_SETUP_FAILURE);
      WorkRequest workRequest = preWorkInit(workerKey, request, worker, lease);

      request.setOutcome(PersistentWorkerMetrics.OUTCOME_WORKER_ERROR);
      WorkResponse workResponse;
      long executionStarted = PersistentWorkerMetrics.startTimer();
      try {
        WorkerResources resources = lifecycle.resourcesFor(lease);
        workResponse = request.execution.run(resources, () -> worker.doWork(workRequest));
        // Quiesce the process before copying outputs or making it available to another lease.
        resources.idle();
      } finally {
        PersistentWorkerMetrics.observePhase(
            PersistentWorkerMetrics.PHASE_WORKER_EXECUTION, executionStarted);
      }

      request.setOutcome(PersistentWorkerMetrics.OUTCOME_OUTPUT_CLEANUP_FAILURE);
      postWorkCleanupCalled = true;
      ResponseCtx responseAfterCleanup = postWorkCleanup(workResponse, worker, request);

      request.setOutcome(
          workResponse.getExitCode() == 0
              ? PersistentWorkerMetrics.OUTCOME_SUCCESS
              : PersistentWorkerMetrics.OUTCOME_ACTION_FAILURE);
      String completedOutcome = request.outcome();
      request.setOutcome(PersistentWorkerMetrics.OUTCOME_WORKER_ERROR);
      checkState(
          lifecycle.release(lease), "persistent worker lease changed before a successful return");
      workerPool.release(workerKey, worker);
      idleMonitor.onIdle(worker);
      PersistentWorkerMetrics.workerReturned(worker);
      request.setOutcome(completedOutcome);
      return responseAfterCleanup;
    } catch (Exception e) {
      if (e instanceof TimeoutException) {
        request.markTimedOut();
      }
      if (e instanceof InterruptedException) {
        request.setOutcome(PersistentWorkerMetrics.OUTCOME_INTERRUPTED);
      } else if (request.timedOut()) {
        request.setOutcome(PersistentWorkerMetrics.OUTCOME_WORKER_TIMEOUT);
      }
      if (!postWorkCleanupCalled) {
        try {
          postWorkCleanup(null, worker, request);
        } catch (Exception cleanupEx) {
          e.addSuppressed(cleanupEx);
        }
      }
      PersistentWorkerMetrics.markDestroyReason(
          worker,
          request.timedOut()
              ? PersistentWorkerMetrics.DESTROY_TIMEOUT
              : PersistentWorkerMetrics.DESTROY_REQUEST_FAILURE);
      lifecycle.beginRetiring(lease);
      idleMonitor.onUnavailable(worker);
      try {
        workerPool.invalidate(workerKey, worker);
      } catch (Exception invalidateEx) {
        // The timeout handler may already have invalidated this worker.
        e.addSuppressed(invalidateEx);
        worker.destroy();
        PersistentWorkerMetrics.workerDestroyed(worker);
      }
      throw e;
    }
  }

  private static String requestId(RequestCtx request) {
    String operationName =
        request.filesContext == null ? "" : request.filesContext.opRoot.toString();
    return operationName + "#" + Integer.toUnsignedString(System.identityHashCode(request));
  }

  public void copyToolInputsIntoWorkerToolRoot(WorkerKey key, WorkerInputs workerFiles)
      throws IOException {
    WorkerKey lock = keyLock(key);
    synchronized (lock) {
      long copiedFiles = 0;
      long copiedBytes = 0;
      try {
        // Copy tool inputs as needed
        Path workToolRoot = key.getToolRoot();
        for (Path opToolPath : workerFiles.opToolInputs) {
          Path workToolPath = workerFiles.relativizeInput(workToolRoot, opToolPath);
          if (!Files.exists(workToolPath)) {
            workerFiles.copyInputFile(opToolPath, workToolPath);
            copiedFiles++;
            copiedBytes += workerFiles.sizeFor(opToolPath);
          }
        }
      } finally {
        PersistentWorkerMetrics.recordInputs(
            PersistentWorkerMetrics.METHOD_TOOL_COPY, copiedFiles, copiedBytes);
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
    long copiedFiles = 0;
    long copiedBytes = 0;
    for (Path relPath : key.getWorkerFilesWithHashes().keySet()) {
      Path toolInputPath = toolInputRoot.resolve(relPath);
      Path execRootPath = workerExecRoot.resolve(relPath);

      FileAccessUtils.copyFile(toolInputPath, execRootPath);
      copiedFiles++;
      copiedBytes += Files.size(toolInputPath);
    }
    PersistentWorkerMetrics.recordInputs(
        PersistentWorkerMetrics.METHOD_TOOL_COPY, copiedFiles, copiedBytes);
  }

  @Override
  public WorkRequest preWorkInit(WorkerKey key, RequestCtx request, PersistentWorker worker)
      throws IOException {
    throw new IllegalStateException("preWorkInit requires a persistent-worker lease");
  }

  @VisibleForTesting
  WorkRequest preWorkInit(
      WorkerKey key,
      RequestCtx request,
      PersistentWorker worker,
      PersistentWorkerLifecycle.Lease lease)
      throws IOException {
    checkNotNull(request.timeout);
    RequestTimeoutHandler task = new RequestTimeoutHandler(request);
    PendingRequest pendingRequest = new PendingRequest(lease, task);
    PendingRequest alreadyPendingRequest = pendingReqs.putIfAbsent(request, pendingRequest);
    // null means that this request was not in pendingReqs (the expected case)
    if (alreadyPendingRequest != null) {
      if (alreadyPendingRequest.lease.worker() != worker) {
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

      // Symlinking should hypothetically be faster+leaner than copying inputs, but it's buggy.
      long inputSetupStarted = PersistentWorkerMetrics.startTimer();
      try {
        copyNontoolInputs(request.workerInputs, worker.getExecRoot());
      } finally {
        PersistentWorkerMetrics.observePhase(
            PersistentWorkerMetrics.PHASE_INPUT_SETUP, inputSetupStarted);
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
    // cleanup that needs to happen despite the failure. The cleanup above (pendingReqs removal,
    // future cancellation) is complete at this point, so we return null as expected.
    if (response == null) {
      return null;
    }

    try {
      Path workerExecRoot = worker.getExecRoot();
      // Always move outputs and clean up non-tool inputs, regardless of exit code. This matches the
      // REAPI spec as well as what Buildfarm and Bazel do elsewhere.
      long outputMoveStarted = PersistentWorkerMetrics.startTimer();
      request.setOutcome(PersistentWorkerMetrics.OUTCOME_OUTPUT_CLEANUP_FAILURE);
      try {
        moveOutputsToOperationRoot(request.filesContext, workerExecRoot);
      } finally {
        PersistentWorkerMetrics.observePhase(
            PersistentWorkerMetrics.PHASE_OUTPUT_MOVE, outputMoveStarted);
      }
      long inputCleanupStarted = PersistentWorkerMetrics.startTimer();
      request.setOutcome(PersistentWorkerMetrics.OUTCOME_INPUT_CLEANUP_FAILURE);
      try {
        cleanUpNontoolInputs(request.workerInputs, workerExecRoot);
      } finally {
        PersistentWorkerMetrics.observePhase(
            PersistentWorkerMetrics.PHASE_INPUT_CLEANUP, inputCleanupStarted);
      }
    } catch (IOException e) {
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
    long copiedFiles = 0;
    long copiedBytes = 0;
    for (Path opPath : workerInputs.allInputs.keySet()) {
      if (!workerInputs.allToolInputs.contains(opPath)) {
        Path execPath = workerInputs.relativizeInput(workerExecRoot, opPath);
        workerInputs.copyInputFile(opPath, execPath);
        copiedFiles++;
        copiedBytes += workerInputs.sizeFor(opPath);
      }
    }
    PersistentWorkerMetrics.recordInputs(
        PersistentWorkerMetrics.METHOD_COPY, copiedFiles, copiedBytes);
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
          onTimeout(this.request, pendingRequest.lease);
        }
      } catch (Throwable t) {
        log.log(
            Level.SEVERE,
            "Exception in persistent worker timeout handler for request: " + this.request.request,
            t);
      }
    }
  }

  @VisibleForTesting
  void onTimeout(RequestCtx request, PersistentWorkerLifecycle.Lease lease) {
    if (!lifecycle.beginRetiring(lease)) {
      PersistentWorkerMetrics.staleLifecycleCallback("action_timeout");
      return;
    }
    request.markTimedOut();
    PersistentWorker worker = lease.worker();
    log.severe("Persistent Worker timed out on request: " + request.request);
    try {
      PersistentWorkerMetrics.markDestroyReason(worker, PersistentWorkerMetrics.DESTROY_TIMEOUT);
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
