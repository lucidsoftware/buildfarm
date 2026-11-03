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

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import persistent.bazel.client.WorkerResources;
import persistent.common.CtxAround;

public class RequestCtx implements CtxAround<WorkRequest> {
  /** Runs a request while its exclusive lifecycle lease is held. */
  @FunctionalInterface
  public interface Execution {
    WorkResponse run(WorkerResources resources, Callable<WorkResponse> work) throws Exception;
  }

  public Execution execution = (resources, work) -> work.call();

  public final WorkRequest request;

  public final WorkFilesContext filesContext;

  public final WorkerInputs workerInputs;

  public final Duration timeout;

  final long metricsStartedNanos;

  private final AtomicBoolean timedOut = new AtomicBoolean(false);

  private volatile String outcome = PersistentWorkerMetrics.OUTCOME_WORKER_ERROR;

  public RequestCtx(
      WorkRequest request, WorkFilesContext ctx, WorkerInputs workFiles, Duration timeout) {
    this(request, ctx, workFiles, timeout, PersistentWorkerMetrics.startTimer());
  }

  public RequestCtx(
      WorkRequest request,
      WorkFilesContext ctx,
      WorkerInputs workFiles,
      Duration timeout,
      long metricsStartedNanos) {
    this.request = request;
    this.filesContext = ctx;
    this.workerInputs = workFiles;
    this.timeout = timeout;
    this.metricsStartedNanos = metricsStartedNanos;
  }

  @Override
  public WorkRequest get() {
    return request;
  }

  void markTimedOut() {
    timedOut.set(true);
    outcome = PersistentWorkerMetrics.OUTCOME_WORKER_TIMEOUT;
  }

  boolean timedOut() {
    return timedOut.get();
  }

  void setOutcome(String outcome) {
    this.outcome = outcome;
  }

  String outcome() {
    return outcome;
  }
}
