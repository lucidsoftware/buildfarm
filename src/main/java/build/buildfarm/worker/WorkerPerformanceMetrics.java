// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/** Comparable, bounded-cardinality measurements for worker performance experiments. */
public final class WorkerPerformanceMetrics {
  private static final Logger logger = Logger.getLogger(WorkerPerformanceMetrics.class.getName());
  private static final Gson gson = new Gson();
  private static final boolean logActions =
      Boolean.parseBoolean(System.getProperty("buildfarm.performance.logActions", "true"));
  private static final Counter actions =
      Counter.build()
          .name("worker_action_results_total")
          .help("Completed worker action results, excluding requeued/incomplete attempts.")
          .labelNames("outcome")
          .register();
  private static final Histogram phases =
      Histogram.build()
          .name("worker_action_phase_seconds")
          .help(
              "Completed action phase wall times by outcome; absent/invalid timestamps are"
                  + " omitted.")
          .labelNames("phase", "outcome")
          .buckets(
              .0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120,
              300, 600, 1200, 1800, 3600)
          .register();
  private static final Histogram prepare =
      Histogram.build()
          .name("create_exec_root_seconds")
          .help("Wall time in createExecDir, including staging and rollback on failure.")
          .labelNames("outcome")
          .buckets(
              .0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120,
              300, 600)
          .register();

  private WorkerPerformanceMetrics() {}

  public static void observePrepare(String outcome, long elapsedNanos) {
    prepare.labels(outcome).observe(elapsedNanos / 1_000_000_000.0);
  }

  /** Cleanup happens after completion reporting; emit a separately joinable event. */
  public static void recordCleanup(Path execRoot, String outcome, long elapsedNanos) {
    if (!logActions) {
      return;
    }
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("event", "worker_execroot_cleanup");
    record.put("exec_root", execRoot.toString());
    record.put("outcome", outcome);
    record.put("cleanup_seconds", elapsedNanos / 1_000_000_000.0);
    logger.info(gson.toJson(record));
  }

  static ExecutedActionMetadata metadata(ExecutionContext context) {
    if (context.operation.hasResponse()) {
      try {
        ExecuteResponse response = context.operation.getResponse().unpack(ExecuteResponse.class);
        if (response.hasResult() && response.getResult().hasExecutionMetadata()) {
          return response.getResult().getExecutionMetadata();
        }
      } catch (InvalidProtocolBufferException ignored) {
        // Fall back to the worker's partial metadata.
      }
    }
    if (context.executeResponse.hasResult()
        && context.executeResponse.getResult().hasExecutionMetadata()) {
      return context.executeResponse.getResult().getExecutionMetadata();
    }
    return context.metadata.getExecuteOperationMetadata().getPartialExecutionMetadata();
  }

  static void recordAction(ExecutionContext context, ExecutedActionMetadata metadata) {
    ExecuteResponse response = context.executeResponse.build();
    String outcome = "unknown";
    try {
      // Input-fetch failures may only populate the terminal Operation response.
      if (context.operation.hasResponse()) {
        response = context.operation.getResponse().unpack(ExecuteResponse.class);
      }
      outcome =
          context.operation.hasError()
              ? statusOutcome(context.operation.getError().getCode())
              : outcome(response);
    } catch (InvalidProtocolBufferException ignored) {
      // Diagnostic data must not prevent an operation from completing.
    }
    actions.labels(outcome).inc();
    Map<String, Double> seconds = phaseSeconds(metadata);
    for (Map.Entry<String, Double> phase : seconds.entrySet()) {
      phases.labels(phase.getKey(), outcome).observe(phase.getValue());
    }
    if (!logActions) {
      return;
    }
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("event", "worker_action_performance");
    record.put("operation", context.operation.getName());
    if (context.queueEntry != null) {
      record.put("action_digest", context.queueEntry.getExecuteEntry().getActionDigest().getHash());
      record.put(
          "invocation_id",
          context.queueEntry.getExecuteEntry().getRequestMetadata().getToolInvocationId());
    }
    if (context.execDir != null) {
      record.put("exec_root", context.execDir.toString());
    }
    record.put("worker", metadata.getWorker());
    record.put("outcome", outcome);
    record.put(
        "status_code",
        context.operation.hasError()
            ? context.operation.getError().getCode()
            : response.getStatus().getCode());
    if (response.hasResult()) {
      record.put("exit_code", response.getResult().getExitCode());
    }
    record.put("phase_seconds", seconds);
    record.put("fetched_bytes", context.workerExecutedMetadata.getFetchedBytes());
    record.put("usage", context.workerExecutedMetadata.getUsageMap());
    logger.info(gson.toJson(record));
  }

  static String outcome(ExecuteResponse response) {
    if (response.getStatus().getCode() != Code.OK_VALUE) {
      return statusOutcome(response.getStatus().getCode());
    }
    if (!response.hasResult()) {
      return "unknown";
    }
    return response.getResult().getExitCode() == 0 ? "success" : "action_failure";
  }

  private static String statusOutcome(int code) {
    if (code == Code.CANCELLED_VALUE) {
      return "cancelled";
    }
    if (code == Code.DEADLINE_EXCEEDED_VALUE) {
      return "timeout";
    }
    return "infrastructure_error";
  }

  static Map<String, Double> phaseSeconds(ExecutedActionMetadata metadata) {
    Map<String, Double> seconds = new LinkedHashMap<>();
    addPhase(
        seconds,
        "queued_to_match",
        metadata.getQueuedTimestamp(),
        metadata.getWorkerStartTimestamp());
    addPhase(
        seconds,
        "match_to_input_fetch",
        metadata.getWorkerStartTimestamp(),
        metadata.getInputFetchStartTimestamp());
    addPhase(
        seconds,
        "input_fetch",
        metadata.getInputFetchStartTimestamp(),
        metadata.getInputFetchCompletedTimestamp());
    addPhase(
        seconds,
        "input_fetch_to_execution",
        metadata.getInputFetchCompletedTimestamp(),
        metadata.getExecutionStartTimestamp());
    addPhase(
        seconds,
        "execution",
        metadata.getExecutionStartTimestamp(),
        metadata.getExecutionCompletedTimestamp());
    addPhase(
        seconds,
        "execution_to_output_upload",
        metadata.getExecutionCompletedTimestamp(),
        metadata.getOutputUploadStartTimestamp());
    addPhase(
        seconds,
        "output_upload",
        metadata.getOutputUploadStartTimestamp(),
        metadata.getOutputUploadCompletedTimestamp());
    addPhase(
        seconds,
        "worker_to_output_complete",
        metadata.getWorkerStartTimestamp(),
        metadata.getOutputUploadCompletedTimestamp());
    return seconds;
  }

  private static void addPhase(
      Map<String, Double> seconds, String phase, Timestamp start, Timestamp end) {
    if (!Timestamps.isValid(start)
        || !Timestamps.isValid(end)
        || start.equals(Timestamp.getDefaultInstance())
        || end.equals(Timestamp.getDefaultInstance())
        || Timestamps.compare(start, end) > 0) {
      return;
    }
    seconds.put(phase, Durations.toNanos(Timestamps.between(start, end)) / 1_000_000_000.0);
  }
}
