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

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.prometheus.client.CollectorRegistry;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkerPerformanceMetricsTest {
  @Test
  public void phasesExcludeDownstreamWaitAndIncludeZeroDuration() {
    ExecutedActionMetadata metadata =
        ExecutedActionMetadata.newBuilder()
            .setWorkerStartTimestamp(Timestamps.fromSeconds(100))
            .setInputFetchStartTimestamp(Timestamps.fromSeconds(101))
            .setInputFetchCompletedTimestamp(Timestamps.fromSeconds(103))
            .setExecutionStartTimestamp(Timestamps.fromSeconds(108))
            .setExecutionCompletedTimestamp(Timestamps.fromSeconds(108))
            .setOutputUploadStartTimestamp(Timestamps.fromSeconds(109))
            .setOutputUploadCompletedTimestamp(Timestamps.fromSeconds(111))
            .build();
    assertThat(WorkerPerformanceMetrics.phaseSeconds(metadata))
        .containsExactly(
            "match_to_input_fetch",
            1.0,
            "input_fetch",
            2.0,
            "input_fetch_to_execution",
            5.0,
            "execution",
            0.0,
            "execution_to_output_upload",
            1.0,
            "output_upload",
            2.0,
            "worker_to_output_complete",
            11.0);
  }

  @Test
  public void missingReversedAndInvalidTimestampsAreNotObserved() {
    assertThat(WorkerPerformanceMetrics.phaseSeconds(ExecutedActionMetadata.getDefaultInstance()))
        .isEmpty();
    ExecutedActionMetadata metadata =
        ExecutedActionMetadata.newBuilder()
            .setInputFetchStartTimestamp(Timestamps.fromSeconds(103))
            .setInputFetchCompletedTimestamp(Timestamps.fromSeconds(101))
            .setExecutionStartTimestamp(Timestamp.newBuilder().setNanos(-1))
            .setExecutionCompletedTimestamp(Timestamps.fromSeconds(105))
            .build();
    assertThat(WorkerPerformanceMetrics.phaseSeconds(metadata)).isEmpty();
  }

  @Test
  public void outcomeDoesNotTreatMissingResultAsSuccess() {
    assertThat(WorkerPerformanceMetrics.outcome(ExecuteResponse.getDefaultInstance()))
        .isEqualTo("unknown");
    assertThat(
            WorkerPerformanceMetrics.outcome(
                ExecuteResponse.newBuilder()
                    .setResult(ActionResult.newBuilder().setExitCode(1))
                    .build()))
        .isEqualTo("action_failure");
    assertThat(
            WorkerPerformanceMetrics.outcome(
                ExecuteResponse.newBuilder().setResult(ActionResult.getDefaultInstance()).build()))
        .isEqualTo("success");
    assertThat(
            WorkerPerformanceMetrics.outcome(
                ExecuteResponse.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED_VALUE))
                    .build()))
        .isEqualTo("timeout");
  }

  @Test
  public void terminalResponseOverridesPartialContextAndLogsJoinableCleanup() throws Exception {
    ExecutedActionMetadata metadata =
        ExecutedActionMetadata.newBuilder()
            .setInputFetchStartTimestamp(Timestamps.fromSeconds(100))
            .setInputFetchCompletedTimestamp(Timestamps.fromSeconds(102))
            .build();
    ExecuteResponse failed =
        ExecuteResponse.newBuilder()
            .setStatus(Status.newBuilder().setCode(Code.INTERNAL_VALUE))
            .setResult(ActionResult.newBuilder().setExecutionMetadata(metadata))
            .build();
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(
                Operation.newBuilder()
                    .setName("operations/test")
                    .setDone(true)
                    .setResponse(Any.pack(failed))
                    .build())
            .setExecDir(Path.of("/execroots/test"))
            .build();
    context.executeResponse.setResult(ActionResult.getDefaultInstance());
    assertThat(WorkerPerformanceMetrics.metadata(context)).isEqualTo(metadata);
    List<String> messages = new ArrayList<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            messages.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    Logger logger = Logger.getLogger(WorkerPerformanceMetrics.class.getName());
    logger.addHandler(handler);
    double before = sample("worker_action_results_total", "outcome", "infrastructure_error");
    try {
      new PutOperationStage(operation -> {}).put(context);
      WorkerPerformanceMetrics.recordCleanup(context.execDir, "success", 250_000_000);
    } finally {
      logger.removeHandler(handler);
    }
    assertThat(sample("worker_action_results_total", "outcome", "infrastructure_error") - before)
        .isEqualTo(1.0);
    assertThat(messages).hasSize(2);
    assertThat(messages.get(0)).contains("\"event\":\"worker_action_performance\"");
    assertThat(messages.get(0)).contains("\"outcome\":\"infrastructure_error\"");
    assertThat(messages.get(0)).contains("\"input_fetch\":2.0");
    assertThat(messages.get(1)).contains("\"cleanup_seconds\":0.25");
    for (String message : messages) {
      assertThat(message).contains("\"exec_root\":\"/execroots/test\"");
    }
  }

  @Test
  public void requeuedOperationsDoNotBecomeSuccessfulActions() throws Exception {
    double before = sample("worker_action_results_total", "outcome", "success");
    new PutOperationStage(operation -> {})
        .put(
            ExecutionContext.newBuilder()
                .setOperation(Operation.newBuilder().setName("requeued").build())
                .build());
    assertThat(sample("worker_action_results_total", "outcome", "success")).isEqualTo(before);
  }

  @Test
  public void prepareHistogramRetainsLongDurations() {
    double before = sample("create_exec_root_seconds_bucket", "outcome", "success", "le", "30.0");
    WorkerPerformanceMetrics.observePrepare("success", 20_000_000_000L);
    assertThat(
            sample("create_exec_root_seconds_bucket", "outcome", "success", "le", "30.0") - before)
        .isEqualTo(1.0);
  }

  private static double sample(String name, String... labels) {
    String[] names = new String[labels.length / 2];
    String[] values = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      names[i] = labels[2 * i];
      values[i] = labels[2 * i + 1];
    }
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(name, names, values);
    return value == null ? 0 : value;
  }
}
