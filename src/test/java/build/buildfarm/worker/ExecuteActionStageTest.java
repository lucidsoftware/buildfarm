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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import io.prometheus.client.CollectorRegistry;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecuteActionStageTest {
  @Test
  public void executionTimeSeparatesMnemonicsAndAccumulatesObservations() {
    ExecuteActionStage stage = new ExecuteActionStage(mock(WorkerContext.class), null, null);
    stage.releaseExecutor("compile-1", "metrics-test-compile", true, 2_000_000, 100_000, 0);
    stage.releaseExecutor("compile-2", "metrics-test-compile", false, 3_000_000, 200_000, 1);
    stage.releaseExecutor("link", "metrics-test-link", false, 7_000_000, 0, 0);

    assertThat(sample("execution_time_ms_sum", "metrics-test-compile", true)).isEqualTo(2000.0);
    assertThat(sample("execution_time_ms_count", "metrics-test-compile", true)).isEqualTo(1.0);
    assertThat(sample("execution_time_ms_sum", "metrics-test-compile", false)).isEqualTo(3000.0);
    assertThat(sample("execution_time_ms_count", "metrics-test-compile", false)).isEqualTo(1.0);
    assertThat(sample("execution_time_ms_sum", "metrics-test-link", false)).isEqualTo(7000.0);
    assertThat(sample("execution_time_ms_count", "metrics-test-link", false)).isEqualTo(1.0);
  }

  @Test
  public void missingMnemonicUsesUnknown() {
    ExecuteActionStage stage = new ExecuteActionStage(mock(WorkerContext.class), null, null);
    Double before = sample("execution_time_ms_sum", "unknown", false);
    stage.releaseExecutor("missing", "", false, 1_000_000, 0, 0);
    assertThat(sample("execution_time_ms_sum", "unknown", false))
        .isEqualTo((before == null ? 0 : before) + 1000.0);
  }

  private static Double sample(String name, String mnemonic, boolean persistentWorker) {
    return CollectorRegistry.defaultRegistry.getSampleValue(
        name,
        new String[] {"mnemonic", "persistent_worker"},
        new String[] {mnemonic, Boolean.toString(persistentWorker)});
  }

  @Test
  public void errorPathDestroysExecDir() throws Exception {
    WorkerContext context = mock(WorkerContext.class);
    when(context.getExecuteStageWidth()).thenReturn(1);
    PipelineStage error = mock(PipelineStage.class);

    QueueEntry errorEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("error"))
            .build();
    ExecutionContext errorContext =
        ExecutionContext.newBuilder()
            .setQueueEntry(errorEntry)
            .setExecDir(Path.of("error-operation-path"))
            .build();

    PipelineStage executeActionStage = new ExecuteActionStage(context, /* output= */ null, error);
    executeActionStage.error().put(errorContext);
    verify(context, times(1)).destroyExecDir(errorContext.execDir);
    verify(error, times(1)).put(errorContext);
  }
}
