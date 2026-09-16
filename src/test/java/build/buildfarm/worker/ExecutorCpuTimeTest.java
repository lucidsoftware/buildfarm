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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Claim;
import build.buildfarm.common.ProcessUtils;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.worker.WorkerContext.IOResource;
import com.google.protobuf.Duration;
import com.google.rpc.Code;
import io.prometheus.client.CollectorRegistry;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

@RunWith(JUnit4.class)
public class ExecutorCpuTimeTest {
  @Rule public final TestName testName = new TestName();

  private Executor executor;
  private IOResource resource;
  private Process process;

  @Before
  public void setUp() throws Exception {
    WorkerContext worker = mock(WorkerContext.class);
    when(worker.getStandardOutputLimit()).thenReturn(1024L);
    when(worker.getStandardErrorLimit()).thenReturn(1024L);
    Claim claim = mock(Claim.class);
    CPULease lease = mock(CPULease.class);
    when(lease.amount()).thenReturn(1000);
    when(claim.get(CPULease.RESOURCE_NAME)).thenReturn(lease);
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setClaim(claim)
            .setMetadata(
                QueuedOperationMetadata.newBuilder()
                    .setRequestMetadata(
                        RequestMetadata.newBuilder().setActionMnemonic(testName.getMethodName())))
            .build();
    executor = new Executor(worker, context, null, 0, 0, 0, 0, 0, Runnable::run);
    resource = mock(IOResource.class);
    when(resource.sample())
        .thenReturn(Map.of("cpu.usage_usec", 500_000L), Map.of("cpu.usage_usec", 2_750_000L));
    process = mock(Process.class);
    when(process.getInputStream()).thenReturn(InputStream.nullInputStream());
    when(process.getErrorStream()).thenReturn(InputStream.nullInputStream());
    when(process.getOutputStream()).thenReturn(OutputStream.nullOutputStream());
    when(process.waitFor()).thenReturn(0);
    when(process.waitFor(anyLong(), any(TimeUnit.class))).thenReturn(true);
  }

  private Code execute(Duration timeout) throws Exception {
    try (MockedStatic<ProcessUtils> processes = mockStatic(ProcessUtils.class)) {
      processes
          .when(() -> ProcessUtils.threadSafeStart(any(ProcessBuilder.class)))
          .thenReturn(process);
      return executor.executeNativeProcess(
          "test", new ProcessBuilder("unused"), resource, timeout, ActionResult.newBuilder());
    }
  }

  private Double cpuSeconds() {
    return CollectorRegistry.defaultRegistry.getSampleValue(
        "action_cpu_seconds_total",
        new String[] {"mnemonic"},
        new String[] {testName.getMethodName()});
  }

  @Test
  public void successfulActionRecordsDeltaInSeconds() throws Exception {
    assertThat(execute(null)).isEqualTo(Code.OK);
    assertThat(cpuSeconds()).isEqualTo(2.25);
  }

  @Test
  public void failedActionStillRecordsCpu() throws Exception {
    when(process.waitFor()).thenReturn(1);
    assertThat(execute(null)).isEqualTo(Code.OK);
    assertThat(cpuSeconds()).isEqualTo(2.25);
  }

  @Test
  public void timedActionRecordsCpuOnce() throws Exception {
    assertThat(execute(Duration.newBuilder().setSeconds(30).build())).isEqualTo(Code.OK);
    assertThat(cpuSeconds()).isEqualTo(2.25);
  }

  @Test
  public void timeoutRecordsCpuOnce() throws Exception {
    when(process.waitFor(anyLong(), any(TimeUnit.class))).thenReturn(false);
    assertThat(execute(Duration.getDefaultInstance())).isEqualTo(Code.DEADLINE_EXCEEDED);
    assertThat(cpuSeconds()).isEqualTo(2.25);
  }

  @Test
  public void interruptedActionRecordsCpuAfterTermination() throws Exception {
    when(process.waitFor()).thenThrow(new InterruptedException("canceled"));
    assertThrows(InterruptedException.class, () -> execute(null));
    verify(process).destroy();
    assertThat(cpuSeconds()).isEqualTo(2.25);
  }

  @Test
  public void missingAccountingDoesNotCreateZeroSeries() throws Exception {
    when(resource.sample()).thenReturn(Map.of());
    execute(null);
    assertThat(cpuSeconds()).isNull();
  }

  @Test
  public void missingFinalSampleDoesNotCreateZeroSeries() throws Exception {
    when(resource.sample()).thenReturn(Map.of("cpu.usage_usec", 500_000L), Map.of());
    execute(null);
    assertThat(cpuSeconds()).isNull();
  }

  @Test
  public void missingStartSampleDoesNotCreateZeroSeries() throws Exception {
    when(resource.sample()).thenReturn(Map.of(), Map.of("cpu.usage_usec", 2_750_000L));
    execute(null);
    assertThat(cpuSeconds()).isNull();
  }

  @Test
  public void unknownMnemonicAccumulatesCpuAcrossActions() {
    String[] labels = {"mnemonic"};
    String[] values = {"unknown"};
    Double before =
        CollectorRegistry.defaultRegistry.getSampleValue(
            "action_cpu_seconds_total", labels, values);
    new ActionCpuTime("", Map.of("cpu.usage_usec", 10L))
        .record(Map.of("cpu.usage_usec", 1_000_010L));
    new ActionCpuTime("", Map.of("cpu.usage_usec", 20L))
        .record(Map.of("cpu.usage_usec", 2_000_020L));
    assertThat(
            CollectorRegistry.defaultRegistry.getSampleValue(
                "action_cpu_seconds_total", labels, values))
        .isEqualTo((before == null ? 0 : before) + 3.0);
  }

  @Test
  public void resetAccountingDoesNotCreateSeries() throws Exception {
    when(resource.sample())
        .thenReturn(Map.of("cpu.usage_usec", 500_000L), Map.of("cpu.usage_usec", 1L));
    execute(null);
    assertThat(cpuSeconds()).isNull();
  }
}
