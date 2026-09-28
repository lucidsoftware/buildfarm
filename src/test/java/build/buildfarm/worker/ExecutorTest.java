// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.buildfarm.common.Claim;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.worker.persistent.WorkFilesContext;
import build.buildfarm.worker.resources.ResourceLimits;
import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import persistent.bazel.client.WorkerResources;
import persistent.common.PoolExhaustedException;

public class ExecutorTest {
  @Test
  public void fallbackRebuildsNativeWrappersAndClosesBothResourceHandles() throws Exception {
    WorkerContext workerContext = mock(WorkerContext.class);
    WorkerContext.IOResource pwResource = mock(WorkerContext.IOResource.class);
    WorkerContext.IOResource nativeResource = mock(WorkerContext.IOResource.class);
    Claim claim = mock(Claim.class);
    when(claim.getPools()).thenReturn(List.of());
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(Operation.newBuilder().setName("fallback").build())
            .setExecDir(Path.of("/tmp/pw-fallback-test"))
            .setCommand(
                Command.newBuilder()
                    .addArguments("compiler")
                    .addArguments("@request.params")
                    .build())
            .setQueueEntry(QueueEntry.getDefaultInstance())
            .setClaim(claim)
            .build();
    doAnswer(
            invocation -> {
              boolean persistent = invocation.getArgument(5);
              ImmutableList.Builder<String> args = invocation.getArgument(2);
              args.add(persistent ? "pw-wrapper" : "native-cgroup-wrapper");
              if (!persistent) {
                verify(pwResource).close();
              }
              return persistent ? pwResource : nativeResource;
            })
        .when(workerContext)
        .limitExecution(
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean());
    Executor executor =
        new Executor(workerContext, context, null, 100, 20, 20, 100, 1000, Runnable::run) {
          @Override
          Code executeCommand(
              String name,
              Path dir,
              List<String> arguments,
              List<Command.EnvironmentVariable> environment,
              ResourceLimits limits,
              WorkerContext.IOResource resource,
              WorkFilesContext files,
              Duration timeout,
              ActionResult.Builder result)
              throws IOException {
            if (files != null) {
              assertThat(arguments)
                  .containsExactly("pw-wrapper", "compiler", "@request.params")
                  .inOrder();
              throw new PoolExhaustedException(null);
            }
            assertThat(arguments)
                .containsExactly("native-cgroup-wrapper", "compiler", "@request.params")
                .inOrder();
            assertThat(resource).isSameInstanceAs(nativeResource);
            return Code.OK;
          }
        };
    var files = mock(WorkFilesContext.class);
    assertThat(
            Executor.executeWithPoolFallback(
                Duration.newBuilder().setSeconds(5).build(),
                (persistent, remaining) ->
                    executor.executeAttempt(
                        new ResourceLimits(),
                        ImmutableList.of(),
                        remaining,
                        persistent ? files : null)))
        .isEqualTo(Code.OK);
    verify(pwResource).close();
    verify(nativeResource).close();
  }

  @Test
  public void poolExhaustionRetriesNativeWithRemainingBudget() throws Exception {
    List<Boolean> attempts = new ArrayList<>();
    Duration budget = Duration.newBuilder().setSeconds(5).build();
    var result =
        Executor.executeWithPoolFallback(
            budget,
            (persistent, remaining) -> {
              attempts.add(persistent);
              if (persistent) {
                throw new PoolExhaustedException(null);
              }
              assertThat(Durations.toNanos(remaining)).isLessThan(Durations.toNanos(budget));
              assertThat(Durations.toNanos(remaining)).isGreaterThan(0);
              return Code.OK;
            });
    assertThat(result).isEqualTo(Code.OK);
    assertThat(attempts).containsExactly(true, false).inOrder();
  }

  @Test
  public void expiredBudgetDoesNotStartNativeFallback() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    assertThat(
            Executor.executeWithPoolFallback(
                Duration.getDefaultInstance(),
                (persistent, timeout) -> {
                  attempts.incrementAndGet();
                  throw new PoolExhaustedException(null);
                }))
        .isEqualTo(Code.DEADLINE_EXCEEDED);
    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  public void workerFailureDoesNotRetryNative() {
    AtomicInteger attempts = new AtomicInteger();
    assertThrows(
        IOException.class,
        () ->
            Executor.executeWithPoolFallback(
                Duration.newBuilder().setSeconds(5).build(),
                (persistent, timeout) -> {
                  attempts.incrementAndGet();
                  throw new IOException("worker launch failed");
                }));
    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  public void interruptedWaitDoesNotRetryNative() {
    AtomicInteger attempts = new AtomicInteger();
    try {
      assertThrows(
          InterruptedException.class,
          () ->
              Executor.executeWithPoolFallback(
                  Duration.newBuilder().setSeconds(5).build(),
                  (persistent, timeout) -> {
                    attempts.incrementAndGet();
                    Thread.currentThread().interrupt();
                    throw new PoolExhaustedException(null);
                  }));
      assertThat(attempts.get()).isEqualTo(1);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void persistentWorkerAllowsMarkedExecutableForAllowlistedMnemonic() {
    assertThat(
            Executor.isPersistentWorkerEligible(
                "ScalaCompile", ImmutableList.of("ScalaCompile"), true))
        .isTrue();
  }

  @Test
  public void persistentWorkerAllowsMarkedExecutableForWildcardAllowlist() {
    assertThat(Executor.isPersistentWorkerEligible("ScalaCompile", ImmutableList.of("*"), true))
        .isTrue();
  }

  @Test
  public void persistentWorkerRejectsUnmarkedExecutable() {
    assertThat(
            Executor.isPersistentWorkerEligible(
                "ScalaCompile", ImmutableList.of("ScalaCompile"), false))
        .isFalse();
  }

  @Test
  public void persistentWorkerRejectsMnemonicOutsideAllowlist() {
    assertThat(
            Executor.isPersistentWorkerEligible(
                "TsProject", ImmutableList.of("ScalaCompile"), true))
        .isFalse();
  }

  @Test
  public void persistentRequestReportsOnlyItsOwnCpuAndAppliesLeaseBeforeResume() throws Exception {
    Claim claim = mock(Claim.class);
    CPULease lease = mock(CPULease.class);
    when(claim.get(CPULease.RESOURCE_NAME)).thenReturn(lease);
    when(lease.amount()).thenReturn(2000);
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(Operation.newBuilder().setName("request").build())
            .setClaim(claim)
            .build();
    Executor executor =
        new Executor(
            mock(WorkerContext.class), context, null, 100, 20, 20, 100, 1000, Runnable::run);
    AtomicLong cumulative = new AtomicLong(1000000);
    AtomicLong quota = new AtomicLong();
    WorkerResources resources =
        new WorkerResources() {
          public void setCpu(int micros) {
            quota.set(micros);
          }

          public void resume() {
            assertThat(quota.get()).isEqualTo(200000);
          }

          public Map<String, Long> sample() {
            return Map.of(
                "cpu.usage_usec",
                cumulative.get(),
                "cpu.nr_periods",
                cumulative.get() / 100,
                "cpu.throttled_usec",
                0L);
          }
        };
    for (int i = 0; i < 2; i++) {
      executor.executePersistentRequest(
          resources,
          () -> {
            cumulative.addAndGet(500);
            return WorkResponse.newBuilder().setOutput("done").build();
          },
          Duration.newBuilder().setSeconds(5).build());
      assertThat(context.workerExecutedMetadata.getUsageOrThrow("cpu.usage_usec")).isEqualTo(500);
    }
  }

  @Test
  public void persistentRequestTimeoutCancelsItsResponseTask() throws Exception {
    Claim claim = mock(Claim.class);
    CPULease lease = mock(CPULease.class);
    when(claim.get(CPULease.RESOURCE_NAME)).thenReturn(lease);
    when(lease.amount()).thenReturn(1000);
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(Operation.newBuilder().setName("timeout").build())
            .setClaim(claim)
            .build();
    Executor executor =
        new Executor(
            mock(WorkerContext.class), context, null, 100, 20, 20, 100, 1000, Runnable::run);
    CountDownLatch exited = new CountDownLatch(1);
    assertThrows(
        TimeoutException.class,
        () ->
            executor.executePersistentRequest(
                WorkerResources.NONE,
                () -> {
                  try {
                    new CountDownLatch(1).await();
                    return WorkResponse.getDefaultInstance();
                  } finally {
                    exited.countDown();
                  }
                },
                Duration.newBuilder().setNanos(100000000).build()));
    assertThat(exited.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void persistentRequestReturnsUnusedCpuThroughTheSharedMarketLoop() throws Exception {
    Claim claim = mock(Claim.class);
    CPULease lease = mock(CPULease.class);
    Market market = mock(Market.class);
    WorkerContext workerContext = mock(WorkerContext.class);
    when(workerContext.market()).thenReturn(market);
    when(claim.get(CPULease.RESOURCE_NAME)).thenReturn(lease);
    AtomicInteger balance = new AtomicInteger(2000);
    when(lease.amount()).thenAnswer(invocation -> balance.get());
    doAnswer(
            invocation -> {
              balance.addAndGet(-(int) invocation.getArgument(0));
              return null;
            })
        .when(lease)
        .deplete(1000);
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setMarketExecution(true)
            .setOperation(Operation.newBuilder().setName("market").build())
            .setClaim(claim)
            .build();
    Executor executor =
        new Executor(workerContext, context, null, 100, 20, 20, 100, 1000, Runnable::run);
    CountDownLatch reduced = new CountDownLatch(1);
    AtomicLong samples = new AtomicLong();
    WorkerResources resources =
        new WorkerResources() {
          public void setCpu(int micros) {
            if (micros == 100000) {
              reduced.countDown();
            }
          }

          public Map<String, Long> sample() {
            return Map.of(
                "cpu.usage_usec",
                0L,
                "cpu.nr_periods",
                samples.incrementAndGet(),
                "cpu.throttled_usec",
                0L);
          }
        };
    executor.executePersistentRequest(
        resources,
        () -> {
          assertThat(reduced.await(2, TimeUnit.SECONDS)).isTrue();
          return WorkResponse.getDefaultInstance();
        },
        Duration.newBuilder().setSeconds(5).build());
    verify(market).sell(1000);
    assertThat(balance.get()).isEqualTo(1000);
  }
}
