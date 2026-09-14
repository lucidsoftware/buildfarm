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

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.persistent.PersistentExecutor;
import build.buildfarm.worker.persistent.WorkFilesContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.gson.JsonParser;
import com.google.longrunning.Operation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.junit.Test;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerResources;

public class PersistentWorkerObservationTest {
  private WorkerKey key(ImmutableMap<String, String> env, String tools) {
    return new WorkerKey(
        ImmutableList.of("compiler-private-argument"),
        ImmutableList.of("--persistent_worker"),
        env,
        Path.of("/work/pws"),
        "ScalaCompile",
        HashCode.fromString(tools),
        new TreeMap<>(),
        true,
        false);
  }

  @Test
  public void observationAlwaysUsesNativeExecutionEvenWhenPreparationFailsOrSamplingIsOff()
      throws Exception {
    var settings =
        build.buildfarm.common.config.BuildfarmConfigs.getInstance()
            .getWorker()
            .getPersistentWorkers();
    double oldRate = settings.getObservationSampleRate();
    try {
      for (int scenario = 0; scenario < 3; scenario++) {
        settings.setObservationSampleRate(scenario == 2 ? 0 : 1);
        WorkerContext workerContext = org.mockito.Mockito.mock(WorkerContext.class);
        WorkerContext.IOResource resource =
            org.mockito.Mockito.mock(WorkerContext.IOResource.class);
        var claim = org.mockito.Mockito.mock(build.buildfarm.common.Claim.class);
        org.mockito.Mockito.when(claim.getPools()).thenReturn(List.of());
        Command command =
            Command.newBuilder()
                .addArguments("/bin/compiler")
                .addArguments("@request.params")
                .build();
        Path root = Path.of("/tmp/observation-operation");
        var context =
            ExecutionContext.newBuilder()
                .setOperation(Operation.newBuilder().setName("observe").build())
                .setCommand(command)
                .setExecDir(root)
                .setMetadata(QueuedOperationMetadata.newBuilder())
                .setQueueEntry(build.buildfarm.v1test.QueueEntry.getDefaultInstance())
                .setClaim(claim)
                .build();
        List<Boolean> modes = new ArrayList<>();
        org.mockito.Mockito.doAnswer(
                invocation -> {
                  boolean persistent = invocation.getArgument(5);
                  modes.add(persistent);
                  return resource;
                })
            .when(workerContext)
            .limitExecution(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.isNull(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyBoolean());
        WorkerResources.Profile profile =
            new WorkerResources.Profile() {
              public String identity() {
                return "test";
              }

              public WorkerResources create() {
                throw new AssertionError("observation allocated a cgroup");
              }
            };
        if (scenario == 1) {
          org.mockito.Mockito.when(workerContext.persistentWorkerResources(command))
              .thenThrow(new IllegalArgumentException("private error detail"));
        } else {
          org.mockito.Mockito.when(workerContext.persistentWorkerResources(command))
              .thenReturn(profile);
        }
        java.util.concurrent.atomic.AtomicInteger executions =
            new java.util.concurrent.atomic.AtomicInteger();
        Executor executor =
            new Executor(workerContext, context, null, 100, 20, 20, 100, 1000, Runnable::run) {
              @Override
              com.google.rpc.Code executeCommand(
                  String name,
                  Path dir,
                  List<String> args,
                  List<Command.EnvironmentVariable> env,
                  build.buildfarm.worker.resources.ResourceLimits limits,
                  WorkerContext.IOResource handle,
                  WorkFilesContext files,
                  com.google.protobuf.Duration timeout,
                  build.bazel.remote.execution.v2.ActionResult.Builder result) {
                assertThat(files).isNull();
                assertThat(args).containsExactly("/bin/compiler", "@request.params").inOrder();
                executions.incrementAndGet();
                return com.google.rpc.Code.OK;
              }
            };
        var files = WorkFilesContext.fromContext(root, Tree.getDefaultInstance(), command);
        assertThat(
                executor.executeObserved(
                    new build.buildfarm.worker.resources.ResourceLimits(),
                    ImmutableList.of(),
                    com.google.protobuf.Duration.newBuilder().setSeconds(10).build(),
                    files))
            .isEqualTo(com.google.rpc.Code.OK);
        assertThat(executions.get()).isEqualTo(1);
        if (scenario == 2) {
          assertThat(modes).containsExactly(false);
        } else {
          assertThat(modes).containsExactly(true, false).inOrder();
        }
      }
    } finally {
      settings.setObservationSampleRate(oldRate);
    }
  }

  @Test
  public void fingerprintsRespectKeyEqualityAndResourceProfiles() {
    var first = key(ImmutableMap.of("A", "secret", "B", "other"), "1234");
    var reordered = key(ImmutableMap.of("B", "other", "A", "secret"), "1234");
    assertThat(first).isEqualTo(reordered);
    assertThat(PersistentWorkerObservation.fingerprint(first))
        .isEqualTo(PersistentWorkerObservation.fingerprint(reordered));
    assertThat(PersistentWorkerObservation.fingerprint(first))
        .isNotEqualTo(PersistentWorkerObservation.fingerprint(key(first.getEnv(), "5678")));
    WorkerResources.Profile profile =
        new WorkerResources.Profile() {
          public String identity() {
            return "limited";
          }

          public WorkerResources create() {
            throw new AssertionError("must not allocate cgroups");
          }
        };
    assertThat(PersistentWorkerObservation.fingerprint(first.withResourceProfile(profile)))
        .isNotEqualTo(PersistentWorkerObservation.fingerprint(first));
  }

  @Test
  public void tracesJoinStartAndFinishWithoutExposingArgumentsOrEnvironment() {
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(Operation.newBuilder().setName("operation-1").build())
            .setCommand(Command.getDefaultInstance())
            .setMetadata(QueuedOperationMetadata.newBuilder())
            .build();
    List<String> lines = new ArrayList<>();
    var observation = new PersistentWorkerObservation(context, "worker-1", 1, lines::add);
    observation.key(key(ImmutableMap.of("PRIVATE_ENV", "private-value"), "1234"));
    observation.start(1_000_000);
    observation.finish("OK", 0);
    assertThat(lines).hasSize(2);
    var start = JsonParser.parseString(lines.get(0)).getAsJsonObject();
    var finish = JsonParser.parseString(lines.get(1)).getAsJsonObject();
    assertThat(start.get("event").getAsString()).isEqualTo("start");
    assertThat(finish.get("event").getAsString()).isEqualTo("finish");
    assertThat(start.get("key")).isEqualTo(finish.get("key"));
    assertThat(start.get("attempt")).isEqualTo(finish.get("attempt"));
    assertThat(start.get("worker_session")).isEqualTo(finish.get("worker_session"));
    assertThat(finish.get("native_attempt_ms").getAsDouble()).isAtLeast(0.0);
    assertThat(lines.toString()).doesNotContain("private-value");
    assertThat(lines.toString()).doesNotContain("PRIVATE_ENV");
    assertThat(lines.toString()).doesNotContain("compiler-private-argument");
  }

  @Test
  public void loggingFailureCannotFailExecution() {
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(Operation.getDefaultInstance())
            .setCommand(Command.getDefaultInstance())
            .setMetadata(QueuedOperationMetadata.newBuilder())
            .build();
    var observation =
        new PersistentWorkerObservation(
            context,
            "worker",
            1,
            line -> {
              throw new IllegalStateException("log unavailable");
            });
    observation.keyError(new IllegalArgumentException("private argument"));
    observation.start(0);
    observation.finish("ERROR", -1);
    assertThat(PersistentWorkerObservation.shouldSample("op", 0)).isFalse();
    assertThat(PersistentWorkerObservation.shouldSample("op", 1)).isTrue();
    assertThat(PersistentWorkerObservation.shouldSample("op", 0.5))
        .isEqualTo(PersistentWorkerObservation.shouldSample("op", 0.5));
  }

  @Test
  public void preparationIgnoresRequestInputsAndDoesNotCreateDirectoriesOrResources()
      throws Exception {
    Path parent = Files.createTempDirectory("pw-observation-test");
    Path workRoot = parent.resolve("must-not-exist");
    WorkerResources.Profile profile =
        new WorkerResources.Profile() {
          public String identity() {
            return "limited";
          }

          public WorkerResources create() {
            throw new AssertionError("must not create a process cgroup");
          }
        };
    var a =
        WorkFilesContext.fromContext(
            parent.resolve("op-a"), Tree.getDefaultInstance(), Command.getDefaultInstance());
    var b =
        WorkFilesContext.fromContext(
            parent.resolve("op-b"), Tree.getDefaultInstance(), Command.getDefaultInstance());
    var first =
        PersistentExecutor.prepareWorker(
            a,
            "op-a",
            ImmutableList.of("/bin/compiler", "@a.params"),
            ImmutableMap.of(),
            workRoot,
            profile);
    var second =
        PersistentExecutor.prepareWorker(
            b,
            "op-b",
            ImmutableList.of("/bin/compiler", "@b.params"),
            ImmutableMap.of(),
            workRoot,
            profile);
    assertThat(first.key()).isEqualTo(second.key());
    assertThat(first.requestArgs()).isNotEqualTo(second.requestArgs());
    assertThat(Files.exists(workRoot)).isFalse();
    Files.delete(parent);
  }
}
