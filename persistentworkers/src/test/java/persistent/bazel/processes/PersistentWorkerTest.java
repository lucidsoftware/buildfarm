// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistent.bazel.processes;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerResources;
import persistent.common.processes.JavaProcessWrapper;
import persistent.testutil.ProcessUtils;
import persistent.testutil.WorkerUtils;

@RunWith(JUnit4.class)
public class PersistentWorkerTest {
  static WorkResponse sendAddRequest(PersistentWorker worker, Path stdErrLog, int x, int y)
      throws IOException, InterruptedException {
    ImmutableList<String> arguments = ImmutableList.of(String.valueOf(x), String.valueOf(y));

    WorkRequest request =
        WorkRequest.newBuilder().addAllArguments(arguments).setRequestId(0).build();

    WorkResponse response;
    try {
      response = worker.doWork(request);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.err.println(Files.readAllLines(stdErrLog));
      throw e;
    }
    return response;
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void endToEndAdder() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");

    String filename = "adder-bin_deploy.jar";

    Path jarPath =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, workDir.resolve(filename));

    ImmutableList<String> initCmd =
        ImmutableList.of(
            JavaProcessWrapper.CURRENT_JVM_COMMAND,
            "-cp",
            jarPath.toString(),
            "adder.Adder",
            "--persistent_worker");

    WorkerKey key = WorkerUtils.emptyWorkerKey(workDir, initCmd);

    Path stdErrLog = workDir.resolve("test-err.log");
    PersistentWorker worker = new PersistentWorker(key, "worker-dir");

    WorkResponse response = sendAddRequest(worker, stdErrLog, 2, 4);

    Assert.assertEquals(response.getOutput(), "6");
    Assert.assertEquals(response.getExitCode(), 0);
    Assert.assertEquals(worker.getExitValue(), Optional.empty()); // Not yet exited

    WorkResponse response2 = sendAddRequest(worker, stdErrLog, 13, 37);

    Assert.assertEquals(response2.getOutput(), "50");
    Assert.assertEquals(response2.getExitCode(), 0);
    Assert.assertEquals(worker.getExitValue(), Optional.empty()); // Not yet exited
  }

  @Test
  public void doWork_afterProcessDestroyed_throwsIOException() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");

    String filename = "adder-bin_deploy.jar";

    Path jarPath =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, workDir.resolve(filename));

    ImmutableList<String> initCmd =
        ImmutableList.of(
            JavaProcessWrapper.CURRENT_JVM_COMMAND,
            "-cp",
            jarPath.toString(),
            "adder.Adder",
            "--persistent_worker");

    WorkerKey key = WorkerUtils.emptyWorkerKey(workDir, initCmd);
    PersistentWorker worker = new PersistentWorker(key, "worker-dir");

    // Verify the worker is functional
    WorkRequest request =
        WorkRequest.newBuilder().addArguments("1").addArguments("2").setRequestId(0).build();
    WorkResponse response = worker.doWork(request);
    Assert.assertEquals("3", response.getOutput());
    Assert.assertEquals(0, response.getExitCode());

    // Kill the worker process
    worker.destroy();

    // Verify that doing work on a destroyed worker results in an error. This also somewhat checks
    // that the kind of exception encountered during doWork is propagated up rather than a null
    // being returned and a RuntimeException later being thrown.
    WorkRequest secondRequest =
        WorkRequest.newBuilder().addArguments("3").addArguments("4").setRequestId(0).build();
    assertThrows(IOException.class, () -> worker.doWork(secondRequest));
  }

  @Test
  public void terminateWaitsForProcessExit() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");
    String filename = "adder-bin_deploy.jar";
    Path jarPath =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, workDir.resolve(filename));
    ImmutableList<String> initCmd =
        ImmutableList.of(
            JavaProcessWrapper.CURRENT_JVM_COMMAND,
            "-cp",
            jarPath.toString(),
            "adder.Adder",
            "--persistent_worker");
    PersistentWorker worker =
        new PersistentWorker(WorkerUtils.emptyWorkerKey(workDir, initCmd), "worker-dir");

    assertThat(worker.terminate(Duration.ofSeconds(1))).isTrue();
    assertThat(worker.getExitValue()).isPresent();
  }

  @Test
  public void resourceProfileSeparatesLimitsButNotIndividualProcessIdentity() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");
    WorkerKey base = WorkerUtils.emptyWorkerKey(workDir, ImmutableList.of("compiler"));
    WorkerResources.Profile first = profile("memory=1024", new AtomicInteger());
    WorkerResources.Profile second = profile("memory=1024", new AtomicInteger());
    WorkerResources.Profile different = profile("memory=2048", new AtomicInteger());
    assertThat(base.withResourceProfile(first)).isEqualTo(base.withResourceProfile(second));
    assertThat(base.withResourceProfile(first).hashCode())
        .isEqualTo(base.withResourceProfile(second).hashCode());
    assertThat(base.withResourceProfile(first)).isNotEqualTo(base.withResourceProfile(different));
  }

  @Test
  public void failedLaunchCleansUpProcessOwnedResources() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");
    AtomicInteger terminations = new AtomicInteger();
    WorkerKey key =
        WorkerUtils.emptyWorkerKey(
                workDir, ImmutableList.of(workDir.resolve("missing-compiler").toString()))
            .withResourceProfile(profile("test", terminations));
    assertThrows(IOException.class, () -> new PersistentWorker(key, "process"));
    assertThat(terminations.get()).isEqualTo(1);
  }

  private static WorkerResources.Profile profile(String identity, AtomicInteger terminations) {
    return new WorkerResources.Profile() {
      public String identity() {
        return identity;
      }

      public WorkerResources create() {
        return new WorkerResources() {
          public boolean terminate(Duration grace) {
            terminations.incrementAndGet();
            return true;
          }
        };
      }
    };
  }
}
