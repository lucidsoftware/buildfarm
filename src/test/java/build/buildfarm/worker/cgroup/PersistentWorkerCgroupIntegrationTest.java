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

package build.buildfarm.worker.cgroup;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.pool2.PooledObject;
import org.junit.Test;
import persistent.bazel.client.*;
import persistent.common.processes.JavaProcessWrapper;
import persistent.testutil.ProcessUtils;
import persistent.testutil.WorkerUtils;

/** Kernel-backed tests requiring a writable cgroup v2 hierarchy. */
public class PersistentWorkerCgroupIntegrationTest {
  @Test(timeout = 30000)
  public void concurrentColdStartsInitializeSharedParent() throws Exception {
    String configured = System.getProperty("buildfarm.pw.cgroupParent");
    if (configured == null) {
      throw new IllegalStateException(
          "Supply -Dbuildfarm.pw.cgroupParent=/sys/fs/cgroup/<delegated-test-parent>");
    }
    Path delegated = Path.of(configured).toRealPath();
    Path root = Group.getRoot().getPath().toRealPath();
    if (!delegated.startsWith(root) || !Files.isWritable(delegated) || !Files.isWritable(root)) {
      throw new IllegalArgumentException(
          "The delegated test parent must be under a writable Buildfarm cgroup root: " + root);
    }
    // Match normal worker startup: initialize the cgroup root before accepting executions.
    Group startup = Group.getRoot().getChild("buildfarm-pw-startup-test-" + UUID.randomUUID());
    startup.getCpu().setCpu(1000);
    startup.getCpu().close();

    for (int round = 0; round < 30; round++) {
      Group parent = Group.getRoot();
      for (Path component : root.relativize(delegated)) {
        if (!component.toString().isEmpty()) {
          parent = parent.getChild(component.toString());
        }
      }
      runConcurrentColdStart(parent.getChild("buildfarm-pw-cold-test-" + UUID.randomUUID()));
    }
  }

  private static void runConcurrentColdStart(Group coldParent) throws Exception {
    var pool = Executors.newFixedThreadPool(16);
    var start = new CountDownLatch(1);
    List<Future<?>> starts = new ArrayList<>();
    try {
      var profile = PersistentWorkerCgroup.profile(coldParent, "/bin/true", 1073741824);
      for (int i = 0; i < 16; i++) {
        starts.add(
            pool.submit(
                () -> {
                  start.await();
                  PersistentWorkerCgroup worker = (PersistentWorkerCgroup) profile.create();
                  try {
                    assertThat(Files.readString(coldParent.getPath().resolve("cgroup.subtree_control")))
                        .contains("memory");
                  } finally {
                    worker.terminate(Duration.ZERO);
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : starts) {
        future.get();
      }
      assertThat(Files.readString(coldParent.getPath().resolve("cgroup.subtree_control")))
          .contains("memory");
    } finally {
      pool.shutdownNow();
      if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Persistent worker start tasks did not stop");
      }
      for (int attempt = 0; attempt < 100; attempt++) {
        try {
          Files.deleteIfExists(coldParent.getPath());
          break;
        } catch (FileSystemException e) {
          if (!e.getMessage().contains("Device or resource busy") || attempt == 99) {
            throw e;
          }
          Thread.sleep(10);
        }
      }
    }
  }

  @Test(timeout = 30000)
  public void reusesFrozenCompilerAndTerminatesItsCgroup() throws Exception {
    Path path = createGroup();
    PersistentWorkerCgroup group =
        new PersistentWorkerCgroup(
            path,
            List.of(
                "/bin/sh",
                "-c",
                "echo $$ > \"$1/cgroup.procs\"; shift; exec \"$@\"",
                "pw-launch",
                path.toString()));
    try {
      Files.writeString(path.resolve("memory.max"), "1073741824");
      Files.writeString(path.resolve("cgroup.freeze"), "1");
      Path work = Files.createTempDirectory("pw-cgroup-test-");
      Path jar =
          ProcessUtils.retrieveFileResource(
              getClass().getClassLoader(), "adder-bin_deploy.jar", work.resolve("adder.jar"));
      WorkerKey key =
          WorkerUtils.emptyWorkerKey(
                  work,
                  ImmutableList.of(
                      JavaProcessWrapper.CURRENT_JVM_COMMAND,
                      "-cp",
                      jar.toString(),
                      "adder.Adder",
                      "--persistent_worker"))
              .withResourceProfile(
                  new WorkerResources.Profile() {
                    public String identity() {
                      return "test-memory=1073741824";
                    }

                    public WorkerResources create() {
                      return group;
                    }
                  });
      CommonsWorkerPool pool =
          new CommonsWorkerPool(
              new WorkerSupervisor() {
                @Override
                public PersistentWorker create(WorkerKey key) throws Exception {
                  return new PersistentWorker(key, UUID.randomUUID().toString());
                }

                @Override
                public void destroyObject(WorkerKey key, PooledObject<PersistentWorker> pooled) {
                  try {
                    assertThat(pooled.getObject().terminate(Duration.ofSeconds(1))).isTrue();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                }
              },
              1);
      try {
        PersistentWorker first = pool.obtain(key);
        group.setCpu(100000);
        group.resume();
        assertThat(
                first
                    .doWork(WorkRequest.newBuilder().addArguments("2").addArguments("4").build())
                    .getOutput())
            .isEqualTo("6");
        group.idle();
        List<String> pids = Files.readAllLines(path.resolve("cgroup.procs"));
        assertThat(pids).isNotEmpty();
        long idleCpu = group.sample().get("cpu.usage_usec");
        Thread.sleep(150);
        assertThat(group.sample().get("cpu.usage_usec")).isEqualTo(idleCpu);
        pool.release(key, first);
        PersistentWorker second = pool.obtain(key);
        assertThat(second).isSameInstanceAs(first);
        group.setCpu(200000);
        group.resume();
        assertThat(
                second
                    .doWork(WorkRequest.newBuilder().addArguments("13").addArguments("37").build())
                    .getOutput())
            .isEqualTo("50");
        group.idle();
        assertThat(Files.readAllLines(path.resolve("cgroup.procs")))
            .containsAtLeastElementsIn(pids);
        assertThat(Files.readString(path.resolve("cpu.max")).trim()).isEqualTo("200000 100000");
        assertThat(Files.readString(path.resolve("memory.max")).trim()).isEqualTo("1073741824");
        pool.release(key, second);
      } finally {
        pool.close();
      }
      assertThat(Files.exists(path)).isFalse();
    } finally {
      group.terminate(Duration.ZERO);
    }
  }

  @Test(timeout = 15000)
  public void retirementKillsChildAfterItsParentExited() throws Exception {
    Path path = createGroup();
    PersistentWorkerCgroup group = new PersistentWorkerCgroup(path, List.of());
    try {
      Process parent =
          new ProcessBuilder(
                  "/bin/sh",
                  "-c",
                  "echo $$ > \"$1/cgroup.procs\"; /bin/sleep 60 & exit 0",
                  "pw-parent",
                  path.toString())
              .redirectOutput(ProcessBuilder.Redirect.DISCARD)
              .redirectError(ProcessBuilder.Redirect.DISCARD)
              .start();
      parent.getOutputStream().close();
      assertThat(parent.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(Files.readAllLines(path.resolve("cgroup.procs"))).isNotEmpty();
      group.idle();
      assertThat(group.terminate(Duration.ZERO)).isTrue();
      assertThat(Files.exists(path)).isFalse();
    } finally {
      group.terminate(Duration.ZERO);
    }
  }

  private static Path createGroup() throws Exception {
    String configured = System.getProperty("buildfarm.pw.cgroupParent");
    if (configured == null) {
      throw new IllegalStateException(
          "Supply -Dbuildfarm.pw.cgroupParent=/sys/fs/cgroup/<delegated-test-parent>");
    }
    Path parent = Path.of(configured).toRealPath();
    if (!parent.startsWith("/sys/fs/cgroup") || !Files.isWritable(parent)) {
      throw new IllegalArgumentException("Expected a writable delegated cgroup parent");
    }
    return Files.createDirectory(parent.resolve("buildfarm-pw-test-" + UUID.randomUUID()));
  }
}
