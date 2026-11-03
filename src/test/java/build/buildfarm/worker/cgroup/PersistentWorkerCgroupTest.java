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
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PersistentWorkerCgroupTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void quotaAndIdleTransitionsUseV2Interfaces() throws Exception {
    Path dir = temporary.newFolder().toPath();
    Files.writeString(dir.resolve("cpu.max"), "max 100000");
    Files.writeString(dir.resolve("memory.max"), "1048576");
    Files.writeString(dir.resolve("cgroup.freeze"), "1");
    Files.writeString(dir.resolve("cgroup.events"), "populated 1\nfrozen 0\n");
    PersistentWorkerCgroup group = new PersistentWorkerCgroup(dir, List.of("launcher"));
    group.setCpu(200000);
    group.resume();
    assertThat(Files.readString(dir.resolve("cpu.max"))).isEqualTo("200000 100000");
    assertThat(Files.readString(dir.resolve("cgroup.freeze"))).isEqualTo("0");
    Files.writeString(dir.resolve("cgroup.events"), "populated 1\nfrozen 1\n");
    group.idle();
    assertThat(Files.readString(dir.resolve("cgroup.freeze"))).isEqualTo("1");
    assertThat(Files.readString(dir.resolve("memory.max"))).isEqualTo("1048576");
  }

  @Test
  public void interruptedRetirementStillRequestsWholeGroupKill() throws Exception {
    Path dir = temporary.newFolder().toPath();
    Files.writeString(dir.resolve("cgroup.events"), "populated 1\nfrozen 1\n");
    Files.writeString(dir.resolve("cgroup.kill"), "0");
    PersistentWorkerCgroup group = new PersistentWorkerCgroup(dir, List.of());
    try {
      Thread.currentThread().interrupt();
      assertThrows(InterruptedException.class, () -> group.terminate(Duration.ofSeconds(1)));
    } finally {
      Thread.interrupted();
    }
    assertThat(Files.readString(dir.resolve("cgroup.kill"))).isEqualTo("1");
    assertThrows(java.io.IOException.class, () -> group.setCpu(100000));
    assertThrows(java.io.IOException.class, group::resume);
  }

  @Test
  public void samplesAreProcessCumulativeCounters() throws Exception {
    Path dir = temporary.newFolder().toPath();
    Files.writeString(dir.resolve("cpu.stat"), "usage_usec 123\nnr_periods 10\nthrottled_usec 5\n");
    PersistentWorkerCgroup group = new PersistentWorkerCgroup(dir, List.of());
    assertThat(group.sample())
        .containsExactly("cpu.usage_usec", 123L, "cpu.nr_periods", 10L, "cpu.throttled_usec", 5L);
  }
}
