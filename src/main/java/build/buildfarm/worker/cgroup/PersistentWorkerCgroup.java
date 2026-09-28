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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import persistent.bazel.client.WorkerResources;

/** A cgroup whose lifetime is the compiler's lifetime, not an operation's lifetime. */
public final class PersistentWorkerCgroup implements WorkerResources {
  private static final Logger logger = Logger.getLogger(PersistentWorkerCgroup.class.getName());
  private static final Duration STATE_WAIT = Duration.ofSeconds(5);
  private static final Set<String> REQUIRED_CONTROLLERS = Set.of("cpu", "memory");
  private static final Object parentInitializationLock = new Object();
  private static final Set<Path> initializedParents = new HashSet<>();
  private final Path path;
  private final List<String> prefix;
  private boolean retiring;
  private boolean terminated;

  public static Profile profile(Group parent, String wrapper, long memoryBytes) {
    if (memoryBytes < 0) {
      throw new IllegalArgumentException("negative memory limit");
    }
    // The factory captures only stable process policy, never an operation or CPU lease.
    return new Profile() {
      public String identity() {
        return "cgroup-v2:" + parent.getPath() + ":" + wrapper + ":memory=" + memoryBytes;
      }

      public WorkerResources create() throws IOException {
        ensureParentControllers(parent);
        Group group = parent.getChild(UUID.randomUUID().toString());
        PersistentWorkerCgroup resources =
            new PersistentWorkerCgroup(
                group.getPath(), List.of(wrapper, "-g", "cpu,memory:" + group.getHierarchy()));
        try {
          group.getCpu().setCpu(1000);
          group.create("memory");
          if (!Files.exists(group.getPath().resolve("cgroup.kill"))) {
            throw new IOException(
                "Persistent worker cgroups require kernel support for cgroup.kill");
          }
          Files.writeString(
              group.getPath().resolve("memory.max"),
              memoryBytes == 0 ? "max" : Long.toString(memoryBytes),
              StandardOpenOption.WRITE);
          // A newly launched wrapper stops on joining this group until a request activates it.
          Files.writeString(group.getPath().resolve("cgroup.freeze"), "1");
          return resources;
        } catch (IOException | RuntimeException e) {
          try {
            resources.terminate(Duration.ZERO);
          } catch (IOException | InterruptedException cleanup) {
            if (cleanup instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            e.addSuppressed(cleanup);
          }
          throw e;
        }
      }
    };
  }

  private static void ensureParentControllers(Group parent) throws IOException {
    Path path = parent.getPath();
    synchronized (parentInitializationLock) {
      if (initializedParents.contains(path)) {
        return;
      }
      // Directory creation alone does not mean that its controllers are ready for children.
      // Finish the shared hierarchy before concurrent worker starts create separate leaf groups.
      parent.create("memory");
      Group.ensureControllerIsEnabled(path, REQUIRED_CONTROLLERS, true);
      Set<String> enabled =
          Set.of(Files.readString(path.resolve("cgroup.subtree_control")).trim().split("\\s+"));
      if (!enabled.containsAll(REQUIRED_CONTROLLERS)) {
        throw new IOException(
            "Persistent worker cgroup controllers not enabled at " + path + ": " + enabled);
      }
      initializedParents.add(path);
    }
  }

  PersistentWorkerCgroup(Path path, List<String> prefix) {
    this.path = path;
    this.prefix = List.copyOf(prefix);
  }

  @Override
  public List<String> launchPrefix() {
    return prefix;
  }

  @Override
  public synchronized Map<String, Long> sample() {
    if (terminated) {
      return Map.of();
    }
    try {
      Map<String, Long> counters = new HashMap<>();
      for (String line : Files.readAllLines(path.resolve("cpu.stat"))) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 2) {
          counters.put("cpu." + parts[0], Long.parseLong(parts[1]));
        }
      }
      return Map.copyOf(counters);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public synchronized void setCpu(int cpuMicros) throws IOException {
    requireActive();
    if (cpuMicros <= 0) {
      throw new IllegalArgumentException("CPU quota must be positive");
    }
    Files.writeString(path.resolve("cpu.max"), cpuMicros + " 100000");
  }

  @Override
  public synchronized void resume() throws IOException, InterruptedException {
    requireActive();
    Files.writeString(path.resolve("cgroup.freeze"), "0");
    if (!waitFor("frozen", "0", STATE_WAIT)) {
      throw new IOException("Persistent worker cgroup did not unfreeze: " + path);
    }
  }

  @Override
  public synchronized void idle() throws IOException, InterruptedException {
    requireActive();
    Files.writeString(path.resolve("cgroup.freeze"), "1");
    if (!waitFor("frozen", "1", STATE_WAIT)) {
      throw new IOException("Persistent worker cgroup did not freeze: " + path);
    }
  }

  private void requireActive() throws IOException {
    if (retiring) {
      throw new IOException("Persistent worker cgroup is retiring: " + path);
    }
  }

  @Override
  public synchronized boolean terminate(Duration grace) throws IOException, InterruptedException {
    if (grace.isNegative()) {
      throw new IllegalArgumentException("negative termination grace");
    }
    if (terminated) {
      return true;
    }
    retiring = true;
    if (Files.exists(path)) {
      boolean empty = false;
      try {
        if (!grace.isZero()) {
          // Signal before thawing so idle workers do not begin a new burst of work on retirement.
          try (var paths = Files.walk(path)) {
            for (Path procs :
                paths.filter(p -> p.getFileName().toString().equals("cgroup.procs")).toList()) {
              for (String pid : Files.readAllLines(procs)) {
                ProcessHandle.of(Long.parseLong(pid)).ifPresent(ProcessHandle::destroy);
              }
            }
          }
          Files.writeString(path.resolve("cgroup.freeze"), "0");
        }
        empty = waitFor("populated", "0", grace);
      } catch (InterruptedException e) {
        Files.writeString(path.resolve("cgroup.kill"), "1");
        throw e;
      } catch (IOException e) {
        // A failed graceful signal/read must not prevent the final group-wide kill.
        logger.log(
            Level.WARNING, "Could not gracefully retire persistent worker cgroup " + path, e);
      }
      if (!empty) {
        // Kernel cgroup.kill covers concurrent forks and descendants whose parent has exited.
        Files.writeString(path.resolve("cgroup.kill"), "1");
        if (!waitFor("populated", "0", grace.isZero() ? Duration.ofSeconds(1) : grace)) {
          return false;
        }
      }
      try (var paths = Files.walk(path)) {
        for (Path dir :
            paths.filter(Files::isDirectory).sorted(java.util.Comparator.reverseOrder()).toList()) {
          Files.delete(dir);
        }
      }
    }
    terminated = true;
    return true;
  }

  private boolean waitFor(String field, String value, Duration timeout)
      throws IOException, InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    do {
      for (String line : Files.readAllLines(path.resolve("cgroup.events"))) {
        if (line.equals(field + " " + value)) {
          return true;
        }
      }
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        return false;
      }
      TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(10)));
    } while (true);
  }
}
