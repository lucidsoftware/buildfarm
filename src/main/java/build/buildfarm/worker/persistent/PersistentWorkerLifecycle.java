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

package build.buildfarm.worker.persistent;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.java.Log;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerResources;

/** Authoritative request ownership and lifecycle state for persistent-worker processes. */
@Log
final class PersistentWorkerLifecycle {
  enum State {
    IDLE,
    LEASED,
    RETIRING,
    TERMINATED
  }

  /** Identifies one particular use of a process; generations are never reused. */
  record Lease(PersistentWorker worker, long generation, String requestId) {}

  record Snapshot(State state, long generation, String requestId, Duration idleDuration) {}

  private static final class Entry {
    private State state = State.IDLE;
    private long generation;
    private String requestId = "";
    private long idleSinceNanos = System.nanoTime();
  }

  private final Map<PersistentWorker, Entry> entries = new IdentityHashMap<>();
  private final AtomicLong nextGeneration = new AtomicLong();

  synchronized void register(PersistentWorker worker) {
    checkState(!entries.containsKey(worker), "persistent worker is already registered");
    entries.put(worker, new Entry());
    PersistentWorkerMetrics.lifecycleTransition("starting", "idle");
  }

  synchronized Lease lease(PersistentWorker worker, String requestId) {
    Entry entry = requireEntry(worker);
    checkState(entry.state == State.IDLE, "persistent worker is not idle: %s", entry.state);
    long generation = nextGeneration.incrementAndGet();
    entry.state = State.LEASED;
    entry.generation = generation;
    entry.requestId = requestId;
    PersistentWorkerMetrics.lifecycleTransition("idle", "leased");
    return new Lease(worker, generation, requestId);
  }

  synchronized boolean release(Lease lease) {
    Entry entry = entries.get(lease.worker());
    if (!matches(entry, lease, State.LEASED)) {
      return false;
    }
    entry.state = State.IDLE;
    entry.requestId = "";
    entry.idleSinceNanos = System.nanoTime();
    PersistentWorkerMetrics.lifecycleTransition("leased", "idle");
    return true;
  }

  synchronized boolean beginRetiring(Lease lease) {
    Entry entry = entries.get(lease.worker());
    if (!matches(entry, lease, State.LEASED)) {
      return false;
    }
    entry.state = State.RETIRING;
    PersistentWorkerMetrics.lifecycleTransition("leased", "retiring");
    return true;
  }

  synchronized boolean beginRetiringIdle(PersistentWorker worker, long generation) {
    Entry entry = entries.get(worker);
    if (entry == null || entry.state != State.IDLE || entry.generation != generation) {
      return false;
    }
    entry.state = State.RETIRING;
    PersistentWorkerMetrics.lifecycleTransition("idle", "retiring");
    return true;
  }

  synchronized void beginRetiring(PersistentWorker worker) {
    Entry entry = entries.get(worker);
    if (entry == null || entry.state == State.RETIRING || entry.state == State.TERMINATED) {
      return;
    }
    String from = stateLabel(entry.state);
    entry.state = State.RETIRING;
    PersistentWorkerMetrics.lifecycleTransition(from, "retiring");
  }

  synchronized void terminated(PersistentWorker worker) {
    Entry entry = entries.remove(worker);
    if (entry == null) {
      return;
    }
    PersistentWorkerMetrics.lifecycleTransition(stateLabel(entry.state), "terminated");
    entry.state = State.TERMINATED;
  }

  synchronized Optional<Snapshot> snapshot(PersistentWorker worker) {
    Entry entry = entries.get(worker);
    if (entry == null) {
      return Optional.empty();
    }
    Duration idleDuration =
        entry.state == State.IDLE
            ? Duration.ofNanos(Math.max(0, System.nanoTime() - entry.idleSinceNanos))
            : Duration.ZERO;
    return Optional.of(new Snapshot(entry.state, entry.generation, entry.requestId, idleDuration));
  }

  /** A request can adjust resources only while it owns this exact generation. */
  WorkerResources resourcesFor(Lease lease) {
    WorkerResources delegate = lease.worker().getResources();
    if (delegate == WorkerResources.NONE) {
      return delegate;
    }
    return new WorkerResources() {
      private void checkLease() {
        checkState(
            matches(entries.get(lease.worker()), lease, State.LEASED),
            "persistent worker resource lease is no longer current");
      }

      public Map<String, Long> sample() {
        synchronized (PersistentWorkerLifecycle.this) {
          checkLease();
          return delegate.sample();
        }
      }

      public void setCpu(int micros) throws IOException {
        synchronized (PersistentWorkerLifecycle.this) {
          checkLease();
          delegate.setCpu(micros);
        }
      }

      public void resume() throws IOException, InterruptedException {
        synchronized (PersistentWorkerLifecycle.this) {
          checkLease();
          delegate.resume();
        }
      }

      public void idle() throws IOException, InterruptedException {
        synchronized (PersistentWorkerLifecycle.this) {
          checkLease();
          delegate.idle();
        }
      }
    };
  }

  private Entry requireEntry(PersistentWorker worker) {
    Entry entry = entries.get(worker);
    checkState(entry != null, "persistent worker is not registered");
    return entry;
  }

  private static boolean matches(Entry entry, Lease lease, State expectedState) {
    return entry != null
        && entry.state == expectedState
        && entry.generation == lease.generation()
        && entry.requestId.equals(lease.requestId());
  }

  private static String stateLabel(State state) {
    return state.name().toLowerCase(java.util.Locale.US);
  }
}
