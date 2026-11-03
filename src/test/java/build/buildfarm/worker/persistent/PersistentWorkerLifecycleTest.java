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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerResources;

@RunWith(JUnit4.class)
public class PersistentWorkerLifecycleTest {
  @Test
  public void leaseAndReleaseTrackRequestOwnership() {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);

    PersistentWorkerLifecycle.Lease lease = lifecycle.lease(worker, "operation-1");

    PersistentWorkerLifecycle.Snapshot leased = lifecycle.snapshot(worker).orElseThrow();
    assertThat(leased.state()).isEqualTo(PersistentWorkerLifecycle.State.LEASED);
    assertThat(leased.generation()).isEqualTo(lease.generation());
    assertThat(leased.requestId()).isEqualTo("operation-1");

    assertThat(lifecycle.release(lease)).isTrue();
    PersistentWorkerLifecycle.Snapshot idle = lifecycle.snapshot(worker).orElseThrow();
    assertThat(idle.state()).isEqualTo(PersistentWorkerLifecycle.State.IDLE);
    assertThat(idle.requestId()).isEmpty();
  }

  @Test
  public void staleLeaseCannotChangeNewRequest() {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);
    PersistentWorkerLifecycle.Lease first = lifecycle.lease(worker, "operation-1");
    assertThat(lifecycle.release(first)).isTrue();
    PersistentWorkerLifecycle.Lease second = lifecycle.lease(worker, "operation-2");

    assertThat(lifecycle.release(first)).isFalse();
    assertThat(lifecycle.beginRetiring(first)).isFalse();

    PersistentWorkerLifecycle.Snapshot snapshot = lifecycle.snapshot(worker).orElseThrow();
    assertThat(snapshot.state()).isEqualTo(PersistentWorkerLifecycle.State.LEASED);
    assertThat(snapshot.generation()).isEqualTo(second.generation());
    assertThat(snapshot.requestId()).isEqualTo("operation-2");
  }

  @Test
  public void cannotLeaseWorkerTwice() {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);
    lifecycle.lease(worker, "operation-1");

    assertThrows(IllegalStateException.class, () -> lifecycle.lease(worker, "operation-2"));
  }

  @Test
  public void idleRetirementRequiresCurrentIdleGeneration() {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);
    PersistentWorkerLifecycle.Lease lease = lifecycle.lease(worker, "operation-1");

    assertThat(lifecycle.beginRetiringIdle(worker, 0)).isFalse();
    assertThat(lifecycle.release(lease)).isTrue();
    assertThat(lifecycle.beginRetiringIdle(worker, 0)).isFalse();
    assertThat(lifecycle.beginRetiringIdle(worker, lease.generation())).isTrue();
    assertThat(lifecycle.snapshot(worker).orElseThrow().state())
        .isEqualTo(PersistentWorkerLifecycle.State.RETIRING);
  }

  @Test
  public void terminationRemovesWorker() {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);
    lifecycle.beginRetiring(worker);

    lifecycle.terminated(worker);

    assertThat(lifecycle.snapshot(worker)).isEmpty();
  }

  @Test
  public void oldResourceLeaseCannotChangeOrFreezeNewRequest() throws Exception {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    WorkerResources delegate = mock(WorkerResources.class);
    when(worker.getResources()).thenReturn(delegate);
    lifecycle.register(worker);
    var first = lifecycle.lease(worker, "first");
    WorkerResources old = lifecycle.resourcesFor(first);
    old.setCpu(100000);
    lifecycle.release(first);
    var second = lifecycle.lease(worker, "second");
    lifecycle.resourcesFor(second).setCpu(200000);
    assertThrows(IllegalStateException.class, () -> old.setCpu(50000));
    assertThrows(IllegalStateException.class, old::resume);
    assertThrows(IllegalStateException.class, old::idle);
    verify(delegate).setCpu(100000);
    verify(delegate).setCpu(200000);
    verify(delegate, never()).setCpu(50000);
    verify(delegate, never()).idle();
    verify(delegate, never()).resume();
  }
}
