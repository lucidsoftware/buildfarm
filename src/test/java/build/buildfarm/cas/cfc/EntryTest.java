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

package build.buildfarm.cas.cfc;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.cas.cfc.CASFileCache.Entry;
import io.grpc.Deadline;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Deterministic, single-threaded tests for the Entry state machine.
 *
 * <p>The 3-state lifecycle (LIVE -> EVICTING -> EVICTED, with one legal rollback EVICTING -> LIVE)
 * and the atomic refCount are the primitives by which acquirers and the evictor's sweep mutually
 * exclude. Every transition reachable without a concurrent thread is pinned here. The multi-thread
 * A3 race (acquirer's state recheck after refCount increment) is intentionally not exercised — it
 * would require either a brute-force racing test (whose outcome distribution is JVM-scheduler-
 * dependent) or invasive test hooks on Entry's private VarHandle fields.
 */
@RunWith(JUnit4.class)
public class EntryTest {
  @Test
  public void entryNewConstructorShouldStartReferencedAndLive() {
    Entry e = new Entry("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.refCount()).isEqualTo(1);
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    assertThat(e.isEvictable()).isFalse();
  }

  @Test
  public void entryOrphanFactoryShouldStartUnreferencedAndUnlinked() {
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    // Must NOT be self-linked: a concurrent tryAcquire between storage.put and the
    // publication's LRU append would otherwise see isLinked()==true and have its
    // unlinkReferenced drive unreferencedCount negative for a counter that was never
    // incremented.
    assertThat(e.isLinked()).isFalse();
  }

  @Test
  public void tryAcquireShouldIncrementRefCountAndReportPreviousValue() {
    Entry e = new Entry("k", 7L, Deadline.after(10, SECONDS));
    // Initial refCount is 1 — acquire should observe previous value 1 and bump to 2.
    int prev = e.tryAcquire();
    assertThat(prev).isEqualTo(1);
    assertThat(e.refCount()).isEqualTo(2);
  }

  @Test
  public void tryAcquireShouldReportZeroPreviousForOrphanEntryRevival() {
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    int prev = e.tryAcquire();
    // Previous value 0 indicates the caller drove the 0 -> 1 revival; the caller is
    // responsible for unlinking the entry from the LRU under lruLock.
    assertThat(prev).isEqualTo(0);
    assertThat(e.refCount()).isEqualTo(1);
  }

  @Test
  public void tryAcquireShouldFailWhenEntryIsEvicting() {
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.tryEvict()).isTrue();
    assertThat(e.state()).isEqualTo(Entry.State.EVICTING);
    int prev = e.tryAcquire();
    assertThat(prev).isLessThan(0);
    // Refcount stays at 0 — the failed acquire reverts the speculative bump.
    assertThat(e.refCount()).isEqualTo(0);
  }

  @Test
  public void releaseShouldReturnTrueOnFinalOneToZeroTransition() {
    Entry e = new Entry("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.refCount()).isEqualTo(1);
    assertThat(e.release()).isTrue();
    assertThat(e.refCount()).isEqualTo(0);
  }

  @Test
  public void releaseShouldReturnFalseWhenRefCountStaysPositive() {
    Entry e = new Entry("k", 7L, Deadline.after(10, SECONDS));
    e.tryAcquire(); // 1 -> 2
    assertThat(e.release()).isFalse();
    assertThat(e.refCount()).isEqualTo(1);
  }

  @Test
  public void releaseShouldThrowOnUnderflow() {
    Entry e = new Entry("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.release()).isTrue();
    IllegalStateException thrown = null;
    try {
      e.release();
    } catch (IllegalStateException ise) {
      thrown = ise;
    }
    assertThat(thrown).isNotNull();
    assertThat(thrown).hasMessageThat().contains("release with refCount");
    // Refcount must not be left in a transient negative state.
    assertThat(e.refCount()).isEqualTo(0);
  }

  @Test
  public void tryEvictShouldSucceedOnlyOnceFromLive() {
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.tryEvict()).isTrue();
    assertThat(e.state()).isEqualTo(Entry.State.EVICTING);
    // Second tryEvict must fail because state is no longer LIVE.
    assertThat(e.tryEvict()).isFalse();
  }

  @Test
  public void tryEvictShouldRollbackWhenRefCountNonZero() {
    Entry e = new Entry("k", 7L, Deadline.after(10, SECONDS));
    // Held by initial constructor refcount = 1.
    assertThat(e.tryEvict()).isFalse();
    // State must rollback to LIVE so the entry remains usable, and the rollback
    // must not perturb the holder's refcount.
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    assertThat(e.refCount()).isEqualTo(1);
  }

  @Test
  public void tryEvictShouldRollbackWhenCasDirectoryHardlinkPinned() {
    // A source pinned by a CAS-directory hardlink (casDirectoryHardlinkCount > 0) must not be
    // evictable even at refCount == 0: a _dir tree depends on the inode and the source's blocks are
    // accounted to this Entry. tryEvict re-reads the count under the EVICTING state and rolls back
    // to LIVE — the authoritative guard behind the sweep's pre-skip optimization. Regression test
    // for the eviction/hardlink TOCTOU (a stale pre-skip read could otherwise reach tryEvict, which
    // checked only refCount and would have evicted the live-hardlinked source).
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    new CasInodeIndex().increment(e, new Object());
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(e.isEvictable()).isFalse();

    assertThat(e.tryEvict()).isFalse();
    // State rolled back to LIVE so the entry stays usable; the pin is untouched.
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(1);
  }

  @Test
  public void tryAcquireShouldFailWhenEntryIsEvicted() {
    // Companion to tryAcquireShouldFailWhenEntryIsEvicting: once the evictor has driven
    // the entry through the terminal EVICTING -> EVICTED transition, a stale caller
    // holding the Entry reference (e.g. via a pre-removal storage.get) must still see
    // tryAcquire fail. Without this, a late acquirer could resurrect a fully-evicted
    // entry whose file has already been unlinked.
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.tryEvict()).isTrue();
    e.completeEviction();
    assertThat(e.state()).isEqualTo(Entry.State.EVICTED);
    int prev = e.tryAcquire();
    assertThat(prev).isLessThan(0);
    // Refcount stays at 0 — the failed acquire reverts the speculative bump.
    assertThat(e.refCount()).isEqualTo(0);
  }

  @Test
  public void completeEvictionShouldMarkEntryTerminal() {
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.tryEvict()).isTrue();
    e.completeEviction();
    assertThat(e.state()).isEqualTo(Entry.State.EVICTED);
  }

  @Test
  public void completeEvictionFromLiveShouldFailLoudly() {
    // The state machine's terminal transition is EVICTING -> EVICTED only. A caller that
    // skips tryEvict and invokes completeEviction directly on a LIVE entry must surface a
    // checkState failure (not a silent state flip) so the protocol bug is observable in
    // production, where -ea is off.
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    IllegalStateException thrown = null;
    try {
      e.completeEviction();
    } catch (IllegalStateException ise) {
      thrown = ise;
    }
    assertThat(thrown).isNotNull();
    // Entry must remain LIVE — the failed CAS does not partially transition.
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
  }

  @Test
  public void completeEvictionTwiceShouldFailLoudly() {
    // Double-complete is a protocol violation that would otherwise be silent.
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.tryEvict()).isTrue();
    e.completeEviction();
    IllegalStateException thrown = null;
    try {
      e.completeEviction();
    } catch (IllegalStateException ise) {
      thrown = ise;
    }
    assertThat(thrown).isNotNull();
    assertThat(e.state()).isEqualTo(Entry.State.EVICTED);
  }
}
