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
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import build.buildfarm.cas.cfc.CASFileCache.Entry;
import io.grpc.Deadline;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CasInodeIndexTest {
  private static Entry entry(String key) {
    return Entry.orphan(key, /* size= */ 1, Deadline.after(10, HOURS));
  }

  @Test
  public void increment_zeroToOne_insertsIntoIndex() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    index.increment(e, fileKey);

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(index.get(fileKey)).isSameInstanceAs(e);
    assertThat(index.size()).isEqualTo(1);
  }

  @Test
  public void increment_oneToMany_keepsSingleIndexEntry() {
    // One source file referenced by many directories collapses to a single index entry whose count
    // tracks the number of CAS-directory hardlinks.
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    for (int i = 0; i < 10; i++) {
      index.increment(e, fileKey);
    }

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(10);
    assertThat(index.size()).isEqualTo(1);
    assertThat(index.get(fileKey)).isSameInstanceAs(e);
  }

  @Test
  public void decrement_oneToZero_removesFromIndex() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();
    index.increment(e, fileKey);

    // The 1->0 transition returns 0, which is the signal the eviction walker uses to wake the
    // shard.
    assertThat(index.decrement(e, fileKey)).isEqualTo(0);

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();
    assertThat(index.size()).isEqualTo(0);
  }

  @Test
  public void decrement_aboveOne_keepsIndexEntry() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();
    index.increment(e, fileKey);
    index.increment(e, fileKey);

    // Still pinned after this decrement, so the returned count is non-zero and no wake is owed.
    assertThat(index.decrement(e, fileKey)).isEqualTo(1);

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(index.get(fileKey)).isSameInstanceAs(e);
  }

  @Test
  public void inodeReuse_remapsToNewEntryOnlyAfterPriorOccupantReleased() {
    // Two distinct Entries key on the same inode identity across its lifetime — the inode-reuse
    // case the increment/decrement re-read pattern must handle. The OS can hand fileKey's inode to
    // a new file only after the prior occupant's last hardlink is gone (count back to 0, mapping
    // removed, Entry evicted), so at most one Entry ever holds count > 0 for the key at a time.
    // Walk that lifecycle and assert the index always tracks the live Entry and never strands a
    // dead one. A broken compute (acting on a pre-read snapshot, or overwriting/removing the wrong
    // mapping) would leave the new occupant unmapped or the old one resurrected.
    CasInodeIndex index = new CasInodeIndex();
    Entry oldOccupant = entry("old");
    Entry newOccupant = entry("new");
    Object fileKey = new Object(); // same inode identity, reused across occupants

    index.increment(oldOccupant, fileKey);
    assertThat(index.get(fileKey)).isSameInstanceAs(oldOccupant);

    // Old occupant's last hardlink released: its mapping is removed before the inode can be reused.
    assertThat(index.decrement(oldOccupant, fileKey)).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();

    // Inode reused by a fresh Entry; the index now tracks it, not the dead old occupant.
    index.increment(newOccupant, fileKey);
    assertThat(index.get(fileKey)).isSameInstanceAs(newOccupant);
    assertThat(newOccupant.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(oldOccupant.casDirectoryHardlinkCount()).isEqualTo(0);

    // New occupant releases cleanly too, leaving the index empty.
    assertThat(index.decrement(newOccupant, fileKey)).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();
    assertThat(index.size()).isEqualTo(0);
  }

  @Test
  public void decrement_oldEntryDoesNotRemoveNewMappingForSameFileKey() {
    CasInodeIndex index = new CasInodeIndex();
    Entry oldOccupant = entry("old");
    Entry newOccupant = entry("new");
    Object fileKey = new Object();

    index.increment(oldOccupant, fileKey);
    index.increment(newOccupant, fileKey);
    assertThat(index.get(fileKey)).isSameInstanceAs(newOccupant);

    assertThat(index.decrement(oldOccupant, fileKey)).isEqualTo(0);

    assertThat(index.get(fileKey)).isSameInstanceAs(newOccupant);
    assertThat(newOccupant.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(oldOccupant.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void decrement_underflow_failsLoudly() {
    // A decrement without a matching increment is a refcount-discipline bug; fail loud rather than
    // silently leak the index entry / leave the Entry permanently unevictable.
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    assertThrows(IllegalStateException.class, () -> index.decrement(e, fileKey));

    // The failed decrement is rolled back so the counter does not go negative.
    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void concurrentBalancedIncrementsDecrements_leaveCountZeroAndMapEmpty() throws Exception {
    // Stress the re-read-inside-compute pattern: many threads drive one inode's count across the
    // 0<->1 boundary repeatedly. Each thread's increments and decrements are balanced, so at
    // quiescence the count must be exactly 0 and the map must be empty (invariant 7: present iff
    // count > 0). A broken compute (e.g. acting on the pre-compute snapshot instead of re-reading)
    // would strand a stale mapping or throw a spurious underflow.
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    int threads = 8;
    int iterations = 5000;
    CyclicBarrier start = new CyclicBarrier(threads);
    CountDownLatch done = new CountDownLatch(threads);
    CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();

    for (int t = 0; t < threads; t++) {
      Thread worker =
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int i = 0; i < iterations; i++) {
                    index.increment(e, fileKey);
                    index.decrement(e, fileKey);
                  }
                } catch (Throwable failure) {
                  failures.add(failure);
                } finally {
                  done.countDown();
                }
              });
      worker.setDaemon(true);
      worker.start();
    }

    assertThat(done.await(30, SECONDS)).isTrue();
    assertThat(failures).isEmpty();
    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(index.size()).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();
  }

  @Test
  public void distinctInodes_trackIndependently() {
    CasInodeIndex index = new CasInodeIndex();
    Entry a = entry("a");
    Entry b = entry("b");
    Object keyA = new Object();
    Object keyB = new Object();

    index.increment(a, keyA);
    index.increment(b, keyB);

    assertThat(index.size()).isEqualTo(2);
    assertThat(index.get(keyA)).isSameInstanceAs(a);
    assertThat(index.get(keyB)).isSameInstanceAs(b);

    index.decrement(a, keyA);
    assertThat(index.size()).isEqualTo(1);
    assertThat(index.get(keyA)).isNull();
    assertThat(index.get(keyB)).isSameInstanceAs(b);
  }
}
