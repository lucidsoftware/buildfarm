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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import build.buildfarm.cas.cfc.CASFileCache.Entry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;
import io.prometheus.client.Counter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import lombok.extern.java.Log;

/**
 * Owns the array of {@link EvictorShard}s the CAS is partitioned into and routes keys to shards by
 * {@code hash(key) & (N - 1)}. Each shard independently owns its LRU list, byte accounting, evictor
 * thread, and snapshot file; the {@link CASFileCache#storage} map and the async-cleanup executor
 * are shared across shards. At {@code N == 1} this degenerates to a single evictor holding the
 * whole cache (the pre-2.2 behavior), with every {@code shardFor} returning shard 0.
 *
 * <p>The global byte total is the on-demand sum of per-shard {@code shardBytes} ({@link
 * #globalTotalBytes}); there is no separate global counter, because eviction triggers and
 * backpressure read only the owning shard's bytes. The sum is consulted only on the metrics-scrape
 * path and by external {@code size()} observers.
 */
@Log
final class CasShards {
  private static final Counter lruMigrationTotal =
      Counter.build()
          .name("lru_migration_total")
          .labelNames("old_n", "new_n", "outcome")
          .help("LRU snapshot shard-count migrations performed at startup.")
          .register();

  private final EvictorShard[] shards;
  private final int shardCount;
  private final int mask;

  CasShards(
      CasEvictorBacking backing,
      int shardCount,
      ExecutorService asyncCleanupExecutor,
      Clock clock,
      Entry[] headers,
      long maxSizeInBytes,
      long lowBytes,
      long wakeBudgetNanos,
      long idleHeartbeatNanos,
      LRUDB lruDb,
      Path root) {
    checkArgument(Integer.bitCount(shardCount) == 1, "shardCount must be a power of two");
    checkArgument(headers.length == shardCount, "one header per shard required");
    this.shardCount = shardCount;
    this.mask = shardCount - 1;
    this.shards = new EvictorShard[shardCount];
    // Watermarks divide evenly across shards. Integer division can leave the per-shard caps summing
    // to slightly less than the global cap; the drift is bounded by shardCount bytes and is fine
    // (byte accounting is approximate per plan principle 1). A cap smaller than shardCount yields
    // perShardPark == 0, which EvictorShard rejects at construction — an intentional loud failure
    // for the nonsensical "tiny cap, many shards" misconfiguration rather than a silent 0-cap
    // shard.
    //
    // Invariant: dividing the global cap into N independent per-shard caps assumes bytes distribute
    // roughly evenly across shards. That holds for Buildfarm's workload — keys are SHA-256 hex
    // digests that avalanche through shardFor's hash, and eviction pressure only exists when the
    // cache is near-full (many resident entries K), where per-shard byte CoV ~ sqrt(N/K) is tiny.
    // It would NOT hold for a cache dominated by a few very large entries (small K, high size
    // variance); the 2 GiB maxEntrySizeBytes cap and the many-small-blobs CAS workload keep us out
    // of that regime.
    long perShardPark = maxSizeInBytes / shardCount;
    long perShardLow = lowBytes / shardCount;
    for (int i = 0; i < shardCount; i++) {
      shards[i] =
          new EvictorShard(
              backing,
              i,
              shardCount,
              asyncCleanupExecutor,
              clock,
              headers[i],
              perShardPark,
              perShardLow,
              wakeBudgetNanos,
              idleHeartbeatNanos,
              lruDb,
              root.resolve(LruSnapshotFiles.name(shardCount, i)));
    }
  }

  /**
   * Auto-derive the shard count from vCPU and worker role. CAS-only workers parallelize harder
   * against bimodal {@code _dir} eviction cost ({@code vCPU / 4}); exec (and combined) workers run
   * fewer writes/sec and reserve threads for execution ({@code vCPU / 8}). The result is rounded up
   * to a power of two so the hash routing is uniform. {@code N == 1} on small workers is correct —
   * the per-shard isolation benefit only matters where the formula naturally gives {@code N >= 2}.
   */
  static int deriveShardCount(int vCpu, boolean executionRole) {
    int divisor = executionRole ? 8 : 4;
    // ceilingPowerOfTwo returns x when already a power of two, else the next one up (x >= 1).
    return IntMath.ceilingPowerOfTwo(Math.max(1, vCpu / divisor));
  }

  int shardCount() {
    return shardCount;
  }

  @VisibleForTesting
  EvictorShard shard(int index) {
    return shards[index];
  }

  /**
   * Route {@code key} to its owning shard.
   *
   * <p>The routing hash is {@link String#hashCode()}, masked to the shard count. Two invariants
   * ride on this and must not be broken:
   *
   * <ul>
   *   <li><b>JDK stability.</b> {@code String.hashCode()} is specified by the JLS ({@code
   *       s[0]*31^(n-1) + ... + s[n-1]}) and is stable across JDK versions. It must NOT be replaced
   *       with {@code Objects.hash}, a {@code HashMap}-seeded hash, or any randomized/version-
   *       dependent function: the shard index is persisted in snapshot filenames ({@code
   *       lru_num_shards_<N>_shard_<I>.txt}) and a silent change would mis-route every key relative
   *       to its on-disk snapshot.
   *   <li><b>Uniform on hex digests.</b> The 64-char SHA-256 hex keys avalanche the polynomial
   *       hash; {@code hashCode() & (N - 1)} is empirically uniform across shards (CoV within
   *       statistical noise of random). No Murmur-style finalizer is needed.
   * </ul>
   *
   * <p>{@code hashCode() & mask} is always non-negative because {@code mask} (a power-of-two minus
   * one) clears the sign bit, so no {@code Math.abs} is required.
   */
  EvictorShard shardFor(String key) {
    return shards[key.hashCode() & mask];
  }

  // === Lifecycle ===

  void start() {
    for (EvictorShard shard : shards) {
      shard.start();
    }
  }

  void stop() throws InterruptedException {
    for (EvictorShard shard : shards) {
      shard.requestStop();
    }
    InterruptedException interrupted = null;
    for (EvictorShard shard : shards) {
      try {
        shard.awaitStopped();
      } catch (InterruptedException e) {
        // Stop every shard even if one stop() is interrupted; re-raise after the loop so a single
        // interrupt can't leave later shards' threads running.
        interrupted = e;
      }
    }
    if (interrupted != null) {
      throw interrupted;
    }
  }

  // === Producer-side routing ===

  void charge(String key, long bytes) throws InterruptedException, IOException {
    shardFor(key).charge(bytes);
  }

  void discharge(String key, long bytes) {
    shardFor(key).discharge(bytes);
  }

  void offerInsertion(Entry entry) throws InterruptedException, IOException {
    shardFor(entry.key).offerInsertion(entry);
  }

  void offerStaleEntry(Entry entry) throws InterruptedException, IOException {
    shardFor(entry.key).offerStaleEntry(entry);
  }

  /** Group keys by owning shard and dispatch each group to its shard's lossy accessEvents queue. */
  void recordAccess(Iterable<String> keys) {
    Iterator<String> it = keys.iterator();
    if (!it.hasNext()) {
      return;
    }
    String first = it.next();
    if (!it.hasNext()) {
      // Single key — the dominant case (contains() routes one key here). Dispatch directly and
      // skip the per-shard bucket-array + ArrayList allocation on this warm path (plan principle
      // 10).
      shards[first.hashCode() & mask].recordAccess(Collections.singletonList(first));
      return;
    }
    List<String>[] byShard = newKeyBuckets();
    addToBucket(byShard, first);
    while (it.hasNext()) {
      addToBucket(byShard, it.next());
    }
    for (int i = 0; i < shardCount; i++) {
      if (byShard[i] != null) {
        shards[i].recordAccess(byShard[i]);
      }
    }
  }

  private void addToBucket(List<String>[] byShard, String key) {
    int i = key.hashCode() & mask;
    if (byShard[i] == null) {
      byShard[i] = new ArrayList<>();
    }
    byShard[i].add(key);
  }

  @SuppressWarnings("unchecked")
  private List<String>[] newKeyBuckets() {
    return (List<String>[]) new List<?>[shardCount];
  }

  // === Startup linking ===

  /** Link a startup-scanned orphan entry onto its owning shard's LRU and reserve its bytes. */
  void linkAtStartup(Entry e, long diskSize) {
    shardFor(e.key).linkAtStartup(e, diskSize);
  }

  // === Aggregates (scrape / size() path; not on the eviction hot path) ===

  long globalTotalBytes() {
    long sum = 0;
    for (EvictorShard shard : shards) {
      sum += shard.currentShardBytes();
    }
    return sum;
  }

  long globalUnreferencedCount() {
    long sum = 0;
    for (EvictorShard shard : shards) {
      sum += shard.currentUnreferencedCount();
    }
    return sum;
  }

  int getEvictedCount() {
    long sum = 0;
    for (EvictorShard shard : shards) {
      sum += shard.getEvictedCount();
    }
    return (int) Math.min(sum, Integer.MAX_VALUE);
  }

  long getEvictedSize() {
    long sum = 0;
    for (EvictorShard shard : shards) {
      sum += shard.getEvictedSize();
    }
    return sum;
  }

  // === Snapshot migration ===

  /**
   * Persist every shard's LRU to its current-N snapshot file and, only if all writes succeed,
   * delete the {@code staleFiles} (stale-N snapshots and the legacy {@code lru.txt}) the entries
   * were migrated from. {@code oldNLabels} is the set of source shard-counts (or "legacy") for the
   * migration metric.
   *
   * <p>Crash safety: snapshots are recency HINTS, never the source of truth — the filesystem scan
   * re-admits every blob regardless — so no CAS data is ever lost here, only LRU warmth. Stale
   * files are deleted strictly after every new file's atomic rename completes, so a JVM crash
   * mid-migration still recovers warmth from the new files, the still-present stale files, or
   * (worst case) a cold scan. The {@code AtomicFileWriter} rename is metadata-atomic but not
   * fsync-durable, so a host crash (power loss) in the narrow window after a stale-file delete but
   * before the new file's data reaches disk can lose that shard's warmth — never its data.
   *
   * <p>Must run during the startup scan, before {@link #start} spawns the evictor threads, since it
   * walks each shard's LRU without the single-writer invariant in force.
   */
  void migrateSnapshots(Collection<Path> staleFiles, Collection<String> oldNLabels) {
    boolean allWritten = true;
    for (EvictorShard shard : shards) {
      try {
        shard.writeSnapshotNow();
      } catch (IOException e) {
        allWritten = false;
        log.log(
            Level.SEVERE,
            format("snapshot migration: failed writing shard %d; aborting migration", shard.index),
            e);
      }
    }
    String outcome;
    if (allWritten) {
      outcome = "succeeded";
      for (Path stale : staleFiles) {
        try {
          Files.deleteIfExists(stale);
        } catch (IOException e) {
          log.log(Level.WARNING, "snapshot migration: failed deleting stale file " + stale, e);
        }
      }
    } else {
      outcome = "failed";
      log.log(
          Level.SEVERE,
          "snapshot migration left stale files in place for retry on the next startup");
    }
    String newN = Integer.toString(shardCount);
    for (String oldN : oldNLabels) {
      lruMigrationTotal.labels(oldN, newN, outcome).inc();
    }
  }
}
