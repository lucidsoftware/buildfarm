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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.cas.cfc.CASFileCache.Entry;
import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.jctools.queues.varhandle.MpscUnboundedVarHandleArrayQueue;
import org.jctools.queues.varhandle.MpscVarHandleArrayQueue;

// === Cache-line padding via inheritance (Phase 2.2) ===
//
// EvictorShard instances live in an EvictorShard[] managed by CasShards. Two adjacent shards'
// objects can be placed adjacently on the heap, and a shard's hot mutable fields (the byte
// counter, the wake flags, the lock) are written from producer threads on every charge. Without
// padding, those writes would dirty a cache line that another core is spinning on for a NEIGHBORING
// shard's read-only fields -> cross-core ping-pong under hot-key skew.
//
// We isolate the hot fields by sandwiching them between two 120-byte pad regions declared in
// superclasses. HotSpot lays superclass fields before subclass fields, so the hot block lands
// after EvictorShardPad0's pad and before EvictorShardPad1's pad; EvictorShard's own (cold) fields
// follow the trailing pad. The result: each shard's hot block is >=120 bytes away from any other
// shard object's fields on both sides. What this isolates is the in-object volatile scalars (the
// wake flags, parked, the sequence/timestamp longs) written on the wake path; shardBytes/dequeued-
// Count/lock/cond are references, and their write-contended state lives in the separately-allocated
// LongAdder Cell[] / ReentrantLock AQS objects, which self-isolate (LongAdder.Cell is @Contended;
// JCTools pads its queue producer/consumer indices). We deliberately do NOT use @Contended (it
// needs -XX:-RestrictContended at deploy time) and do NOT double-pad those already-padded
// structures. The JOL layout regression test pins this layout against future field-addition
// regressions. See plan principle 3: per-Entry padding (69 GB at 540M entries) is forbidden; this
// per-shard pad is ~7.7 KB at N=32. (The dominant per-shard cost is not the pad but the pre-
// allocated MPSC backing arrays: the insertion and stale queues each eagerly allocate a 64K-ref
// chunk and the access queue a 32K-ref one, so ~640 KB/shard under compressed oops (4-byte refs)
// and ~1.25 MB/shard with 8-byte refs on a >32 GB heap. Either way trivial against a CAS worker's
// heap.)
abstract class EvictorShardPad0 {
  @SuppressWarnings("unused")
  byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014;
  @SuppressWarnings("unused")
  byte p015, p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029;
  @SuppressWarnings("unused")
  byte p030, p031, p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044;
  @SuppressWarnings("unused")
  byte p045, p046, p047, p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059;
  @SuppressWarnings("unused")
  byte p060, p061, p062, p063, p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074;
  @SuppressWarnings("unused")
  byte p075, p076, p077, p078, p079, p080, p081, p082, p083, p084, p085, p086, p087, p088, p089;
  @SuppressWarnings("unused")
  byte p090, p091, p092, p093, p094, p095, p096, p097, p098, p099, p100, p101, p102, p103, p104;
  @SuppressWarnings("unused")
  byte p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116, p117, p118, p119;
}

/** Hot mutable fields written from producer threads and the evictor thread, isolated by padding. */
abstract class EvictorShardHotRefs extends EvictorShardPad0 {
  // Per-shard byte accounting. Producers add on charge; the evictor subtracts on eviction. The
  // ground-truth eviction/backpressure trigger (sum() vs the per-shard watermarks).
  final LongAdder shardBytes = new LongAdder();
  // Incremented by the evictor on each MPSC poll; read by producers for the cheap depth probe.
  final LongAdder dequeuedCount = new LongAdder();

  final ReentrantLock lock = new ReentrantLock();
  final Condition cond = lock.newCondition();

  volatile boolean workPending;
  volatile boolean snapshotPending;
  volatile boolean stopping;

  // True while the evictor thread is parked on {@link #cond}. Producers check this BEFORE
  // taking the lock so a steady-state {@code requestDrain} on a running evictor is a single
  // volatile read instead of a full mutex acquire. Written by the evictor inside the park block;
  // read by producers without the lock. The wait loop re-checks the predicate AFTER publishing
  // parked=true (Dekker handshake) so a producer that set workPending and observed parked==false
  // (and thus skipped its signal) cannot be lost.
  volatile boolean parked;

  // Published by the evictor under {@link #lock} on each pre-await; readers (tests calling
  // awaitQuiescence) read it without the lock. Single-writer + volatile gives the happens-before
  // edge tests need.
  volatile long quiescedSequence;

  volatile long lastWakeNanos;

  // Wall-clock (System.nanoTime) heartbeat for the per-shard heartbeat-age gauge; distinct from
  // lastWakeNanos, which uses the injectable nanosSupplier so tests can drive it deterministically.
  volatile long heartbeatWallNanos = System.nanoTime();
}

/** Trailing pad separating the hot block from EvictorShard's cold fields and neighbor objects. */
abstract class EvictorShardPad1 extends EvictorShardHotRefs {
  @SuppressWarnings("unused")
  byte q000, q001, q002, q003, q004, q005, q006, q007, q008, q009, q010, q011, q012, q013, q014;
  @SuppressWarnings("unused")
  byte q015, q016, q017, q018, q019, q020, q021, q022, q023, q024, q025, q026, q027, q028, q029;
  @SuppressWarnings("unused")
  byte q030, q031, q032, q033, q034, q035, q036, q037, q038, q039, q040, q041, q042, q043, q044;
  @SuppressWarnings("unused")
  byte q045, q046, q047, q048, q049, q050, q051, q052, q053, q054, q055, q056, q057, q058, q059;
  @SuppressWarnings("unused")
  byte q060, q061, q062, q063, q064, q065, q066, q067, q068, q069, q070, q071, q072, q073, q074;
  @SuppressWarnings("unused")
  byte q075, q076, q077, q078, q079, q080, q081, q082, q083, q084, q085, q086, q087, q088, q089;
  @SuppressWarnings("unused")
  byte q090, q091, q092, q093, q094, q095, q096, q097, q098, q099, q100, q101, q102, q103, q104;
  @SuppressWarnings("unused")
  byte q105, q106, q107, q108, q109, q110, q111, q112, q113, q114, q115, q116, q117, q118, q119;
}

/**
 * One asynchronous evictor shard that owns its slice of the LRU list and shardBytes accounting, and
 * serves as the lock-free integration point between producer cache calls ({@code charge}, {@code
 * recordAccess}) and the per-{@link Entry} atomic state machine. {@link CasShards} owns an array of
 * these and routes keys to shards by {@code hash(key) & (N - 1)}; at {@code N == 1} a single shard
 * holds the whole cache (the pre-2.2 behavior).
 *
 * <h2>Wake protocol</h2>
 *
 * Three {@code volatile boolean} flags accessed under {@link #lock}: {@code workPending} (set by
 * producers crossing a wake threshold), {@code snapshotPending} (set by the snapshot scheduler),
 * {@code stopping} (set by {@link #stop}). Producers MUST NOT hold {@link #lock} while computing
 * what they signal. {@code workPending} is a wake-DEBOUNCE flag, not a work-presence flag — work
 * presence is carried by the MPSC queues and {@code shardBytes}, which the park predicate re-checks
 * directly. The consumer clears {@code workPending} only at the idle gate, after a drain+sweep has
 * established quiescence, and re-checks the predicate AFTER publishing {@code parked=true} (the
 * Dekker handshake on {@link #parked}) so a producer that set {@code workPending} and observed
 * {@code parked==false} (and thus skipped its signal) cannot be lost. Correctness rests on (1)
 * predicate re-check on every wake, (2) {@link #idleHeartbeatNanos} heartbeat self-heals lost
 * signals, (3) {@code shardBytes.sum() > lowBytes} is the ground-truth trigger.
 *
 * <h2>Notifications</h2>
 *
 * Three MPSC queues:
 *
 * <ul>
 *   <li>{@link #insertionEvents} (unbounded must-not-lose) — carries published {@link Entry}
 *       references that need to land on the LRU. Drain-side guard {@code state == LIVE &&
 *       !isLinked() && refCount == 0} resolves concurrent eviction races.
 *   <li>{@link #staleEvents} (unbounded must-not-lose) — carries stale entries discovered on read.
 *       Dropping one can strand stale storage metadata after the canonical file is gone.
 *   <li>{@link #accessEvents} (bounded drop-newest) — carries recordAccess key strings. Drops
 *       degrade LRU recency only, never correctness — refcount governs eviction.
 * </ul>
 *
 * <h2>Snapshot</h2>
 *
 * The shard's evictor thread walks its LRU and writes its own snapshot file ({@code
 * lru_num_shards_<N>_shard_<index>.txt}) synchronously. Per-shard snapshots parallelize the walk
 * across shards; first fires are staggered by {@code index} so they don't all trigger at once.
 */
@Log
final class EvictorShard extends EvictorShardPad1 {
  static final int INSERTION_QUEUE_CAPACITY = 64 * 1024;
  static final int ACCESS_QUEUE_CAPACITY = 32 * 1024;
  private static final int INSERTION_DRAIN_BATCH = 8192;
  private static final int ACCESS_DRAIN_BATCH = 4096;
  static final long HARD_CAP_PARK_TIMEOUT_SECONDS = 300L;
  private static final long SNAPSHOT_INTERVAL_MS = 600_000L;
  private static final long OVERDUE_SNAPSHOT_FACTOR = 2L;
  private static final long RESTART_BACKOFF_INITIAL_MS = 1000L;
  private static final long RESTART_BACKOFF_CAP_MS = 60_000L;
  // Consecutive successful iterations required before decrementing restartCount. Decay-per-
  // success would let a 50% intermittent failure pin restartCount in {0, 1, 2} — alternating
  // success/fail keeps backoff at 1-2s exactly when the underlying infra needs breathing room.
  private static final int RESTART_DECAY_SUCCESS_STREAK = 8;

  // Operator-facing defaults: 50 ms per-sweep wall-budget, 2 s heartbeat self-heal interval.
  // Production tuning lives in Cas config; these are the test-fixture / direct-constructor
  // defaults.
  static final long DEFAULT_WAKE_BUDGET_NANOS = 50_000_000L;
  static final long DEFAULT_IDLE_HEARTBEAT_NANOS = 2_000_000_000L;

  // Wake threshold offsets: wake when shardBytes crosses (parkBytes - 2%) so producers
  // typically observe sub-cap state before they reach the hard cap; clamp above lowBytes
  // by 1% of cap so a config with low close to park still yields a non-empty wake window.
  private static final int WAKE_BELOW_PARK_PERCENT = 2;
  private static final int WAKE_ABOVE_LOW_PERCENT = 1;

  // awaitQuiescence stability window: how long must atRest persist before declaring quiesced.
  private static final long QUIESCENCE_STABILITY_MS = 50L;
  // awaitQuiescence poll cadence.
  private static final long QUIESCENCE_POLL_INTERVAL_MS = 5L;

  // === Prometheus metrics (lock-free counters; Prometheus Histogram for the
  //     evictor-thread-recorded histograms — single-writer, no producer-side contention) ===

  // All counters carry a {shard} label so per-shard rates are distinguishable; the high-cardinality
  // histograms (evictions_per_wake, sweep_duration, skip_prefix_length, snapshot_duration) stay
  // GLOBAL per plan step 11 to avoid N_SHARDS x quantiles series. Per-shard children are resolved
  // once into instance fields in the constructor so the producer hot path calls inc() on a cached
  // Child rather than re-resolving labels.
  private static final Counter evictedTotal =
      Counter.build()
          .name("evictor_evicted_total")
          .labelNames("shard")
          .help("Successful evictions.")
          .register();
  private static final Counter stateRollbacksTotal =
      Counter.build()
          .name("evictor_state_rollbacks_total")
          .labelNames("shard")
          .help("tryEvict EVICTING -> LIVE rollbacks.")
          .register();
  private static final Counter wakesTotal =
      Counter.build()
          .name("evictor_wakes_total")
          .labelNames("shard", "trigger")
          .help("Evictor wakeups by trigger.")
          .register();

  private static final String TRIGGER_BYTE_THRESHOLD = "byte_threshold";
  private static final String TRIGGER_QUEUE_DEPTH = "queue_depth";
  private static final String TRIGGER_SNAPSHOT = "snapshot";
  private static final Counter mpscDropsTotal =
      Counter.build()
          .name("mpsc_drops_total")
          .labelNames("shard", "queue")
          .help("MPSC queue offer-full drops.")
          .register();
  private static final Counter expireEntryFallbackFailureTotal =
      Counter.build()
          .name("expire_entry_fallback_failure_total")
          .labelNames("shard", "reason")
          .help("Delegate-fallback failures during eviction, labeled by exception simple name.")
          .register();
  private static final Counter restartsTotal =
      Counter.build()
          .name("evictor_restarts_total")
          .labelNames("shard", "reason")
          .help("Evictor loop body restarts.")
          .register();
  private static final Counter lruSnapshotFailureTotal =
      Counter.build()
          .name("lru_snapshot_failure_total")
          .labelNames("shard", "error_class")
          .help("Snapshot write failures.")
          .register();

  private static final Gauge queueDepthGauge =
      Gauge.build()
          .name("evictor_queue_depth")
          .labelNames("shard")
          .help("Approximate insertion and stale-entry queue depth.")
          .register();
  private static final Gauge heartbeatAgeGauge =
      Gauge.build()
          .name("evictor_heartbeat_age_seconds")
          .labelNames("shard")
          .help("Seconds since the shard's evictor loop body last advanced.")
          .register();

  private static final Histogram evictionsPerWake =
      Histogram.build()
          .name("evictor_evictions_per_wake")
          .buckets(1, 4, 16, 64, 256, 1024, 4096)
          .help("Evictions per sweep wake.")
          .register();
  private static final Histogram sweepDurationSeconds =
      Histogram.build()
          .name("evictor_sweep_duration_seconds")
          .buckets(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30)
          .help("Sweep wall time per wake.")
          .register();
  private static final Histogram skipPrefixLength =
      Histogram.build()
          .name("evictor_skip_prefix_length")
          .buckets(0, 1, 4, 16, 64, 256, 1024, 4096)
          .help("Cursor advances per successful eviction.")
          .register();
  private static final Histogram lruSnapshotDurationSeconds =
      Histogram.build()
          .name("lru_snapshot_duration_seconds")
          .buckets(0.1, 0.5, 1, 5, 10, 30, 60, 120)
          .help("Snapshot walk + write wall time.")
          .register();

  // === Configuration ===

  private final CasEvictorBacking backing;
  final int index;
  private final int shardCount;
  private final String shardLabel;
  private final ExecutorService asyncCleanupExecutor;
  private final Clock clock;
  private final long parkBytes;
  private final long wakeBytes;
  private final long lowBytes;
  private final long wakeBudgetNanos;
  private final long idleHeartbeatNanos;
  private final LRUDB lruDb;
  private final Path snapshotPath;

  // === Per-shard cached metric children (resolved once in the constructor) ===

  private final Counter.Child evictedChild;
  private final Counter.Child stateRollbacksChild;
  private final Counter.Child wakesByteThreshold;
  private final Counter.Child wakesQueueDepth;
  private final Counter.Child wakesSnapshot;
  private final Counter.Child mpscDropsAccessChild;
  private final Gauge.Child queueDepthChild;

  // === Concurrency state ===
  // (lock, cond, the wake flags, parked, quiescedSequence, lastWakeNanos, shardBytes, and
  //  dequeuedCount live in the padded superclasses EvictorShardHotRefs / EvictorShardPad0/1.)

  private volatile long lastSnapshotMs;

  // === Hot accounting (single-writer: the evictor thread) ===

  private final LongAdder unreferencedCount = new LongAdder();
  private final LongAdder removedBytes = new LongAdder();
  private final LongAdder removedCount = new LongAdder();
  private final LongAdder stateRollbackCount = new LongAdder();

  // === MPSC queues ===

  private final MpscUnboundedVarHandleArrayQueue<Entry> insertionEvents =
      new MpscUnboundedVarHandleArrayQueue<>(INSERTION_QUEUE_CAPACITY);
  private final MpscVarHandleArrayQueue<String> accessEvents =
      new MpscVarHandleArrayQueue<>(ACCESS_QUEUE_CAPACITY);
  private final MpscUnboundedVarHandleArrayQueue<Entry> staleEvents =
      new MpscUnboundedVarHandleArrayQueue<>(INSERTION_QUEUE_CAPACITY);

  // === LRU list (single-writer: the evictor thread, after start()) ===

  private final Entry header;

  // === Threads ===

  private Thread evictorThread;
  private Thread snapshotSchedulerThread;
  private boolean evictOneDischarged;

  EvictorShard(
      CasEvictorBacking backing,
      int index,
      int shardCount,
      ExecutorService asyncCleanupExecutor,
      Clock clock,
      Entry header,
      long parkBytes,
      long lowBytes,
      long wakeBudgetNanos,
      long idleHeartbeatNanos,
      LRUDB lruDb,
      Path snapshotPath) {
    checkArgument(index >= 0 && index < shardCount, "index must be in [0, shardCount)");
    checkArgument(Integer.bitCount(shardCount) == 1, "shardCount must be a power of two");
    checkArgument(parkBytes > 0, "parkBytes must be positive");
    checkArgument(lowBytes >= 0 && lowBytes <= parkBytes, "lowBytes must be in [0, parkBytes]");
    checkArgument(wakeBudgetNanos > 0, "wakeBudgetNanos must be positive");
    checkArgument(idleHeartbeatNanos > 0, "idleHeartbeatNanos must be positive");
    this.backing = backing;
    this.index = index;
    this.shardCount = shardCount;
    this.shardLabel = Integer.toString(index);
    this.asyncCleanupExecutor = asyncCleanupExecutor;
    this.clock = clock;
    this.header = header;
    this.parkBytes = parkBytes;
    this.lowBytes = lowBytes;
    this.evictedChild = evictedTotal.labels(shardLabel);
    this.stateRollbacksChild = stateRollbacksTotal.labels(shardLabel);
    this.wakesByteThreshold = wakesTotal.labels(shardLabel, TRIGGER_BYTE_THRESHOLD);
    this.wakesQueueDepth = wakesTotal.labels(shardLabel, TRIGGER_QUEUE_DEPTH);
    this.wakesSnapshot = wakesTotal.labels(shardLabel, TRIGGER_SNAPSHOT);
    this.mpscDropsAccessChild = mpscDropsTotal.labels(shardLabel, "access");
    this.queueDepthChild = queueDepthGauge.labels(shardLabel);
    // setChild registers a callback child keyed on shard index alone in the shared static gauge, so
    // it computes age at scrape time without a per-wake metric write. This assumes a single CAS
    // instance per JVM (production): if two caches existed, the second's shard i would replace the
    // first's heartbeat child. The other per-shard metrics use idempotent labels(...) and are
    // unaffected. Tests construct many caches but never assert this gauge, so the replacement is
    // benign there.
    heartbeatAgeGauge.setChild(
        new Gauge.Child() {
          @Override
          public double get() {
            return Math.max(0, System.nanoTime() - heartbeatWallNanos) / 1e9;
          }
        },
        shardLabel);
    // Wake threshold sits at max(2% below park, 1% above low) clamped to at most parkBytes - 1.
    // Picking max() means a config with lowBytes close to parkBytes places the wake near
    // parkBytes - 1, not 2% below park — the goal is a non-empty wake window above lowBytes,
    // not a fixed "2% below" position. The final parkBytes - 1 clamp keeps wake strictly below
    // park even when lowBytes == parkBytes (eager eviction disabled).
    long belowPark = parkBytes - (parkBytes * WAKE_BELOW_PARK_PERCENT / 100);
    long aboveLow = lowBytes + (parkBytes * WAKE_ABOVE_LOW_PERCENT / 100);
    long unclamped = Math.max(belowPark, aboveLow);
    this.wakeBytes = Math.min(unclamped, Math.max(0, parkBytes - 1));
    this.wakeBudgetNanos = wakeBudgetNanos;
    this.idleHeartbeatNanos = idleHeartbeatNanos;
    this.lruDb = lruDb;
    this.snapshotPath = checkNotNull(snapshotPath, "snapshotPath");
  }

  // === Lifecycle ===

  void start() {
    lock.lock();
    try {
      if (evictorThread != null) {
        // Idempotent: tests sometimes call start() implicitly via fileCache.start() AND
        // explicitly during fixture setup. Production callers only call once, so no
        // observable behavior change.
        return;
      }
      lastWakeNanos = nowNanos();
      heartbeatWallNanos = System.nanoTime();
      lastSnapshotMs = clock.millis();
      stopping = false;
      evictorThread = new Thread(this::loop, "buildfarm-cas-evictor-" + index);
      evictorThread.setDaemon(true);
      evictorThread.start();
      snapshotSchedulerThread =
          new Thread(this::snapshotSchedulerLoop, "buildfarm-cas-snapshot-scheduler-" + index);
      snapshotSchedulerThread.setDaemon(true);
      snapshotSchedulerThread.start();
    } finally {
      lock.unlock();
    }
  }

  void stop() throws InterruptedException {
    requestStop();
    awaitStopped();
  }

  void requestStop() {
    Thread scheduler;
    lock.lock();
    try {
      if (evictorThread == null) {
        return;
      }
      stopping = true;
      snapshotPending = true; // shutdown snapshot
      cond.signalAll();
      scheduler = snapshotSchedulerThread;
    } finally {
      lock.unlock();
    }
    if (scheduler != null) {
      scheduler.interrupt();
    }
  }

  void awaitStopped() throws InterruptedException {
    Thread evictor;
    Thread scheduler;
    lock.lock();
    try {
      evictor = evictorThread;
      scheduler = snapshotSchedulerThread;
    } finally {
      lock.unlock();
    }
    if (scheduler != null) {
      scheduler.join();
    }
    if (evictor != null) {
      evictor.join();
    }
    lock.lock();
    try {
      if (evictorThread == evictor) {
        evictorThread = null;
      }
      if (snapshotSchedulerThread == scheduler) {
        snapshotSchedulerThread = null;
      }
    } finally {
      lock.unlock();
    }
  }

  // === Producer-side API ===

  /**
   * Reserve {@code bytes}. Returns once reservation succeeds; throws {@link InterruptedException}
   * if interrupted while parked; {@link IOException} if the 5-minute hard-cap timeout fires.
   */
  void charge(long bytes) throws InterruptedException, IOException {
    shardBytes.add(bytes);
    long current = shardBytes.sum();
    // Wake whenever shardBytes is above the wake watermark; requestDrain's workPending guard
    // debounces redundant signals so steady-state above-watermark charges cost one volatile
    // read each. An earlier "edge-triggered" form computed (current - bytes) and only fired
    // on the producer that observed the crossing, but two concurrent producers each adding
    // less than the wake-margin could both read shardBytes.sum() after both adds had landed
    // and so each see (current - bytes) > wakeBytes — leaving NO producer to fire the wake.
    // Always-fire + workPending-dedup is correct and removes the lost-wake hazard.
    if (current > wakeBytes) {
      requestDrain(TRIGGER_BYTE_THRESHOLD);
    }
    if (current > parkBytes) {
      parkUntilUnderHardCap(bytes);
    }
  }

  /**
   * Add startup-time bytes to {@link #shardBytes} without the wake/park machinery. Production
   * startup goes through {@link #linkAtStartup}, which reserves bytes itself; this finer-grained
   * primitive exists only for tests that construct an {@code EvictorShard} directly and seed its
   * accounting by hand. Safe before {@link #start} since it only accumulates LongAdder cells.
   */
  @VisibleForTesting
  void addBytesAtStartup(long bytes) {
    shardBytes.add(bytes);
  }

  /**
   * Increment the unreferenced count for an entry the caller has already linked onto {@link
   * #header}. Production startup goes through {@link #linkAtStartup}, which does the link and this
   * increment together under the header lock; this primitive exists only for tests that link a
   * victim by hand and need the count to match.
   */
  @VisibleForTesting
  void noteUnreferencedAtStartup() {
    unreferencedCount.increment();
  }

  /**
   * Link a freshly-scanned orphan entry onto this shard's LRU and reserve its bytes, the startup
   * counterpart to the runtime {@code charge} + {@code offerInsertion} path. Startup runs before
   * {@link #start} spawns the evictor thread, so the LRU has no single-writer yet; the parallel
   * scan pool routes many keys to many shards concurrently, so we serialize the pointer mutation on
   * this shard's own {@code header} (per-shard locks contend far less than a single global header
   * lock). The byte reservation is unconditional and matches the link's accounting.
   */
  void linkAtStartup(Entry e, long diskSize) {
    synchronized (header) {
      if (!e.isLinked() && e.state() == Entry.State.LIVE && e.refCount() == 0) {
        e.addBefore(header);
        unreferencedCount.increment();
      }
    }
    shardBytes.add(diskSize);
  }

  /** Reverse a prior {@link #charge} reservation. */
  void discharge(long bytes) {
    shardBytes.add(-bytes);
    // Use plain lock (not tryLock): a missed signal leaves a hard-cap-parked producer
    // waiting until the 2-second heartbeat self-heals, introducing latency variance.
    // discharge is off the producer hot path (writer cancel/error + evictor sweep), so
    // the lock acquire is acceptable.
    signalParkers();
  }

  private void signalParkers() {
    lock.lock();
    try {
      cond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Variant of {@link #discharge} used by the evictor thread inside a sweep. The end-of-sweep block
   * in {@link #sweepWithBudget} issues a single {@code signalAll}, so per-eviction signals would
   * just churn the lock.
   */
  private void dischargeNoSignal(long bytes) {
    shardBytes.add(-bytes);
  }

  /**
   * Enqueue {@code entry} for LRU linking. Used after {@code safeStorageInsertion} returns null and
   * on 1→0 release transitions. This queue is unbounded because insertion events are must-not-lose:
   * dropping one can strand an unreferenced charged entry off the LRU.
   */
  void offerInsertion(Entry entry) throws InterruptedException, IOException {
    checkState(insertionEvents.offer(entry), "unbounded insertion queue rejected entry");
    requestDrain(TRIGGER_QUEUE_DEPTH);
  }

  void offerStaleEntry(Entry entry) throws InterruptedException, IOException {
    checkState(staleEvents.offer(entry), "unbounded stale queue rejected entry");
    requestDrain(TRIGGER_QUEUE_DEPTH);
  }

  /** Lossy: dispatch each key to {@link #accessEvents}; drop on full and count the drop. */
  void recordAccess(Iterable<String> keys) {
    boolean offered = false;
    for (String key : keys) {
      if (accessEvents.offer(key)) {
        offered = true;
      } else {
        mpscDropsAccessChild.inc();
      }
    }
    if (offered) {
      // requestDrain short-circuits on workPending so the steady-state cost is one volatile
      // read; per-batch dedup of the wake signal is unnecessary.
      requestDrain(TRIGGER_QUEUE_DEPTH);
    }
  }

  void requestDrain(String trigger) {
    if (workPending) {
      // The evictor already has work pending and will see this enqueue on its next loop
      // iteration. No signal needed.
      return;
    }
    workPending = true;
    if (trigger == TRIGGER_BYTE_THRESHOLD) {
      wakesByteThreshold.inc();
    } else if (trigger == TRIGGER_QUEUE_DEPTH) {
      wakesQueueDepth.inc();
    } else if (trigger == TRIGGER_SNAPSHOT) {
      wakesSnapshot.inc();
    } else {
      wakesTotal.labels(shardLabel, trigger).inc();
    }
    if (!parked) {
      // Evictor is running; it'll re-check workPending on its next loop iteration. Skip
      // the mutex acquire and signal entirely. The hot-path cost is two volatile reads
      // (workPending and parked) plus one volatile write to workPending. The write is
      // racy (no CAS): two producers can both observe false and both set true, so the
      // trigger counter can over-count under contention. workPending dedup is still
      // correct because the evictor's predicate re-check absorbs the extra signal.
      return;
    }
    lock.lock();
    try {
      // signalAll because the same condvar carries parkUntilUnderHardCap chargers and
      // offerInsertion producers; a plain signal() could wake one of them instead of the
      // evictor and the work-pending wake gets absorbed until the next heartbeat.
      cond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  void requestSnapshot() {
    if (!snapshotPending) {
      snapshotPending = true;
      wakesSnapshot.inc();
      lock.lock();
      try {
        cond.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  // === Accessors ===

  /**
   * Return a monotonic nano timestamp. Default impl uses {@link System#nanoTime} for monotonicity
   * (immune to wall-clock steps); the package-private {@link #setNanosSupplier} lets tests inject a
   * deterministic clock.
   */
  private long nowNanos() {
    return nanosSupplier.getAsLong();
  }

  private volatile LongSupplier nanosSupplier = System::nanoTime;

  @VisibleForTesting
  void setNanosSupplier(LongSupplier supplier) {
    this.nanosSupplier = supplier;
  }

  long currentShardBytes() {
    return shardBytes.sum();
  }

  long currentUnreferencedCount() {
    return unreferencedCount.sum();
  }

  long currentStateRollbackCount() {
    return stateRollbackCount.sum();
  }

  long parkBytes() {
    return parkBytes;
  }

  long lowBytes() {
    return lowBytes;
  }

  long wakeBytes() {
    return wakeBytes;
  }

  /**
   * Operator-facing count of evictions since the last call. {@code sumThenReset} is a best-effort
   * rate counter, not an exact total; the monotonic {@code evictor_evicted_total} is the
   * authoritative completed-eviction count. Clamp to int range rather than silently wrapping on a
   * very long poll gap.
   */
  int getEvictedCount() {
    return (int) Math.min(removedCount.sumThenReset(), Integer.MAX_VALUE);
  }

  long getEvictedSize() {
    return removedBytes.sumThenReset();
  }

  // === Hard-cap park ===

  private void parkUntilUnderHardCap(long bytesReserved) throws InterruptedException, IOException {
    long startNanos = nowNanos();
    long deadlineNanos = startNanos + SECONDS.toNanos(HARD_CAP_PARK_TIMEOUT_SECONDS);
    requestDrain(TRIGGER_BYTE_THRESHOLD);
    try {
      lock.lockInterruptibly();
    } catch (InterruptedException ie) {
      // Lock not held: roll back the reservation but skip signalAll (no lock). Sibling parkers
      // self-heal via the heartbeat or the next discharge's signal.
      shardBytes.add(-bytesReserved);
      throw ie;
    }
    try {
      while (shardBytes.sum() > parkBytes && !stopping) {
        long remainingNanos = deadlineNanos - nowNanos();
        if (remainingNanos <= 0) {
          // Roll back AND wake sibling parkers: the freed bytes are headroom they may need.
          // Without this, a parker timing out leaves the next one to wait for the heartbeat.
          shardBytes.add(-bytesReserved);
          cond.signalAll();
          throw new IOException(
              "cas charge backpressure: parked "
                  + HARD_CAP_PARK_TIMEOUT_SECONDS
                  + "s at hard cap; try another worker");
        }
        cond.await(remainingNanos, NANOSECONDS);
      }
      if (stopping && shardBytes.sum() > parkBytes) {
        // stop() set stopping and signalAll-woke us; the evictor will not run more sweeps so
        // we cannot expect shardBytes to drop. Roll back the reservation and surface a typed
        // failure so the caller exits promptly instead of waiting the full hard-cap timeout.
        shardBytes.add(-bytesReserved);
        cond.signalAll();
        throw new IOException("cas charge backpressure: evictor stopped while parked");
      }
    } catch (InterruptedException ie) {
      shardBytes.add(-bytesReserved);
      cond.signalAll();
      throw ie;
    } finally {
      lock.unlock();
    }
  }

  // === Evictor loop ===

  private void loop() {
    int restartCount = 0;
    int consecutiveSuccesses = 0;
    boolean shutdownSnapshotEmitted = false;
    while (!stopping) {
      lastWakeNanos = nowNanos();
      heartbeatWallNanos = System.nanoTime();
      try {
        long dequeuedBefore = dequeuedCount.sum();
        drainMpsc();
        long dequeuedAfter = dequeuedCount.sum();
        // Widen to long before summing: each size() can transiently report up to Integer.MAX_VALUE
        // (unbounded-queue estimate), and an int sum could overflow negative on the gauge.
        queueDepthChild.set((long) insertionEvents.size() + staleEvents.size());
        if (dequeuedAfter > dequeuedBefore) {
          lock.lock();
          try {
            cond.signalAll();
          } finally {
            lock.unlock();
          }
        }

        if (snapshotPending && snapshotIsOverdue()) {
          // Sustained-pressure preemption: shard stayed above LOW for two intervals; force a
          // snapshot to keep warm-LRU fresh.
          performSnapshot();
          snapshotPending = false;
          if (stopping) shutdownSnapshotEmitted = true;
          continue;
        }

        boolean stuckAboveLow = false;
        if (shardBytes.sum() > lowBytes) {
          int evicted = sweepWithBudget(wakeBudgetNanos);
          if (evicted > 0
              || !insertionEvents.isEmpty()
              || !staleEvents.isEmpty()
              || !accessEvents.isEmpty()) {
            continue;
          }
          // Sweep made no progress and the MPSC queues are empty — every entry on the LRU
          // was either refCount > 0 (resurrected) or there were none. Park on the heartbeat
          // condvar so we don't tight-spin while waiting for chargers to release.
          stuckAboveLow = true;
        }

        if (snapshotPending) {
          performSnapshot();
          snapshotPending = false;
          if (stopping) shutdownSnapshotEmitted = true;
          continue;
        }

        // Park on the heartbeat condvar. The wait predicate accepts two distinct quiescent
        // states: (a) genuinely idle (shardBytes <= lowBytes), or (b) stuck above LOW with
        // nothing evictable yet — in both cases the wake signal comes from charge() (new
        // work) or offerInsertion (new LRU candidate from release).
        lock.lock();
        try {
          while (!workPending
              && !snapshotPending
              && !stopping
              && insertionEvents.isEmpty()
              && staleEvents.isEmpty()
              && accessEvents.isEmpty()
              && (shardBytes.sum() <= lowBytes || stuckAboveLow)) {
            quiescedSequence++;
            parked = true;
            try {
              // Re-check the predicate AFTER parked=true publishes. A producer in
              // requestDrain's fast-path that wrote workPending=true and then read
              // parked=false (and so skipped its signal) is detected here: per JMM
              // SC ordering of volatile reads/writes, the producer's workPending-W
              // store and our parked-W store cannot both precede the corresponding
              // reads in the racing thread. So if the producer's parked-R saw false,
              // our workPending-R here MUST see true. The same logic covers
              // snapshotPending, insertionEvents/accessEvents producers, and discharge
              // callers that arrive between the outer predicate check and the await.
              if (workPending
                  || snapshotPending
                  || stopping
                  || !insertionEvents.isEmpty()
                  || !staleEvents.isEmpty()
                  || !accessEvents.isEmpty()
                  || (shardBytes.sum() > lowBytes && !stuckAboveLow)) {
                continue;
              }
              cond.await(idleHeartbeatNanos, NANOSECONDS);
            } finally {
              parked = false;
            }
            if (stopping) break;
          }
          workPending = false;
        } finally {
          lock.unlock();
        }
        // Decay restartCount only after a streak of successful iterations. Decay-on-every-
        // success would let a 50% intermittent failure pin restartCount in {0, 1, 2} —
        // alternating success/fail keeps backoff at 1-2s exactly when the underlying infra
        // needs breathing room. Requiring RESTART_DECAY_SUCCESS_STREAK successes in a row
        // before decrementing means a sustained failure rate above ~1/N keeps backoff
        // elevated, while a genuinely recovered system winds the counter back down.
        if (restartCount > 0) {
          consecutiveSuccesses++;
          if (consecutiveSuccesses >= RESTART_DECAY_SUCCESS_STREAK) {
            restartCount--;
            consecutiveSuccesses = 0;
          }
        }
      } catch (InterruptedException ie) {
        if (stopping) {
          Thread.currentThread().interrupt();
          break;
        }
        Thread.interrupted();
      } catch (Exception e) {
        // Only transient Exceptions get the backoff-restart treatment: a RuntimeException from
        // a backing hook, or a stray IOException. Errors (OutOfMemoryError, LinkageError, ...)
        // are deliberately NOT caught — they propagate out of the loop and kill the evictor
        // thread. The JVM state after an unrecoverable Error is undefined, so we make no
        // attempt to survive it and install no handler; letting the thread (and, where it
        // matters, the JVM) die is the intended behavior.
        log.log(Level.SEVERE, "evictor loop iteration failed; backoff restart", e);
        String reason = (e instanceof IOException) ? "io_exception" : "runtime_exception";
        restartsTotal.labels(shardLabel, reason).inc();
        restartCount++;
        consecutiveSuccesses = 0;
        long sleepMs =
            Math.min(
                RESTART_BACKOFF_CAP_MS,
                RESTART_BACKOFF_INITIAL_MS << Math.min(restartCount - 1, 6));
        // Wait on the condvar (not Thread.sleep) so stop()'s signalAll wakes us promptly.
        // stop() does not interrupt the evictor thread, only signals; a plain Thread.sleep
        // would block shutdown for up to RESTART_BACKOFF_CAP_MS.
        boolean shutdownDuringBackoff = false;
        lock.lock();
        try {
          if (!stopping) {
            cond.await(sleepMs, MILLISECONDS);
          }
        } catch (InterruptedException ie) {
          if (stopping) {
            Thread.currentThread().interrupt();
            shutdownDuringBackoff = true;
          } else {
            Thread.interrupted();
          }
        } finally {
          lock.unlock();
        }
        if (shutdownDuringBackoff) {
          break;
        }
      }
    }
    // Shutdown snapshot. Skip if the loop body already emitted one after stopping was set —
    // stop() sets both snapshotPending and stopping, so a still-running loop iteration will
    // typically run performSnapshot before noticing stopping. Re-running here doubles the
    // shutdown latency on large workers.
    if (!shutdownSnapshotEmitted) {
      try {
        drainMpscUntilEmpty();
        performSnapshot();
      } catch (Exception e) {
        log.log(Level.WARNING, "shutdown snapshot failed", e);
      }
    }
  }

  private void snapshotSchedulerLoop() {
    // Staggered first fire: shard i fires initially at interval * (i + 1) / N so the N shards'
    // first snapshots spread across the first interval window instead of triggering simultaneously
    // (which would spike disk I/O and contend for the scan/io budget). Subsequent fires are at the
    // full interval cadence.
    long firstFireMs = Math.max(1L, SNAPSHOT_INTERVAL_MS * (index + 1) / shardCount);
    long nextSleepMs = firstFireMs;
    while (!stopping) {
      try {
        Thread.sleep(nextSleepMs);
      } catch (InterruptedException e) {
        if (stopping) break;
        Thread.interrupted();
        continue;
      }
      nextSleepMs = SNAPSHOT_INTERVAL_MS;
      requestSnapshot();
    }
  }

  private boolean snapshotIsOverdue() {
    return clock.millis() - lastSnapshotMs > OVERDUE_SNAPSHOT_FACTOR * SNAPSHOT_INTERVAL_MS;
  }

  // === MPSC drain ===

  private void drainMpsc() {
    staleEvents.drain(this::onStale, INSERTION_DRAIN_BATCH);
    insertionEvents.drain(this::onInsertion, INSERTION_DRAIN_BATCH);
    accessEvents.drain(this::onAccess, ACCESS_DRAIN_BATCH);
  }

  private void drainMpscUntilEmpty() {
    while (!staleEvents.isEmpty() || !insertionEvents.isEmpty() || !accessEvents.isEmpty()) {
      drainMpsc();
    }
  }

  private void onStale(Entry entry) {
    dequeuedCount.increment();
    if (!entry.tryEvict()) {
      return;
    }
    long victimDiskSize = backing.onDiskSize(entry);
    evictOneDischarged = false;
    try {
      evictOne(entry, victimDiskSize, /* expireEntryFallback= */ false);
    } catch (IOException e) {
      log.log(Level.WARNING, format("stale eviction of %s failed; will retry later", entry.key), e);
    } finally {
      if (evictOneDischarged) {
        signalParkers();
      }
    }
  }

  private void onInsertion(Entry entry) {
    dequeuedCount.increment();
    // Drain-side guard resolves three concurrent eviction races:
    //   - state != LIVE: a concurrent evictor took the entry between offer and drain
    //   - isLinked(): a concurrent path already linked it
    //   - refCount > 0: holder bumped the refcount; we'll see another offer when it releases
    if (entry.state() != Entry.State.LIVE || entry.isLinked() || entry.refCount() != 0) {
      return;
    }
    entry.addBefore(header);
    // Defensive re-check: all current state transitions are evictor-thread-owned after drain, but
    // keeping the post-link guard prevents a future state path from leaving a non-LIVE entry on
    // the LRU.
    if (entry.state() != Entry.State.LIVE) {
      entry.unlink();
      return;
    }
    unreferencedCount.increment();
  }

  private void onAccess(String key) {
    dequeuedCount.increment();
    Entry entry = backing.lookupEntry(key);
    if (entry == null || entry.state() != Entry.State.LIVE || entry.refCount() != 0) {
      return;
    }
    if (entry.isLinked()) {
      entry.unlink();
      // Defensive re-check after unlink. Leave the entry detached if a future state path makes it
      // non-LIVE here; the evictor sweep will clean up.
      if (entry.state() != Entry.State.LIVE) {
        unreferencedCount.decrement();
        return;
      }
      entry.addBefore(header);
    }
  }

  // === Sweep ===

  /** Returns the number of successful evictions this wake. */
  private int sweepWithBudget(long budgetNanos) throws IOException {
    long sweepStartNanos = nowNanos();
    long deadlineNanos = sweepStartNanos + budgetNanos;
    int evictionsThisWake = 0;
    int skipsBeforeEviction = 0;
    // bytesFreedThisWake is the signal gate: if dischargeNoSignal ran and then evictOne threw
    // before the matching evictionsThisWake++, we still owe parkers a wake. The two counters
    // diverge only on the throw-after-discharge path; under normal success they match.
    long bytesFreedThisWake = 0;
    try {
      Entry cursor = header.after;
      while (cursor != header
          && shardBytes.sum() > lowBytes
          && nowNanos() < deadlineNanos
          && !stopping) {
        Entry victim = cursor;
        cursor = cursor.after;
        if (!victim.tryEvict()) {
          stateRollbackCount.increment();
          stateRollbacksChild.inc();
          // tryEvict returns false in two cases:
          //   (1) CAS failed because state != LIVE — entry is EVICTING (off-thread evictor has
          //       it) or EVICTED (terminal); detach so we never re-walk it.
          //   (2) refCount > 0 triggered the LIVE->EVICTING->LIVE rollback — the entry is
          //       referenced again. Leave LIVE rollback victims on the LRU: a failed acquirer
          //       can still be about to undo its transient increment, and unlinking here can
          //       strand a LIVE refCount==0 entry off-LRU with no release event to re-offer it.
          if (victim.state() != Entry.State.LIVE && victim.isLinked()) {
            victim.unlink();
            unreferencedCount.decrement();
          }
          skipsBeforeEviction++;
          continue;
        }
        long victimDiskSize = backing.onDiskSize(victim);
        evictOneDischarged = false;
        try {
          evictOne(victim, victimDiskSize, /* expireEntryFallback= */ true);
        } finally {
          if (evictOneDischarged) {
            bytesFreedThisWake += victimDiskSize;
          }
        }
        evictionsThisWake++;
        skipPrefixLength.observe(skipsBeforeEviction);
        skipsBeforeEviction = 0;
      }
    } finally {
      // dischargeNoSignal in evictOne batches the post-sweep wakeup here. Run in finally so
      // a RuntimeException out of evictOne (onEntryEvicted / onDigestFullyExpired / etc.)
      // still wakes hard-cap-parked producers — without this they'd wait for the heartbeat
      // (idleHeartbeatNanos, default 2s) despite the discharge having freed their headroom.
      // The signal predicate is bytesFreedThisWake > 0 (not evictionsThisWake > 0) so a
      // throw between dischargeNoSignal and evictionsThisWake++ still fires the wake.
      if (evictionsThisWake > 0) {
        evictionsPerWake.observe(evictionsThisWake);
      }
      if (bytesFreedThisWake > 0) {
        lock.lock();
        try {
          cond.signalAll();
        } finally {
          lock.unlock();
        }
      }
      sweepDurationSeconds.observe((nowNanos() - sweepStartNanos) / 1e9);
    }
    return evictionsThisWake;
  }

  /**
   * Drive a single victim to EVICTED. Must be called only after {@link Entry#tryEvict} returned
   * true. {@code victimDiskSize} must be captured by the caller BEFORE any state mutation, in the
   * same units {@code charge} used (block-aligned on-disk bytes), so {@code discharge} matches
   * across the charge/discharge boundary — otherwise shardBytes drifts by (block - logical) per
   * eviction, eventually starving backpressure. The sweep also uses this value to credit
   * bytesFreedThisWake even when this method throws.
   */
  private void evictOne(Entry victim, long victimDiskSize, boolean expireEntryFallback)
      throws IOException {
    if (victim.isLinked()) {
      victim.unlink();
      unreferencedCount.decrement();
    }
    boolean completed = false;
    boolean detached = false;
    try {
      if (expireEntryFallback && !victim.key.endsWith("_dir")) {
        try {
          backing.expireEntryFallback(victim.key, victim.size);
        } catch (RuntimeException e) {
          expireEntryFallbackFailureTotal.labels(shardLabel, e.getClass().getSimpleName()).inc();
          log.log(
              Level.SEVERE, format("expireEntryFallback failed for %s; continuing", victim.key), e);
        }
      }
      // invalidateWriteForKey removes the write-future cache entry synchronously, while its
      // listener completion is dispatched off-thread. We MUST still run safeStorageRemoval if a
      // shutdown-race fallback throws — without this guard, a throw here leaves the file present,
      // storage map populated, and the in-memory Entry transitioned to EVICTED in the rollback
      // finally. Net: silent disk leak counted against shardBytes.
      try {
        backing.invalidateWriteForKey(victim.key, victim.size);
      } catch (RuntimeException e) {
        log.log(
            Level.SEVERE, format("invalidateWriteForKey failed for %s; continuing", victim.key), e);
      }
      Entry removedEntry = backing.safeStorageRemoval(victim.key);
      detached = true;
      if (removedEntry != null && removedEntry != victim) {
        // Defensive race-loss path: a different Entry replaced ours between offer and removal.
        // Restore the impostor (its own state machine continues) and discharge victim ourselves.
        log.log(
            Level.SEVERE,
            format(
                "evictOne: removed entry for %s differs from victim; restoring impostor",
                victim.key));
        backing.restoreEntryAfterRaceLoss(victim.key, removedEntry);
      }
      dischargeNoSignal(victimDiskSize);
      removedBytes.add(victimDiskSize);
      removedCount.increment();
      evictOneDischarged = true;
      // Subclass directory cleanup hook (LegacyDirectoryCFC drains its directoriesIndex
      // here). Runs synchronously on the evictor thread but the subclass implementation
      // dispatches the actual work to executors.
      backing.onEntryEvicted(victim);
      // Notify the cache of digest-level expiration before the async cleanup of _removed:
      // the storage check inside onDigestFullyExpired must observe the post-removal state
      // (this victim is gone from storage; only a different-variant entry under the same
      // digest might remain).
      if (!victim.key.endsWith("_dir")) {
        backing.onDigestFullyExpired(victim.key, victim.size);
      }
      submitAsyncCleanup(victim.key);
      victim.completeEviction();
      // Count the eviction only after the full success path. removedBytes/removedCount
      // bytes accounting is incremented above so shardBytes backpressure unblocks promptly,
      // but evictedTotal is the operator-facing "completed successfully" metric and should
      // skip evictions that threw mid-completion.
      evictedChild.inc();
      completed = true;
    } finally {
      if (!completed) {
        if (detached) {
          if (!evictOneDischarged) {
            dischargeNoSignal(victimDiskSize);
            removedBytes.add(victimDiskSize);
            removedCount.increment();
            evictOneDischarged = true;
          }
          try {
            victim.completeEviction();
          } catch (IllegalStateException ise) {
            // Already terminal; tolerate.
          }
        } else {
          rollbackEviction(victim);
        }
      }
    }
  }

  private void rollbackEviction(Entry victim) {
    try {
      victim.abortEviction();
    } catch (IllegalStateException ise) {
      log.log(Level.WARNING, format("could not roll back eviction state for %s", victim.key), ise);
      return;
    }
    if (victim.refCount() == 0 && !victim.isLinked()) {
      victim.addBefore(header);
      unreferencedCount.increment();
    }
  }

  private void submitAsyncCleanup(String key) {
    try {
      asyncCleanupExecutor.execute(
          () -> {
            try {
              backing.deleteExpiredKey(key);
            } catch (NoSuchFileException e) {
              // Already gone — fine.
            } catch (IOException e) {
              log.log(Level.WARNING, format("async cleanup for %s failed", key), e);
            }
          });
    } catch (RejectedExecutionException e) {
      // Shutdown race: run inline as a best-effort fallback. Match the dispatched path's
      // log discipline rather than silently swallowing — if the inline delete fails the
      // _removed sibling leaks, and operators want to see why.
      try {
        backing.deleteExpiredKey(key);
      } catch (NoSuchFileException ignored) {
        // Already gone — fine.
      } catch (IOException ioe) {
        log.log(Level.WARNING, format("inline async cleanup fallback for %s failed", key), ioe);
      }
    }
  }

  // === Snapshot ===

  /**
   * Write this shard's LRU to its snapshot file, rethrowing on failure. Used by the startup
   * migration path (CasShards) to persist the post-migration current-N snapshot before the stale-N
   * source files are deleted — the all-or-nothing crash-safety contract (see {@link
   * CasShards#migrateSnapshots} for the warmth-only durability semantics). Must be called only
   * while no evictor thread is running (startup scan), since it walks the LRU without holding the
   * single-writer invariant. Unlike {@link #performSnapshot}, propagates the IOException so the
   * migration can leave the stale files in place.
   */
  void writeSnapshotNow() throws IOException {
    if (failSnapshotWriteForTesting) {
      throw new IOException("injected snapshot write failure for testing");
    }
    lruDb.save(new LruEntryIterator(header), snapshotPath);
  }

  private void performSnapshot() {
    long startMs = clock.millis();
    try {
      // Walk LRU head -> tail. Sole writer of the LRU on this thread, so no synchronization.
      // Delegate the on-disk format to {@link LRUDB} so the same producer/consumer interface
      // covers both the legacy startup loader and this snapshot path.
      lruDb.save(new LruEntryIterator(header), snapshotPath);
      // Only update lastSnapshotMs and observe the duration histogram on success. A failed
      // snapshot must NOT reset the overdue clock — otherwise snapshotIsOverdue() returns
      // false for another 2*SNAPSHOT_INTERVAL_MS and we'd silently fall behind under
      // sustained pressure.
      long endMs = clock.millis();
      lastSnapshotMs = endMs;
      lruSnapshotDurationSeconds.observe((endMs - startMs) / 1000.0);
    } catch (IOException e) {
      lruSnapshotFailureTotal.labels(shardLabel, e.getClass().getSimpleName()).inc();
      log.log(Level.WARNING, "snapshot write failed", e);
    }
  }

  /** Single-pass iterator over the LRU head chain. The evictor thread owns the LRU. */
  private static final class LruEntryIterator implements Iterator<LRUDB.SizeEntry> {
    private final Entry header;
    private Entry cursor;

    LruEntryIterator(Entry header) {
      this.header = header;
      this.cursor = header.after;
    }

    @Override
    public boolean hasNext() {
      return cursor != header;
    }

    @Override
    public LRUDB.SizeEntry next() {
      if (cursor == header) {
        throw new NoSuchElementException();
      }
      LRUDB.SizeEntry entry = new LRUDB.SizeEntry(cursor.key, cursor.size);
      cursor = cursor.after;
      return entry;
    }
  }

  // === Test hooks ===
  //
  // Minimum necessary per cross-cutting principle 6. Add more only when a test actually
  // needs them.

  /**
   * Wait until the evictor parks AND the MPSC queues are empty AND the stability window has
   * elapsed. Returns true on success; false on timeout. Tests use this instead of {@code
   * shutdownAndAwaitTermination} so the cache remains usable post-wait.
   *
   * <p>The {@code atRest} predicate (no pending work/snapshot/stop flags, {@code shardBytes <=
   * lowBytes}, MPSC queues empty) plus the {@link #QUIESCENCE_STABILITY_MS} stability window is the
   * PRIMARY quiescence test. The {@code quiescedSequence} straddle read is a secondary guard only:
   * {@code quiescedSequence} is incremented just before the evictor parks, so reading it before and
   * after the predicate additionally rejects the narrow case where the evictor parked and re-parked
   * (ran a full loop iteration) across the two reads. It does NOT detect the evictor being
   * mid-drain or mid-sweep — {@code atRest} plus the stability window is what catches that.
   */
  @VisibleForTesting
  boolean awaitQuiescence(Duration timeout) throws InterruptedException {
    long deadlineMs = clock.millis() + timeout.toMillis();
    long stableSinceMs = -1;
    while (clock.millis() < deadlineMs) {
      long seqBefore = quiescedSequence;
      boolean atRest =
          !workPending
              && !snapshotPending
              && !stopping
              && shardBytes.sum() <= lowBytes
              && insertionEvents.isEmpty()
              && staleEvents.isEmpty()
              && accessEvents.isEmpty();
      long seqAfter = quiescedSequence;
      if (atRest && seqBefore == seqAfter) {
        if (stableSinceMs < 0) {
          stableSinceMs = clock.millis();
        }
        if (clock.millis() - stableSinceMs >= QUIESCENCE_STABILITY_MS) {
          return true;
        }
      } else {
        stableSinceMs = -1;
      }
      Thread.sleep(QUIESCENCE_POLL_INTERVAL_MS);
    }
    return false;
  }

  /**
   * When true, {@link #writeSnapshotNow} throws instead of writing. Lets the migration-failure test
   * exercise the all-or-nothing path (stale files retained when a shard's snapshot write fails)
   * without depending on filesystem-specific failure injection.
   */
  @VisibleForTesting volatile boolean failSnapshotWriteForTesting;

  @VisibleForTesting
  void setTotalBytesForTesting(long bytes) {
    shardBytes.reset();
    shardBytes.add(bytes);
  }
}
