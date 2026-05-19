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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.jctools.queues.varhandle.MpscUnboundedVarHandleArrayQueue;
import org.jctools.queues.varhandle.MpscVarHandleArrayQueue;

/**
 * Single asynchronous evictor that owns the LRU list and totalBytes accounting, and serves as the
 * lock-free integration point between producer cache calls ({@code charge}, {@code recordAccess})
 * and the per-{@link Entry} atomic state machine.
 *
 * <h2>Wake protocol</h2>
 *
 * Three {@code volatile boolean} flags accessed under {@link #lock}: {@code workPending} (set by
 * producers crossing a wake threshold), {@code snapshotPending} (set by the snapshot scheduler),
 * {@code stopping} (set by {@link #stop}). Producers MUST NOT hold {@link #lock} while computing
 * what they signal. {@code workPending} is a wake-DEBOUNCE flag, not a work-presence flag — work
 * presence is carried by the MPSC queues and {@code totalBytes}, which the park predicate re-checks
 * directly. The consumer clears {@code workPending} only at the idle gate, after a drain+sweep has
 * established quiescence, and re-checks the predicate AFTER publishing {@code parked=true} (the
 * Dekker handshake on {@link #parked}) so a producer that set {@code workPending} and observed
 * {@code parked==false} (and thus skipped its signal) cannot be lost. Correctness rests on (1)
 * predicate re-check on every wake, (2) {@link #idleHeartbeatNanos} heartbeat self-heals lost
 * signals, (3) {@code totalBytes.sum() > lowBytes} is the ground-truth trigger.
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
 * The evictor thread walks the LRU and writes the snapshot file synchronously. Single-evictor
 * design (no sharding); a future sharded layout would benefit from pipelined SPSC handoff.
 */
@Log
final class Evictor {
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

  // Wake threshold offsets: wake when totalBytes crosses (parkBytes - 2%) so producers
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

  private static final Counter evictedTotal =
      Counter.build().name("evictor_evicted_total").help("Successful evictions.").register();
  private static final Counter stateRollbacksTotal =
      Counter.build()
          .name("evictor_state_rollbacks_total")
          .help("tryEvict EVICTING -> LIVE rollbacks.")
          .register();
  private static final Counter wakesTotal =
      Counter.build()
          .name("evictor_wakes_total")
          .labelNames("trigger")
          .help("Evictor wakeups by trigger.")
          .register();

  // Cached per-trigger children. Counter.labels(...) allocates a List on each call and
  // does a ConcurrentHashMap lookup; on the producer hot path we resolve the Child once
  // at class init and call inc() directly.
  private static final String TRIGGER_BYTE_THRESHOLD = "byte_threshold";
  private static final String TRIGGER_QUEUE_DEPTH = "queue_depth";
  private static final String TRIGGER_SNAPSHOT = "snapshot";
  private static final Counter.Child wakesByteThreshold = wakesTotal.labels(TRIGGER_BYTE_THRESHOLD);
  private static final Counter.Child wakesQueueDepth = wakesTotal.labels(TRIGGER_QUEUE_DEPTH);
  private static final Counter.Child wakesSnapshot = wakesTotal.labels(TRIGGER_SNAPSHOT);
  private static final Counter.Child mpscDropsAccess =
      Counter.build()
          .name("mpsc_drops_total")
          .labelNames("queue")
          .help("MPSC queue offer-full drops.")
          .register()
          .labels("access");
  private static final Counter expireEntryFallbackFailureTotal =
      Counter.build()
          .name("expire_entry_fallback_failure_total")
          .labelNames("reason")
          .help("Delegate-fallback failures during eviction, labeled by exception simple name.")
          .register();
  private static final Counter restartsTotal =
      Counter.build()
          .name("evictor_restarts_total")
          .labelNames("reason")
          .help("Evictor loop body restarts.")
          .register();
  private static final Counter lruSnapshotFailureTotal =
      Counter.build()
          .name("lru_snapshot_failure_total")
          .labelNames("error_class")
          .help("Snapshot write failures.")
          .register();

  private static final Gauge queueDepthGauge =
      Gauge.build()
          .name("evictor_queue_depth")
          .help("Approximate insertion and stale-entry queue depth.")
          .register();
  private static final AtomicLong heartbeatLastAdvancedNanos = new AtomicLong(System.nanoTime());

  @SuppressWarnings("unused")
  private static final Gauge heartbeatAgeGauge =
      Gauge.build()
          .name("evictor_heartbeat_age_seconds")
          .help("Seconds since evictor loop body last advanced.")
          .create()
          .setChild(
              new Gauge.Child() {
                @Override
                public double get() {
                  return Math.max(0, System.nanoTime() - heartbeatLastAdvancedNanos.get()) / 1e9;
                }
              })
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
  private final ExecutorService asyncCleanupExecutor;
  private final Clock clock;
  private final long parkBytes;
  private final long wakeBytes;
  private final long lowBytes;
  private final long wakeBudgetNanos;
  private final long idleHeartbeatNanos;
  private final LRUDB lruDb;
  private final Path snapshotPath;

  // === Concurrency state ===

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition cond = lock.newCondition();

  private volatile boolean workPending;
  private volatile boolean snapshotPending;
  private volatile boolean stopping;

  // True while the evictor thread is parked on {@link #cond}. Producers check this BEFORE
  // taking the lock so a steady-state {@link #requestDrain} on a running evictor is a
  // single volatile read instead of a full mutex acquire. Written by the evictor inside the
  // park block; read by producers without the lock. The wait loop re-checks the predicate
  // AFTER publishing parked=true so the JMM total order on volatile reads/writes pairs the
  // producer's (workPending=true; read parked) sequence with our (parked=true; re-read
  // workPending) sequence — if the producer's parked-read sees false (so it skipped the
  // signal), our re-read MUST see workPending=true. Without the re-check, both reads can
  // observe stale values in a legal SC interleaving and the evictor sleeps until the
  // heartbeat self-heals (idleHeartbeatNanos, default 2s) — a latency spike, not a
  // correctness bug, but unacceptable when the signal exists to unblock cap pressure.
  private volatile boolean parked;

  // Published by the evictor under {@link #lock} on each pre-await; readers (tests calling
  // {@link #awaitQuiescence}) read the volatile value without the lock. Single-writer +
  // volatile gives the happens-before edge tests need.
  private volatile long quiescedSequence;

  private volatile long lastWakeNanos;
  private volatile long lastSnapshotMs;

  // === Hot accounting ===

  private final LongAdder totalBytes = new LongAdder();
  private final LongAdder unreferencedCount = new LongAdder();
  private final LongAdder removedBytes = new LongAdder();
  private final LongAdder removedCount = new LongAdder();
  private final LongAdder dequeuedCount = new LongAdder();
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

  Evictor(
      CasEvictorBacking backing,
      ExecutorService asyncCleanupExecutor,
      Clock clock,
      Entry header,
      long parkBytes,
      long lowBytes,
      long wakeBudgetNanos,
      long idleHeartbeatNanos,
      LRUDB lruDb,
      Path snapshotPath) {
    checkArgument(parkBytes > 0, "parkBytes must be positive");
    checkArgument(lowBytes >= 0 && lowBytes <= parkBytes, "lowBytes must be in [0, parkBytes]");
    checkArgument(wakeBudgetNanos > 0, "wakeBudgetNanos must be positive");
    checkArgument(idleHeartbeatNanos > 0, "idleHeartbeatNanos must be positive");
    this.backing = backing;
    this.asyncCleanupExecutor = asyncCleanupExecutor;
    this.clock = clock;
    this.header = header;
    this.parkBytes = parkBytes;
    this.lowBytes = lowBytes;
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
      lastSnapshotMs = clock.millis();
      stopping = false;
      evictorThread = new Thread(this::loop, "buildfarm-cas-evictor");
      evictorThread.setDaemon(true);
      evictorThread.start();
      snapshotSchedulerThread =
          new Thread(this::snapshotSchedulerLoop, "buildfarm-cas-snapshot-scheduler");
      snapshotSchedulerThread.setDaemon(true);
      snapshotSchedulerThread.start();
    } finally {
      lock.unlock();
    }
  }

  void stop() throws InterruptedException {
    Thread evictor;
    Thread scheduler;
    lock.lock();
    try {
      if (evictorThread == null) {
        return;
      }
      stopping = true;
      snapshotPending = true; // shutdown snapshot
      cond.signalAll();
      evictor = evictorThread;
      scheduler = snapshotSchedulerThread;
    } finally {
      lock.unlock();
    }
    if (scheduler != null) {
      scheduler.interrupt();
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
    totalBytes.add(bytes);
    long current = totalBytes.sum();
    // Wake whenever totalBytes is above the wake watermark; requestDrain's workPending guard
    // debounces redundant signals so steady-state above-watermark charges cost one volatile
    // read each. An earlier "edge-triggered" form computed (current - bytes) and only fired
    // on the producer that observed the crossing, but two concurrent producers each adding
    // less than the wake-margin could both read totalBytes.sum() after both adds had landed
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
   * Add startup-time bytes to {@link #totalBytes} without the wake/park machinery. Used by the
   * cache's {@code scanRoot} when seeding totalBytes from on-disk files; safe whether or not the
   * evictor thread has been started, because the additions are accumulating LongAdder cells and
   * don't touch the LRU.
   */
  void addBytesAtStartup(long bytes) {
    totalBytes.add(bytes);
  }

  /**
   * Account a newly LRU-linked entry at startup. Increments the unreferenced count so the
   * post-start metrics agree with the LRU walk. Callers must hold whatever lock the cache uses to
   * serialize concurrent {@link Entry#addBefore} on {@link #header} with the evictor's drain — in
   * practice this is the {@code synchronized(header)} block inside {@code processRootFile}.
   */
  void noteUnreferencedAtStartup() {
    unreferencedCount.increment();
  }

  /** Reverse a prior {@link #charge} reservation. */
  void discharge(long bytes) {
    totalBytes.add(-bytes);
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
    totalBytes.add(-bytes);
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
        mpscDropsAccess.inc();
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
      wakesTotal.labels(trigger).inc();
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

  long currentTotalBytes() {
    return totalBytes.sum();
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
      totalBytes.add(-bytesReserved);
      throw ie;
    }
    try {
      while (totalBytes.sum() > parkBytes && !stopping) {
        long remainingNanos = deadlineNanos - nowNanos();
        if (remainingNanos <= 0) {
          // Roll back AND wake sibling parkers: the freed bytes are headroom they may need.
          // Without this, a parker timing out leaves the next one to wait for the heartbeat.
          totalBytes.add(-bytesReserved);
          cond.signalAll();
          throw new IOException(
              "cas charge backpressure: parked "
                  + HARD_CAP_PARK_TIMEOUT_SECONDS
                  + "s at hard cap; try another worker");
        }
        cond.await(remainingNanos, NANOSECONDS);
      }
      if (stopping && totalBytes.sum() > parkBytes) {
        // stop() set stopping and signalAll-woke us; the evictor will not run more sweeps so
        // we cannot expect totalBytes to drop. Roll back the reservation and surface a typed
        // failure so the caller exits promptly instead of waiting the full hard-cap timeout.
        totalBytes.add(-bytesReserved);
        cond.signalAll();
        throw new IOException("cas charge backpressure: evictor stopped while parked");
      }
    } catch (InterruptedException ie) {
      totalBytes.add(-bytesReserved);
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
      heartbeatLastAdvancedNanos.set(System.nanoTime());
      try {
        long dequeuedBefore = dequeuedCount.sum();
        drainMpsc();
        long dequeuedAfter = dequeuedCount.sum();
        queueDepthGauge.set(insertionEvents.size() + staleEvents.size());
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
        if (totalBytes.sum() > lowBytes) {
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
        // states: (a) genuinely idle (totalBytes <= lowBytes), or (b) stuck above LOW with
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
              && (totalBytes.sum() <= lowBytes || stuckAboveLow)) {
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
                  || (totalBytes.sum() > lowBytes && !stuckAboveLow)) {
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
        restartsTotal.labels(reason).inc();
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
    while (!stopping) {
      try {
        Thread.sleep(SNAPSHOT_INTERVAL_MS);
      } catch (InterruptedException e) {
        if (stopping) break;
        Thread.interrupted();
        continue;
      }
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
          && totalBytes.sum() > lowBytes
          && nowNanos() < deadlineNanos
          && !stopping) {
        Entry victim = cursor;
        cursor = cursor.after;
        if (!victim.tryEvict()) {
          stateRollbackCount.increment();
          stateRollbacksTotal.inc();
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
   * across the charge/discharge boundary — otherwise totalBytes drifts by (block - logical) per
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
          expireEntryFallbackFailureTotal.labels(e.getClass().getSimpleName()).inc();
          log.log(
              Level.SEVERE, format("expireEntryFallback failed for %s; continuing", victim.key), e);
        }
      }
      // invalidateWriteForKey removes the write-future cache entry synchronously, while its
      // listener completion is dispatched off-thread. We MUST still run safeStorageRemoval if a
      // shutdown-race fallback throws — without this guard, a throw here leaves the file present,
      // storage map populated, and the in-memory Entry transitioned to EVICTED in the rollback
      // finally. Net: silent disk leak counted against totalBytes.
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
      // bytes accounting is incremented above so totalBytes backpressure unblocks promptly,
      // but evictedTotal is the operator-facing "completed successfully" metric and should
      // skip evictions that threw mid-completion.
      evictedTotal.inc();
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
      lruSnapshotFailureTotal.labels(e.getClass().getSimpleName()).inc();
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
   * <p>The {@code atRest} predicate (no pending work/snapshot/stop flags, {@code totalBytes <=
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
              && totalBytes.sum() <= lowBytes
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

  @VisibleForTesting
  void setTotalBytesForTesting(long bytes) {
    totalBytes.reset();
    totalBytes.add(bytes);
  }
}
