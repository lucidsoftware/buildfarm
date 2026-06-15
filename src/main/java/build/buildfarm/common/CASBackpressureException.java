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

package build.buildfarm.common;

import static java.lang.String.format;

import io.grpc.Status;
import java.io.IOException;
import org.jspecify.annotations.Nullable;

/**
 * Thrown by the CAS when a {@code charge} cannot make progress because of backpressure: either a
 * shard's evictor could not free space within the hard-cap park timeout ({@link Reason#HARD_CAP}),
 * or the shard's evictor thread terminated and the shard fails writes fast until redeploy ({@link
 * Reason#SHARD_DEAD}). It carries the structured diagnostic fields an operator needs to triage the
 * condition, and maps itself to the gRPC-canonical status code via {@link #toStatus}.
 *
 * <p>The two reasons map to distinct gRPC codes deliberately. {@code RESOURCE_EXHAUSTED} is "the
 * workload exceeds capacity but the infrastructure is healthy — try elsewhere"; {@code UNAVAILABLE}
 * is "a server-side component is down, retry-with-backoff may recover after operator action." Both
 * are retryable under Bazel's default policy, so the action retries on another worker either way;
 * the code distinction tells operators which kind of problem they are looking at without custom
 * retry tuning.
 *
 * <p>Lives in {@code build.buildfarm.common} (next to {@link EntryLimitException}) so both the CAS
 * implementation that throws it and the gRPC service layer that maps it can reach it. The gRPC
 * entry points detect it anywhere in a failed future's cause chain via {@link #findInCauseChain}.
 */
public class CASBackpressureException extends IOException {
  /** Why the charge was rejected; selects the gRPC status code and the message form. */
  public enum Reason {
    /** A charger parked at the per-shard hard cap until the park timeout fired. */
    HARD_CAP,
    /** The shard's evictor terminated (uncaught {@code Error}); {@code charge} fails fast. */
    SHARD_DEAD;

    // Note: the plan also documents a `queue_full` reason for a bounded insertion queue, but the
    // shipped evictor uses an unbounded insertion/stale queue (insertions are must-not-lose and
    // never drop), so no charge ever fails on queue capacity. The reason is intentionally omitted
    // rather than declared and never thrown.
  }

  private final Reason reason;
  private final int shardIndex;
  private final long shardBytes;
  private final long shardCap;
  private final long parkSeconds;
  private final double recentEvictionsPerSec;

  public CASBackpressureException(
      Reason reason,
      int shardIndex,
      long shardBytes,
      long shardCap,
      long parkSeconds,
      double recentEvictionsPerSec) {
    super(describe(reason, shardIndex, shardBytes, shardCap, parkSeconds, recentEvictionsPerSec));
    this.reason = reason;
    this.shardIndex = shardIndex;
    this.shardBytes = shardBytes;
    this.shardCap = shardCap;
    this.parkSeconds = parkSeconds;
    this.recentEvictionsPerSec = recentEvictionsPerSec;
  }

  public Reason getReason() {
    return reason;
  }

  public int getShardIndex() {
    return shardIndex;
  }

  public long getShardBytes() {
    return shardBytes;
  }

  public long getShardCap() {
    return shardCap;
  }

  public long getParkSeconds() {
    return parkSeconds;
  }

  public double getRecentEvictionsPerSec() {
    return recentEvictionsPerSec;
  }

  /**
   * Map to the gRPC status the service layer surfaces to the client. {@link Reason#HARD_CAP} (and
   * any capacity-exhausted reason) becomes {@code RESOURCE_EXHAUSTED}; {@link Reason#SHARD_DEAD}
   * becomes {@code UNAVAILABLE}. The description is this exception's message; the cause is retained
   * for server-side logging.
   */
  public Status toStatus() {
    Status base = reason == Reason.SHARD_DEAD ? Status.UNAVAILABLE : Status.RESOURCE_EXHAUSTED;
    return base.withDescription(getMessage()).withCause(this);
  }

  /**
   * Walk {@code t}'s cause chain and return the first {@link CASBackpressureException}, or null if
   * none is present. A failed write future surfaces this exception either directly (the {@code
   * FutureCallback.onFailure} path) or wrapped inside a {@code Status.UNKNOWN} exception (the
   * synchronous {@code catch} path that pre-maps via {@code Status.fromThrowable}); walking the
   * cause chain detects it in both cases.
   */
  public static @Nullable CASBackpressureException findInCauseChain(@Nullable Throwable t) {
    // Cause chains are shallow in practice; cap the walk so a pathological cycle (constructible by
    // Throwable.initCause forming a loop) terminates rather than spins.
    Throwable cause = t;
    for (int depth = 0;
        cause != null && depth < MAX_CAUSE_DEPTH;
        depth++, cause = cause.getCause()) {
      if (cause instanceof CASBackpressureException) {
        return (CASBackpressureException) cause;
      }
    }
    return null;
  }

  private static final int MAX_CAUSE_DEPTH = 64;

  private static String describe(
      Reason reason,
      int shardIndex,
      long shardBytes,
      long shardCap,
      long parkSeconds,
      double recentEvictionsPerSec) {
    if (reason == Reason.SHARD_DEAD) {
      return format("cas shard %d unavailable: evictor terminated; redeploy required", shardIndex);
    }
    return format(
        "cas charge backpressure: shard=%d parkSeconds=%d shardBytes=%s shardCap=%s"
            + " evictionRate=%s/s; try another worker",
        shardIndex,
        parkSeconds,
        humanBytes(shardBytes),
        humanBytes(shardCap),
        formatRate(recentEvictionsPerSec));
  }

  private static final String[] BYTE_UNITS = {"KB", "MB", "GB", "TB", "PB"};

  /** SI (1000-based) human-readable byte size, e.g. {@code 215.4GB}. */
  private static String humanBytes(long bytes) {
    if (bytes < 1000) {
      return bytes + "B";
    }
    double value = bytes;
    int unit = -1;
    do {
      value /= 1000.0;
      unit++;
    } while (value >= 1000.0 && unit < BYTE_UNITS.length - 1);
    return format("%.1f%s", value, BYTE_UNITS[unit]);
  }

  /** Whole rates render without a decimal ({@code 0/s}); fractional rates keep one place. */
  private static String formatRate(double rate) {
    if (!Double.isInfinite(rate) && !Double.isNaN(rate) && rate == Math.rint(rate)) {
      return Long.toString((long) rate);
    }
    return format("%.1f", rate);
  }
}
