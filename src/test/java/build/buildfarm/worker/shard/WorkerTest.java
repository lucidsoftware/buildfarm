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

package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.config.Cas;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkerTest {
  // ===== auto-default (lowWatermarkPercent == 0) =====

  @Test
  public void computeLowBytes_bytesModeAuto_returns80PercentOfCap() {
    // bytes-mode (maxSizePercent == 0) with default lowWatermarkPercent picks 80% of the
    // explicit byte cap.
    Cas cas = new Cas();
    cas.setMaxSizeBytes(10_000L);

    assertThat(Worker.computeLowBytes(cas, 10_000L)).isEqualTo(8_000L);
  }

  @Test
  public void computeLowBytes_percentModeAuto_returns80PercentOfCap() {
    // percent-mode (maxSizePercent > 0) with default lowWatermarkPercent picks 80% of the
    // (already-resolved) byte cap. The unified default makes the auto behavior monotonic
    // across modes.
    Cas cas = new Cas();
    cas.setMaxSizePercent(50);

    assertThat(Worker.computeLowBytes(cas, 50_000L)).isEqualTo(40_000L);
  }

  @Test
  public void computeLowBytes_percentModeAutoSmallPercent_returns80PercentOfCap() {
    // Regression: percent-mode with maxSizePercent <= 10 previously collapsed derivedPercent
    // to 0 and the function returned 0 (sentinel) — CASFileCache would then fall back to its
    // own 80%-of-cap default. Now Worker computes 80% directly so the contract is consistent
    // across all valid maxSizePercent values.
    Cas cas = new Cas();
    cas.setMaxSizePercent(5);

    assertThat(Worker.computeLowBytes(cas, 10_000L)).isEqualTo(8_000L);
  }

  @Test
  public void computeLowBytes_percentModeAutoBoundary10_returns80PercentOfCap() {
    // maxSizePercent == 10 was the exact collapse point of the old (p - 10) / p formula
    // (0/10 == 0). Pin the boundary explicitly so a future refactor that reintroduces a
    // similar non-monotonic formula trips a focused test failure here, not just at the
    // p == 5 regression case.
    Cas cas = new Cas();
    cas.setMaxSizePercent(10);

    assertThat(Worker.computeLowBytes(cas, 10_000L)).isEqualTo(8_000L);
  }

  @Test
  public void computeLowBytes_percentModeAuto_isMonotonicAcrossPercents() {
    // Auto-default invariant: for a fixed resolved byte cap, lowBytes must not depend on
    // maxSizePercent — every percent picks 80% of the same cap. This catches any
    // reintroduction of a maxSizePercent-coupled formula at a value the explicit regression
    // tests don't cover.
    long resolvedCap = 100_000L;
    long expectedLow = 80_000L;
    for (int percent = 1; percent <= 100; percent++) {
      Cas cas = new Cas();
      cas.setMaxSizePercent(percent);
      assertThat(Worker.computeLowBytes(cas, resolvedCap)).isEqualTo(expectedLow);
    }
  }

  // ===== explicit lowWatermarkPercent =====

  @Test
  public void computeLowBytes_bytesModeExplicit_returnsPercentOfCap() {
    // bytes-mode (maxSizePercent == 0): lowWatermarkPercent is a percent of the cap itself.
    Cas cas = new Cas();
    cas.setMaxSizeBytes(10_000L);
    cas.setLowWatermarkPercent(70);

    assertThat(Worker.computeLowBytes(cas, 10_000L)).isEqualTo(7_000L);
  }

  @Test
  public void computeLowBytes_percentModeExplicit_returnsProportionalShare() {
    // percent-mode: lowWatermarkPercent and maxSizePercent share the filesystem-total base,
    // so lowBytes is lowWatermarkPercent / maxSizePercent of the resolved byte cap.
    Cas cas = new Cas();
    cas.setMaxSizePercent(50);
    cas.setLowWatermarkPercent(40);

    assertThat(Worker.computeLowBytes(cas, 50_000L)).isEqualTo(40_000L);
  }

  @Test
  public void computeLowBytes_bytesModeExplicitUpperBound_returnsAlmostFullCap() {
    // Validator caps lowWatermarkPercent at 99 in bytes-mode; this case is the documented
    // upper boundary.
    Cas cas = new Cas();
    cas.setMaxSizeBytes(10_000L);
    cas.setLowWatermarkPercent(99);

    assertThat(Worker.computeLowBytes(cas, 10_000L)).isEqualTo(9_900L);
  }

  // ===== computeEvictorShards (explicit value clamping) =====

  @Test
  public void computeEvictorShards_explicitNonPowerOfTwo_clampsDownToPowerOfTwo() {
    // A non-power-of-two value is clamped down rather than crashing the constructor.
    Cas cas = new Cas();
    cas.setEvictorShards(6);
    assertThat(
            Worker.computeEvictorShards(
                cas, /* maxEntrySizeInBytes= */ 1024L, /* maxSizeBytes= */ 1L << 20))
        .isEqualTo(4);
  }

  @Test
  public void computeEvictorShards_explicitValidPowerOfTwo_isHonored() {
    // A valid power-of-two within the entry-size bound passes through unchanged.
    Cas cas = new Cas();
    cas.setEvictorShards(8);
    assertThat(Worker.computeEvictorShards(cas, /* maxEntrySizeInBytes= */ 1024L, 1L << 20))
        .isEqualTo(8);
  }

  @Test
  public void computeEvictorShards_explicitTooLargeForEntrySize_clampsToEntryBound() {
    // maxSizeBytes / maxEntrySizeBytes == 4, so at most 4 shards can each hold a max-size entry.
    // An explicit 64 exceeds that and would crash the constructor; it is clamped to 4.
    Cas cas = new Cas();
    cas.setEvictorShards(64);
    assertThat(
            Worker.computeEvictorShards(
                cas, /* maxEntrySizeInBytes= */ 1L << 18, /* maxSizeBytes= */ 1L << 20))
        .isEqualTo(4);
  }

  @Test
  public void computeEvictorShards_explicitOne_isHonored() {
    Cas cas = new Cas();
    cas.setEvictorShards(1);
    assertThat(Worker.computeEvictorShards(cas, /* maxEntrySizeInBytes= */ 1024L, 1L << 20))
        .isEqualTo(1);
  }
}
