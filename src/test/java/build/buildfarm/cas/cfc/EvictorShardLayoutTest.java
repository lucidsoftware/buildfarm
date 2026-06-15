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
import static com.google.common.truth.Truth.assertWithMessage;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldLayout;

/**
 * Regression test for the {@link EvictorShard} cache-line padding (Phase 2.2). The shard's hot
 * mutable fields are sandwiched between two 120-byte pad regions declared in superclasses so that
 * no two shard objects share a cache line on their hot fields. This test pins that layout against
 * future field-addition or JDK-layout-heuristic regressions that would silently undo the padding.
 *
 * <p>JOL imports {@code sun.misc.Unsafe} to inspect object layout, so it is confined to this test
 * target — no production code path loads it. The project builds and runs tests on JDK 21 (see
 * {@code .bazelrc}); on a newer JDK any JEP 471 sun.misc.Unsafe warnings here are cosmetic.
 */
@RunWith(JUnit4.class)
public class EvictorShardLayoutTest {
  // Every field declared in EvictorShardHotRefs. The whole block must land between the pads, so a
  // future reorder that moves any one of them out (e.g. lock drifting past the trailing pad) trips
  // this test, not just a move of shardBytes.
  private static final java.util.Set<String> HOT_FIELDS =
      java.util.Set.of(
          "shardBytes",
          "dequeuedCount",
          "lock",
          "cond",
          "workPending",
          "snapshotPending",
          "stopping",
          "parked",
          "quiescedSequence",
          "lastWakeNanos",
          "heartbeatWallNanos");

  @Test
  public void evictorShardLayoutShouldMatchPaddingExpectations() {
    ClassLayout layout = ClassLayout.parseClass(EvictorShard.class);

    int hotFieldsSeen = 0;
    long minHotOffset = Long.MAX_VALUE;
    long maxHotOffset = -1;
    long lastLeadingPadOffset = -1;
    long firstTrailingPadOffset = Long.MAX_VALUE;
    int leadingPadFieldsSeen = 0;
    int trailingPadFieldsSeen = 0;
    // Cold/foreign fields (anything that is neither a hot field nor a p###/q### pad byte).
    // Collected
    // so we can assert below that none of them landed INSIDE the hot block.
    java.util.List<FieldLayout> otherFields = new java.util.ArrayList<>();
    for (FieldLayout field : layout.fields()) {
      String name = field.name();
      if (HOT_FIELDS.contains(name)) {
        hotFieldsSeen++;
        minHotOffset = Math.min(minHotOffset, field.offset());
        maxHotOffset = Math.max(maxHotOffset, field.offset());
      } else if (name.matches("p\\d{3}")) { // leading pad p000..p119
        leadingPadFieldsSeen++;
        lastLeadingPadOffset = Math.max(lastLeadingPadOffset, field.offset());
      } else if (name.matches("q\\d{3}")) { // trailing pad q000..q119
        trailingPadFieldsSeen++;
        firstTrailingPadOffset = Math.min(firstTrailingPadOffset, field.offset());
      } else {
        otherFields.add(field);
      }
    }

    assertWithMessage("all EvictorShardHotRefs fields present in layout")
        .that(hotFieldsSeen)
        .isEqualTo(HOT_FIELDS.size());
    assertWithMessage("all leading pad bytes present").that(leadingPadFieldsSeen).isEqualTo(120);
    assertWithMessage("all trailing pad bytes present").that(trailingPadFieldsSeen).isEqualTo(120);

    // The ENTIRE hot block must sit AFTER the leading pad (isolated from the object header / a
    // neighbor's trailing fields) and BEFORE the trailing pad (isolated from this object's cold
    // fields), so no hot field shares a cache line with another shard object's fields.
    assertWithMessage("hot block (min offset %s) must follow the leading pad", minHotOffset)
        .that(minHotOffset)
        .isGreaterThan(lastLeadingPadOffset);
    assertWithMessage("trailing pad must follow the hot block (max offset %s)", maxHotOffset)
        .that(firstTrailingPadOffset)
        .isGreaterThan(maxHotOffset);
    // At least a cache line of padding precedes the hot block.
    assertThat(minHotOffset).isAtLeast(120L);
    // No cold/foreign field may be packed INSIDE the hot block. HotSpot can fill a superclass's
    // alignment gaps with a subclass's small fields, so a cold EvictorShard field (or a future
    // addition) could land between minHotOffset and maxHotOffset and share a cache line with the
    // hot
    // scalars while the bracket checks above still pass. This catches that interloper case.
    for (FieldLayout field : otherFields) {
      assertWithMessage(
              "field %s at offset %s must lie outside the hot block [%s, %s]",
              field.name(), field.offset(), minHotOffset, maxHotOffset)
          .that(field.offset() < minHotOffset || field.offset() > maxHotOffset)
          .isTrue();
    }
  }
}
