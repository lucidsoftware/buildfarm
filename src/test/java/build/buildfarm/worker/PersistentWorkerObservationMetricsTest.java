// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;

import io.prometheus.client.CollectorRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class PersistentWorkerObservationMetricsTest {
  @Test
  public void repeatedKeysRefreshTheirWindowAndScrapesExpireWithoutTraffic() {
    CollectorRegistry registry = new CollectorRegistry();
    AtomicLong now = new AtomicLong();
    var metrics = new PersistentWorkerObservationMetrics(registry, now::get, 10, 10);
    metrics.start("a");
    metrics.start("b");
    now.set(5_000_000_000L);
    metrics.start("a");
    assertThat(registry.getSampleValue("persistent_worker_observation_distinct_keys"))
        .isEqualTo(2.0);
    assertThat(
            registry.getSampleValue(
                "persistent_worker_observation_actions_total",
                new String[] {"outcome"},
                new String[] {"seen_key"}))
        .isEqualTo(1.0);
    assertThat(registry.getSampleValue("persistent_worker_observation_key_arrival_gap_seconds_sum"))
        .isEqualTo(5.0);
    now.set(10_000_000_000L);
    assertThat(registry.getSampleValue("persistent_worker_observation_distinct_keys"))
        .isEqualTo(1.0);
    now.set(15_000_000_000L);
    assertThat(registry.getSampleValue("persistent_worker_observation_distinct_keys"))
        .isEqualTo(0.0);
  }

  @Test
  public void overflowIsExplicitAndStateIsBounded() {
    CollectorRegistry registry = new CollectorRegistry();
    AtomicLong now = new AtomicLong();
    var metrics = new PersistentWorkerObservationMetrics(registry, now::get, 10, 2);
    metrics.start("a");
    metrics.start("b");
    metrics.start("c");
    assertThat(registry.getSampleValue("persistent_worker_observation_distinct_keys"))
        .isEqualTo(2.0);
    assertThat(registry.getSampleValue("persistent_worker_observation_tracking_incomplete"))
        .isEqualTo(1.0);
    assertThat(
            registry.getSampleValue(
                "persistent_worker_observation_actions_total",
                new String[] {"outcome"},
                new String[] {"capacity_exceeded"}))
        .isEqualTo(1.0);
    now.set(10_000_000_000L);
    assertThat(registry.getSampleValue("persistent_worker_observation_tracking_incomplete"))
        .isEqualTo(0.0);
    metrics.start("c");
    assertThat(registry.getSampleValue("persistent_worker_observation_distinct_keys"))
        .isEqualTo(1.0);
  }

  @Test
  public void failedKeyDoesNotCountAsDistinctAndCompletionTracksResults() {
    CollectorRegistry registry = new CollectorRegistry();
    var metrics = new PersistentWorkerObservationMetrics(registry, () -> 0L, 10, 2);
    metrics.start(null);
    assertThat(registry.getSampleValue("persistent_worker_observation_distinct_keys"))
        .isEqualTo(0.0);
    assertThat(registry.getSampleValue("persistent_worker_observation_in_flight")).isEqualTo(1.0);
    metrics.finish("OK", 1, 2.5);
    assertThat(registry.getSampleValue("persistent_worker_observation_in_flight")).isEqualTo(0.0);
    assertThat(
            registry.getSampleValue(
                "persistent_worker_observation_native_seconds_sum",
                new String[] {"outcome"},
                new String[] {"action_failure"}))
        .isEqualTo(2.5);
  }
}
