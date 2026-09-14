// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker;

import build.buildfarm.common.config.BuildfarmConfigs;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.LongSupplier;

/** Aggregate metrics with bounded per-key state and expiration at observation and scrape time. */
final class PersistentWorkerObservationMetrics extends Collector {
  private static class Holder {
    static final PersistentWorkerObservationMetrics INSTANCE =
        new PersistentWorkerObservationMetrics(
            CollectorRegistry.defaultRegistry,
            System::nanoTime,
            BuildfarmConfigs.getInstance()
                .getWorker()
                .getPersistentWorkers()
                .getObservationWindowSeconds(),
            BuildfarmConfigs.getInstance()
                .getWorker()
                .getPersistentWorkers()
                .getObservationMaxKeys());
  }

  static PersistentWorkerObservationMetrics instance() {
    return Holder.INSTANCE;
  }

  private final LongSupplier clock;
  private final long windowNanos;
  private final int maxKeys;
  private final LinkedHashMap<String, Long> keys = new LinkedHashMap<>();
  private final Counter observations;
  private final Histogram arrivalGap;
  private final Histogram duration;
  private long inFlight;
  private Long lastOverflow;

  PersistentWorkerObservationMetrics(
      CollectorRegistry registry, LongSupplier clock, long windowSeconds, int maxKeys) {
    if (windowSeconds <= 0 || windowSeconds > Long.MAX_VALUE / 1_000_000_000L || maxKeys <= 0) {
      throw new IllegalArgumentException("Invalid observation tracking bounds");
    }
    this.clock = clock;
    this.windowNanos = windowSeconds * 1_000_000_000L;
    this.maxKeys = maxKeys;
    observations =
        Counter.build()
            .name("persistent_worker_observation_actions_total")
            .help("Sampled candidate actions by key tracking outcome.")
            .labelNames("outcome")
            .register(registry);
    for (String outcome : List.of("new_key", "seen_key", "capacity_exceeded", "key_error")) {
      observations.labels(outcome);
    }
    arrivalGap =
        Histogram.build()
            .name("persistent_worker_observation_key_arrival_gap_seconds")
            .help(
                "Time between sampled starts for the same retained key within the observation"
                    + " window; not idle time.")
            .buckets(0.01, 0.1, 1, 5, 10, 30, 60, 120, 300, 900, 3600)
            .register(registry);
    duration =
        Histogram.build()
            .name("persistent_worker_observation_native_seconds")
            .help(
                "Native attempt duration for sampled PW candidates, including preparation and"
                    + " cleanup.")
            .labelNames("outcome")
            .buckets(0.01, 0.1, 1, 5, 10, 30, 60, 120, 300, 900, 3600)
            .register(registry);
    register(registry);
  }

  synchronized void start(String key) {
    long now = clock.getAsLong();
    expire(now);
    inFlight++;
    if (key == null) {
      observations.labels("key_error").inc();
      return;
    }
    Long previous = keys.remove(key);
    if (previous != null) {
      keys.put(key, now);
      observations.labels("seen_key").inc();
      arrivalGap.observe((now - previous) / 1_000_000_000.0);
    } else if (keys.size() < maxKeys) {
      keys.put(key, now);
      observations.labels("new_key").inc();
    } else {
      lastOverflow = now;
      observations.labels("capacity_exceeded").inc();
    }
  }

  synchronized void finish(String status, int exitCode, double seconds) {
    inFlight--;
    String outcome =
        !status.equals("OK") ? "execution_error" : exitCode == 0 ? "success" : "action_failure";
    duration.labels(outcome).observe(Math.max(0, seconds));
  }

  private void expire(long now) {
    var iterator = keys.entrySet().iterator();
    while (iterator.hasNext()) {
      if (now - iterator.next().getValue() < windowNanos) {
        break;
      }
      iterator.remove();
    }
    if (lastOverflow != null && now - lastOverflow >= windowNanos) {
      lastOverflow = null;
    }
  }

  @Override
  public synchronized List<MetricFamilySamples> collect() {
    expire(clock.getAsLong());
    List<MetricFamilySamples> result = new ArrayList<>();
    result.add(
        new GaugeMetricFamily(
            "persistent_worker_observation_distinct_keys",
            "Distinct sampled keys retained in the rolling window; a lower bound when tracking is"
                + " incomplete.",
            keys.size()));
    result.add(
        new GaugeMetricFamily(
            "persistent_worker_observation_tracking_incomplete",
            "One if key capacity was exceeded within the rolling window, otherwise zero.",
            lastOverflow == null ? 0 : 1));
    result.add(
        new GaugeMetricFamily(
            "persistent_worker_observation_in_flight",
            "Sampled candidate native attempts currently in flight.",
            inFlight));
    result.add(
        new GaugeMetricFamily(
            "persistent_worker_observation_window_seconds",
            "Rolling key observation window.",
            windowNanos / 1_000_000_000.0));
    result.add(
        new GaugeMetricFamily(
            "persistent_worker_observation_key_capacity",
            "Maximum number of key fingerprints retained.",
            maxKeys));
    return result;
  }
}
