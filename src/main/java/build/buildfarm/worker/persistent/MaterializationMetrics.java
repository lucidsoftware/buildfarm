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

package build.buildfarm.worker.persistent;

import io.prometheus.client.Histogram;

/**
 * Shared registration of {@code materialize_exec_root_seconds} (plan §0). The histogram is written
 * from BOTH the worker package ({@code CFCExecFileSystem}/{@code CFCLinkExecFileSystem}, {@code
 * kind=regular}) and this persistent package ({@code ProtoCoordinator}, the {@code
 * persistent_worker}/{@code persistent_worker_incremental} kinds), so the registration cannot live
 * in either consumer: a Prometheus metric name may be registered only once, and {@code worker}
 * depends on {@code persistent} (not the reverse), so a worker-package field is unreachable from
 * here. This neutral holder is reachable from both. The {@code kind} label is what makes the PW
 * paths directly comparable to the regular path on one series.
 */
public final class MaterializationMetrics {
  private MaterializationMetrics() {}

  public static final String KIND_REGULAR = "regular";
  public static final String KIND_PERSISTENT_WORKER = "persistent_worker";
  public static final String KIND_PERSISTENT_WORKER_INCREMENTAL = "persistent_worker_incremental";

  public static final Histogram MATERIALIZE_EXEC_ROOT_SECONDS =
      Histogram.build()
          .name("materialize_exec_root_seconds")
          .labelNames("kind")
          .buckets(0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30)
          .help("Wall time to materialize an action exec root, by materialization kind.")
          .register();
}
