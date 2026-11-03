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

package build.buildfarm.worker;

import io.prometheus.client.Counter;
import java.util.Map;

/** Accounts for measured CPU usage of a native action, excluding unavailable samples. */
final class ActionCpuTime {
  private static final String CPU_USAGE_USEC = "cpu.usage_usec";
  private static final Counter cpuSeconds =
      Counter.build()
          .name("action_cpu_seconds_total")
          .labelNames("mnemonic")
          .help("Measured CPU seconds consumed by native actions with CPU accounting available.")
          .register();

  private final String mnemonic;
  private final Long startUsecs;

  ActionCpuTime(String mnemonic, Map<String, Long> startSample) {
    this.mnemonic = mnemonic.isEmpty() ? "unknown" : mnemonic;
    startUsecs = startSample.get(CPU_USAGE_USEC);
  }

  void record(Map<String, Long> endSample) {
    Long endUsecs = endSample.get(CPU_USAGE_USEC);
    // Missing accounting is not zero usage. Also exclude invalid or reset counters.
    if (startUsecs != null && endUsecs != null && startUsecs >= 0 && endUsecs >= startUsecs) {
      cpuSeconds.labels(mnemonic).inc((endUsecs - startUsecs) / 1_000_000.0);
    }
  }
}
