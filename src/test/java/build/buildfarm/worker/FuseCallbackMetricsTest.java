// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;

import io.prometheus.client.CollectorRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ru.serce.jnrfuse.ErrorCodes;

@RunWith(JUnit4.class)
public class FuseCallbackMetricsTest {
  @Test
  public void shortReadsCountActualBytesAndFailedReadsOnlyCountRequestedBytes() {
    FuseCallbackMetrics metrics = FuseCallbackMetrics.READ;
    double requested = sample("fuse_io_requested_bytes_total", "read");
    double returned = sample("fuse_io_bytes_total", "read");
    double missing = sample("fuse_callbacks_total", "read", "enoent");
    double calls = sample("fuse_callback_seconds_count", "read");
    long start = metrics.start();
    assertThat(sample("fuse_callbacks_in_flight", "read")).isEqualTo(1.0);
    metrics.finish(start, 20, 4096);
    metrics.finish(metrics.start(), -ErrorCodes.ENOENT(), 4096);
    metrics.finish(metrics.start(), 0, 4096); // EOF
    assertThat(sample("fuse_io_requested_bytes_total", "read") - requested).isEqualTo(12288.0);
    assertThat(sample("fuse_io_bytes_total", "read") - returned).isEqualTo(20.0);
    assertThat(sample("fuse_callbacks_total", "read", "enoent") - missing).isEqualTo(1.0);
    assertThat(sample("fuse_callback_seconds_count", "read") - calls).isEqualTo(3.0);
    assertThat(sample("fuse_callbacks_in_flight", "read")).isEqualTo(0.0);
  }

  @Test
  public void exceptionsAreCountedAndDoNotLeakInflightRequests() {
    FuseCallbackMetrics metrics = FuseCallbackMetrics.GETATTR;
    double before = sample("fuse_callbacks_total", "getattr", "exception");
    metrics.finish(metrics.start(), FuseCallbackMetrics.EXCEPTION, 0);
    assertThat(sample("fuse_callbacks_total", "getattr", "exception") - before).isEqualTo(1.0);
    assertThat(sample("fuse_callbacks_in_flight", "getattr")).isEqualTo(0.0);
  }

  private static double sample(String name, String operation, String... result) {
    Double value =
        CollectorRegistry.defaultRegistry.getSampleValue(
            name,
            result.length == 0 ? new String[] {"operation"} : new String[] {"operation", "result"},
            result.length == 0 ? new String[] {operation} : new String[] {operation, result[0]});
    return value == null ? 0 : value;
  }
}
