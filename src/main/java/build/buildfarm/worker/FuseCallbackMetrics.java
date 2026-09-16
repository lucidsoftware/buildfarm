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

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.Locale;
import ru.serce.jnrfuse.ErrorCodes;

/** Pre-bound metric children avoid label lookup/allocation on successful callback hot paths. */
enum FuseCallbackMetrics {
  GETATTR,
  READLINK,
  SYMLINK,
  RENAME,
  LINK,
  CHOWN,
  TRUNCATE,
  FTRUNCATE,
  CHMOD,
  UTIMENS,
  ACCESS,
  UNLINK,
  RMDIR,
  GETXATTR,
  SETXATTR,
  LISTXATTR,
  REMOVEXATTR,
  MKNOD,
  CREATE,
  OPEN,
  RELEASE,
  WRITE,
  FLUSH,
  FSYNC,
  STATFS,
  READ,
  MKDIR,
  READDIR,
  FALLOCATE;

  static final int EXCEPTION = Integer.MIN_VALUE;

  // Nested holder: collectors must initialize before enum constructors bind their children.
  private static final class Metrics {
    static final Counter calls =
        Counter.build()
            .name("fuse_callbacks_total")
            .help("FUSE callbacks completed, including errno returns and Java exceptions.")
            .labelNames("operation", "result")
            .register();
    static final Histogram duration =
        Histogram.build()
            .name("fuse_callback_seconds")
            .help("Time inside Java FUSE callbacks, excluding kernel/dispatch queue time.")
            .labelNames("operation")
            .buckets(
                .000001, .000005, .00001, .000025, .00005, .0001, .00025, .0005, .001, .0025, .005,
                .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30)
            .register();
    static final Gauge inflight =
        Gauge.build()
            .name("fuse_callbacks_in_flight")
            .help("Currently executing Java FUSE callbacks.")
            .labelNames("operation")
            .register();
    static final Counter requestedBytes =
        Counter.build()
            .name("fuse_io_requested_bytes_total")
            .help("Requested bytes in read/write callbacks, including failed requests.")
            .labelNames("operation")
            .register();
    static final Counter bytes =
        Counter.build()
            .name("fuse_io_bytes_total")
            .help(
                "Bytes successfully returned by read/write callbacks; not unique or network bytes.")
            .labelNames("operation")
            .register();
    static final Histogram requestSize =
        Histogram.build()
            .name("fuse_io_request_bytes")
            .help("Requested size of read/write callbacks.")
            .labelNames("operation")
            .buckets(0, 64, 256, 1024, 4096, 16384, 32768, 65536, 131072, 262144, 1048576)
            .register();
  }

  private final Counter.Child ok;
  private final Counter.Child missing;
  private final Counter.Child unsupported;
  private final Counter.Child permission;
  private final Counter.Child ioError;
  private final Counter.Child otherError;
  private final Counter.Child exception;
  private final Histogram.Child duration;
  private final Gauge.Child inflight;
  private final boolean io;
  private final Counter.Child requestedBytes;
  private final Counter.Child bytes;
  private final Histogram.Child requestSize;

  FuseCallbackMetrics() {
    String operation = name().toLowerCase(Locale.ROOT);
    ok = Metrics.calls.labels(operation, "ok");
    missing = Metrics.calls.labels(operation, "enoent");
    unsupported = Metrics.calls.labels(operation, "unsupported");
    permission = Metrics.calls.labels(operation, "permission");
    ioError = Metrics.calls.labels(operation, "io_error");
    otherError = Metrics.calls.labels(operation, "other_error");
    exception = Metrics.calls.labels(operation, "exception");
    duration = Metrics.duration.labels(operation);
    inflight = Metrics.inflight.labels(operation);
    io = operation.equals("read") || operation.equals("write");
    requestedBytes = io ? Metrics.requestedBytes.labels(operation) : null;
    bytes = io ? Metrics.bytes.labels(operation) : null;
    requestSize = io ? Metrics.requestSize.labels(operation) : null;
  }

  long start() {
    inflight.inc();
    return System.nanoTime();
  }

  void finish(long startedNanos, int result, long requested) {
    double elapsed = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    inflight.dec();
    duration.observe(elapsed);
    if (result >= 0) {
      ok.inc();
    } else if (result == EXCEPTION) {
      exception.inc();
    } else if (result == -ErrorCodes.ENOENT()) {
      missing.inc();
    } else if (result == -ErrorCodes.EOPNOTSUPP() || result == -ErrorCodes.ENOSYS()) {
      unsupported.inc();
    } else if (result == -ErrorCodes.EPERM() || result == -ErrorCodes.EACCES()) {
      permission.inc();
    } else if (result == -ErrorCodes.EIO()) {
      ioError.inc();
    } else {
      otherError.inc();
    }
    if (io) {
      requestedBytes.inc(Math.max(0, requested));
      requestSize.observe(Math.max(0, requested));
      bytes.inc(Math.max(0, result));
    }
  }
}
