# Buildfarm before/after performance experiment

## Branches and bases

| Variant | Branch | Base |
| --- | --- | --- |
| Before, no FUSE | `bfreestone-perf-before-fuse` | `origin/lucid` at `71af06e93c15d0dc6ab06239697781dc68e9d417` |
| After, FUSE | `bfreestone-perf-with-fuse` | `bfreestone-fuse` at `06cb9cef252e8fb58aa5ce9a33b74c80c9f290f0` |

These deliberately retain the requested bases. Lucid also contains CAS and persistent-worker
changes absent from the FUSE base: this measures the two builds, not the isolated effect of FUSE.
The FUSE branch includes Lucid's lifecycle histogram commit before the shared instrumentation.

Build either branch with:

```sh
bazel build //src/main/java/build/buildfarm:buildfarm-shard-worker
```

Use your normal worker configuration on the before branch. On the after branch, set
`worker.execFileSystemType: FUSE` and retain `FILESYSTEM` CAS storage. The after branch still eagerly
stages input blobs in local CAS. FUSE requires the host's FUSE setup, including `libfuse.so.2` (FUSE 3 alone is insufficient), and does not support
`execOwner`/`execOwners`. The baseline does not recognize `execFileSystemType`; do not add it there.

Add a Prometheus scrape target label `variant="before"` or `variant="fuse"` to distinguish cohorts.
Metric labels intentionally contain no action IDs, digests, invocation IDs, or paths.

## Shared metrics

| Metric | Meaning |
| --- | --- |
| `input_fetch_time_ms`, `execution_time_ms`, `report_result_time_ms` | Existing full stage lifetimes, with identical explicit millisecond buckets in both builds. Include downstream handoff wait; report-result also includes cleanup. |
| Corresponding `*_stall_time_ms` | Existing downstream handoff wait, with identical explicit millisecond buckets. |
| `worker_operation_phase_seconds{phase}` | Existing timestamp-based lifecycle phases, now available on both builds. |
| `worker_action_phase_seconds{phase,outcome}` | One observation per completed action with valid timestamps for that phase, including zero durations. Allows comparing successful actions separately from failures. |
| `worker_action_results_total{outcome}` | Completed results observed at the worker terminal stage. Outcomes: `success`, `action_failure`, `cancelled`, `timeout`, `infrastructure_error`, `unknown`. Incomplete/requeued attempts are excluded, as are action-cache hits served without execution. |
| `create_exec_root_seconds{outcome}` | Monotonic wall time around `ExecFileSystem.createExecDir`, including staging and any rollback before it returns. Outcome is `success` or `failure`, including unchecked exceptions and interruption. |
| `destroy_exec_root_seconds{outcome}` | Monotonic wall time around `ExecFileSystem.destroyExecDir`, including reference release. Same success/failure semantics. Includes cleanup after reporting and other calls through the worker context. |

Action phases are `queued_to_match`, `match_to_input_fetch`, `input_fetch`,
`input_fetch_to_execution`, `execution`, `execution_to_output_upload`, `output_upload`,
and `worker_to_output_complete`. The last spans worker acceptance to output-upload completion;
it excludes subsequent result publication and cleanup. Missing, invalid, or reversed timestamps
are omitted rather than recorded as zero. Outcome histograms cover completed operations only;
use the stage and execroot histograms to see work on unsuccessful/incomplete attempts too.

Internal rollback invoked directly by a filesystem implementation is included in the preparation
timer, not counted again as a worker-context cleanup call. Cleanup measures the synchronous API
boundary, not background disk/CAS eviction. A cleanup failure does not relabel an already
successful action as failed.

The existing milliseconds histograms retain their Lucid bucket ranges (see code); inspect the
`+Inf` fraction for unusually long actions/stalls. New action histograms extend through 3600s,
preparation through 600s, and cleanup through 300s. Never pool buckets from uninstrumented older
workers with these cohorts.

## Per-action records

Both branches emit INFO log messages containing JSON from
`build.buildfarm.worker.WorkerPerformanceMetrics`:

* `worker_action_performance`: operation name, action digest, invocation ID, worker, execroot,
  outcome/status/exit code, available phase durations, fetched bytes, and worker usage metadata.
  `usage.execroot_prepare_nanos` is the preparation duration for that attempt.
* `worker_execroot_cleanup`: execroot, outcome, and cleanup duration. Cleanup occurs after the
  completion record. Join on execroot plus the worker/log source; paths can repeat across workers
  or retries, so retain timestamps and occurrence order. Cleanup of a requeued action may have no
  corresponding completed-action record.

Logs may be wrapped in the deployment's JSON logging envelope; parse its message as JSON.
Logging is enabled for these experiment branches. Disable records (keeping Prometheus metrics)
with JVM option `-Dbuildfarm.performance.logActions=false`, or suppress that logger. Compare with
logging disabled on both cohorts if log overhead is material. Raw records are necessary for paired
per-action analysis: Prometheus aggregates observations and does not retain individual actions.
`fetched_bytes` is backend accounting, not an exact network-byte measurement; the two bases have
different directory/blob accounting paths.

## FUSE-only metrics

All 29 callbacks overridden by `FuseCAS` are instrumented. Inherited default/unimplemented FUSE
callbacks are not instrumented. Metrics appear when the callback instrumentation first initializes.

| Metric | Meaning |
| --- | --- |
| `fuse_callbacks_total{operation,result}` | Completed callbacks; result is `ok`, `enoent`, `unsupported`, `permission`, `io_error`, `other_error`, or `exception`. `ok` includes EOF and successful short I/O. |
| `fuse_callback_seconds{operation}` | Java callback duration, with microsecond-to-30-second buckets. Includes failed callbacks. |
| `fuse_callbacks_in_flight{operation}` | Concurrent callbacks currently inside Java. |
| `fuse_io_requested_bytes_total{operation}` | Requested read/write bytes, including failed calls. |
| `fuse_io_bytes_total{operation}` | Bytes actually returned from successful read/write callbacks. |
| `fuse_io_request_bytes{operation}` | Read/write request-size distribution, through 1 MiB. |

These aggregate across actions and across input, scratch, and output-upload access. They do not
measure network transfer or unique bytes, and do not count reads satisfied entirely by kernel
caches. Callback duration excludes kernel/dispatch queue time. Concurrent durations overlap, so
summing them is not action wall time. Delegated `ftruncate`/`fallocate` helper calls do not generate
extra `truncate` callback counts. No per-path tracing or per-callback logging is performed.
Pre-bound Prometheus children avoid hot-path label lookup; timing/counter overhead still exists.

## Initial dashboard queries

Successful action phase p95 by cohort (seconds):

```promql
histogram_quantile(0.95,
  sum by (variant, phase, le) (
    rate(worker_action_phase_seconds_bucket{outcome="success"}[5m])
  )
)
```

Successful completed actions per second:

```promql
sum by (variant) (rate(worker_action_results_total{outcome="success"}[5m]))
```

Cleanup p95:

```promql
histogram_quantile(0.95,
  sum by (variant, le) (rate(destroy_exec_root_seconds_bucket{outcome="success"}[5m]))
)
```

FUSE callback rate and mean latency (including errors):

```promql
sum by (operation, result) (rate(fuse_callbacks_total{variant="fuse"}[5m]))
```

```promql
sum by (operation) (rate(fuse_callback_seconds_sum{variant="fuse"}[5m]))
/
sum by (operation) (rate(fuse_callback_seconds_count{variant="fuse"}[5m]))
```

Compare identical action sets and worker counts/resources. Force remote execution instead of
accepting action-cache results. Separate cold/warm CAS and OS page-cache conditions; test low and
saturated concurrency. Include metadata-heavy, large-input, output-heavy, and CPU-heavy workloads.
Separate first-mount runs from steady state (FUSE unmounts after 10 idle seconds). Pair stage metrics
with client build duration, host/action/worker CPU and memory, disk I/O, and JVM GC. A smaller
InputFetch alone is not evidence of a net gain.

## Validation

Both branch worker binaries were built with the build command above. Both worker and shard test
suites passed. On the FUSE branch, tests also ran with `--config=fuse`, including the mounted
filesystem smoke test, callback exception/delegation tests, and metrics accounting tests.

```sh
# Before branch
bazel test //src/test/java/build/buildfarm/worker:tests \
  //src/test/java/build/buildfarm/worker/shard:tests --test_output=errors

# FUSE branch, on a host providing libfuse.so.2 and /dev/fuse
bazel test //src/test/java/build/buildfarm/worker:tests \
  //src/test/java/build/buildfarm/worker/shard:tests --config=fuse --test_output=errors
```

For validation on the development host, `libfuse2t64` was downloaded and extracted under `/tmp`
and its library directory passed via `--test_env=LD_LIBRARY_PATH=...`; no system package was
installed. The mounted smoke test is conditional on `/dev/fuse` and does not replace running
representative builds under load. No before/after performance results have been collected yet.
