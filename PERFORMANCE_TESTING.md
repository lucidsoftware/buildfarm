# Buildfarm before/after performance experiment

## Branches and bases

| Variant | Branch | Base |
| --- | --- | --- |
| Before, no FUSE | `bfreestone-perf-before-fuse` | `origin/lucid` at `71af06e93c15d0dc6ab06239697781dc68e9d417` |
| After, FUSE | `bfreestone-perf-with-fuse` | `bfreestone-perf-before-fuse` at `6da6cc66b47e3311f95ce44d248cf95ec38e01aa` |

Both variants now share the exact Lucid base and shared performance instrumentation. The after
branch adds the FUSE implementation cherry-picked from `06cb9cef`, now with its callback data
plane replaced by a native Rust process. CAS and persistent-worker
implementations are identical across the two branches. The original `bfreestone-fuse` branch is
unchanged and remains separate for upstream work.

The previous version of `bfreestone-perf-with-fuse` (`994981f4`) used the older upstream-based
FUSE branch as its base. Use the rebuilt branch for comparable runs; the testing guide in the
unchanged before branch describes that earlier arrangement.

Build either branch with:

```sh
bazel build //src/main/java/build/buildfarm:buildfarm-shard-worker
```

Use your normal worker configuration on the before branch. On the after branch, set
`worker.execFileSystemType: FUSE` and retain `FILESYSTEM` CAS storage. The after branch still eagerly
stages input blobs in local CAS. FUSE requires `/dev/fuse` and permission to mount it, but the Rust
implementation talks to the kernel directly and has no `libfuse` runtime dependency. It does not support
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
`fetched_bytes` is backend accounting, not an exact network-byte measurement; CFC and FUSE have
different directory/blob accounting paths even with the same CAS implementation.

## FUSE data plane

FUSE callbacks execute concurrently in the native Rust process; the JVM only sends root lifecycle
manifests over a control pipe. The filesystem keeps one mount for the worker lifetime, uses persistent
file handles, requests 1 MiB reads/writes and readahead, and negotiates Linux FUSE passthrough when the
kernel supports it. Callback-level Prometheus instrumentation was deliberately removed from the hot
path. Use the shared action and exec-root metrics above for the before/after comparison.

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

Compare identical action sets and worker counts/resources. Force remote execution instead of
accepting action-cache results. Separate cold/warm CAS and OS page-cache conditions; test low and
saturated concurrency. Include metadata-heavy, large-input, output-heavy, and CPU-heavy workloads.
Separate first-mount runs from steady state. The native filesystem mounts on the first FUSE root and
stays mounted for the worker lifetime. Pair stage metrics
with client build duration, host/action/worker CPU and memory, disk I/O, and JVM GC. A smaller
InputFetch alone is not evidence of a net gain.

## Validation

Both branch worker binaries were built with the build command above. Both worker and shard test
suites passed. After rebuilding FUSE on the shared Lucid baseline, its worker, shard,
configuration, and persistent-worker suites passed with `--config=fuse`. The Rust state tests and
Java control-protocol tests cover the native implementation boundary.

```sh
# Before branch
bazel test //src/test/java/build/buildfarm/worker:tests \
  //src/test/java/build/buildfarm/worker/shard:tests --test_output=errors

# FUSE branch; mounted integration requires /dev/fuse
bazel test //src/test/java/build/buildfarm/worker:tests \
  //src/test/java/build/buildfarm/worker/shard:tests \
  //src/test/java/build/buildfarm/common/config:tests \
  //src/test/java/build/buildfarm/worker/persistent:tests --config=fuse --test_output=errors
```

The mounted smoke test is conditional on `/dev/fuse` and does not replace running representative
builds under load. No before/after performance results have been collected yet.
