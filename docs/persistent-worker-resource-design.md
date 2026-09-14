# Persistent worker resource ownership

Persistent workers own their compiler process, descendants, private execution directory, and
stable cgroup. A request owns an exclusive lifecycle lease and an active CPU allocation.
Request completion does not destroy process-owned resources.

## Cgroup layout and compatibility

Normal actions retain their existing per-operation cgroups. Persistent workers use
`executions/persistent-workers/<process UUID>` below the same execution parent, so the common
execution CPU ceiling still applies. The operation's resource handle does not own this cgroup.

The process-specific cgroup wrapper is attached at process creation, after choosing the pool key.
Operation IDs and cgroup launch paths therefore do not split otherwise compatible compiler keys.
The key also contains the stable resource profile (cgroup parent, wrapper, and memory limit).
Different memory limits use different pools. Tool hashes, environment, and existing execution
wrapper arguments remain part of compiler compatibility as before. Per-operation sandbox or
custom wrapper arguments can still prevent reuse; this change does not strip those restrictions.

The existing path without cgroup enforcement remains supported. Freezing and cgroup accounting
apply to managed workers. Managed workers require cgroup v2 with `cgroup.freeze`, `memory.max`,
and `cgroup.kill` support; startup fails if group-wide termination is unavailable.

## Request execution

1. Acquire a worker and its generation-checked lifecycle lease.
2. Prepare request inputs while the compiler is inactive.
3. Install the current CPU allocation, snapshot cumulative counters, and unfreeze the compiler.
4. Submit the protocol request. Use the same CPU adjustment loop as native execution, waiting
   for the protocol response instead of process exit.
5. Stop monitoring and confirm the compiler is frozen before copying outputs and releasing
   the lifecycle lease. Report CPU counter differences for this request, not process-lifetime totals.
6. Release request CPU capacity. Keep the frozen compiler and its retained memory for reuse.

Resource mutations are guarded by the exact lifecycle generation. A callback from a previous
request cannot change the quota or resume/freeze a worker already serving another request.

Timeout and interruption invalidate the lease, cancel outstanding CPU orders, and retire the
worker. Cancelling the response-reader task interrupts its wait; process termination closes
its pipes if it is blocked in protocol I/O. The coordinator retains its lease-guarded watchdog
covering setup/execution. For market executions this permits twice the original action timeout,
matching the monitor's single extension, while imposing a hard bound including input setup.

## Pool contention and native fallback

The execution stage claims CPU capacity before obtaining a compatible persistent worker.
Waiting indefinitely for a busy key or a full global PW pool can therefore reserve most of a
machine's execution capacity without running useful work.

`worker.persistentWorkers.poolWaitTimeoutMillis` bounds this pool-contention wait (default
1000 milliseconds; zero attempts immediate acquisition/creation and falls back when full).
The borrow wait is also capped by the supplied request timeout. Tool setup and creation of a
new process are synchronous operations, not bounded by this pool-contention timer.

Only a pool-exhaustion result before acquiring a process permits native fallback. Startup,
validation, protocol, cleanup, and execution failures do not trigger a second execution.
Interrupted requests do not fall back. The executor closes the initial resource handle and
rebuilds the original command with native wrappers, ownership handling, and per-operation
cgroup enforcement. It retains the existing execution claim and subtracts elapsed attempt
time from the original timeout; an expired budget returns DEADLINE_EXCEEDED without launching.
The fallback does not occupy a PW-pool slot and remains subject to normal execution capacity.

`persistent_worker_requests_total{outcome="pool_timeout"}` records the unsuccessful PW
acquisition. `persistent_worker_fallbacks_total` counts transitions into native execution,
not successful native completions. A pool timeout can therefore precede a successful action.
Native fallback work does not increment the PW successful-request counter. Compare fallback
counts with pool waiters and normal action outcomes when testing saturation.

## Idle CPU and memory policy

Idle managed workers are frozen, with confirmation bounded to five seconds. They consume no
background execution CPU while idle; background GC pauses until the next request. A newly
created group's launch wrapper also joins a frozen group and cannot start the compiler until
the request's CPU quota is installed.

Memory limits remain stable for the life of a PW and use the cgroup-v2 `memory.max` interface.
An unlimited request has an unlimited profile; a limited request cannot reuse that worker.
Retained memory remains charged to the PW while idle. There is no new global memory budget:
operators still need to size the process count, memory limits, and idle-retirement policy together.

## Termination and shutdown

Retirement sends graceful signals, thaws idle workers to allow exit, and waits for the configured
grace. If necessary it uses `cgroup.kill`, then waits for an empty group with a second bounded
wait. Forced fallback termination also targets the group, including descendants whose parent
has exited. Directory cleanup follows only confirmed termination. Failures are reported and
the execution directory is preserved when termination cannot be confirmed.

Shutdown drains the execution pipeline and closes the PW pool before dismantling the shared
cgroup hierarchy. Existing native process cleanup stays on its per-action path.

## Observation-only key tracing

Enable this on a controlled cohort of execution workers:

```yaml
worker:
  persistentWorkerActionMnemonicAllowlist:
    - ScalaCompile
    - ScalaCheckDeps
  persistentWorkers:
    observationOnly: true
    observationSampleRate: 1.0
```

Clients must use `--experimental_remote_mark_tool_inputs`. The existing mnemonic and marked
executable checks still determine which actions have a traceable PW key. Missing markings
produce `unmarked_executable` eligibility counts, not invented compatibility keys. Tracing
covers executed actions, not remote cache hits or requests never dispatched to these workers.

Observation mode always executes actions through the ordinary native/container path with its
normal resource handling, including unsampled actions and key-preparation failures. It does
not obtain a PW, initialize the pool, copy tools into PW directories, or create PW cgroups.
The candidate key uses the same command preparation, environment overrides, tool hashes, and
resource profile as real PW execution. Its PW launch wrappers are prepared separately from
the actual native wrappers, so per-operation native cgroup paths do not fragment the key.
The mode defaults off and is read from worker configuration at startup.

The logger `build.buildfarm.worker.PersistentWorkerObservation` emits INFO messages prefixed
`PW_OBSERVATION ` followed by a single JSON object. Preserve these INFO messages in your log
collection. There is one `start` and one `finish` record per sampled eligible action during
normal operation; process termination can leave unmatched starts. No per-key Prometheus
series or unbounded in-process key registry is added.

Both records include:

- `schema_version` (currently 1), `event`, UTC `timestamp`, `worker`, `worker_session`,
  `operation`, unique `attempt`, `invocation`, `mnemonic`, `host_architecture`, and `sample_rate`.
- `action_architecture` when the command has the `cpu-architecture` platform property.
- `queued_at`, `worker_started_at`, and `execution_stage_started_at` when present in action
  metadata. These are the existing execution metadata timestamps; worker start is not a new
  measurement of queue insertion or CPU reservation.
- `key`: a SHA-256 fingerprint of all current WorkerKey equality fields. Environment keys
  are sorted before hashing. Raw arguments, environment values, tool paths, and key objects
  are not logged by the observation logger.
- `tools_hash`, `environment_hash`, `launch_hash`, `resource_profile_hash`, and `work_root_hash`
  to help distinguish tool changes from configuration fragmentation.
- `key_status` (`computed` or `error`) and `observation_preparation_ms`. On preparation failure,
  `key` is absent and `error_type` contains only the exception class, not its message.

Finish records also include `status`, `exit_code`, and `native_attempt_ms`. Native attempt time
includes native launch/resource preparation and cleanup; it is not compiler-only CPU time.
`status=OK` is a successful executor status; check `exit_code` for action success. Trace emission
is best effort: a logging failure does not fail an action. Retain logs externally to analyze
across worker restarts. Observation has CPU/logging overhead, recorded in part by preparation
latency; use observation-disabled runs for final performance comparisons.

Use `(worker_session, attempt)` to join starts and finishes, and group by `(worker, key)`
to estimate reuse gaps and overlapping demand. An operation retry gets a new attempt ID.
Compare component hashes before grouping across workers with different work-root or cgroup
layouts: the exact compatibility fingerprint intentionally includes those configuration fields.

At sample rate 1 every eligible action is traced. Lower rates sample deterministically by
operation identity and still execute all actions normally. Sampling misses intervening requests:
do not treat sampled interarrival gaps or peak concurrency as the true workload. Prefer full
capture on a small representative cohort when sizing idle timeouts and per-key pool limits.

For plain-text worker logs, extract the JSON messages with:

```sh
sed -n 's/^.*PW_OBSERVATION //p' worker.log > pw-observations.jsonl
jq -r 'select(.event == "finish" and .key_status == "computed") |
  [.worker, .operation, .key, .mnemonic, .native_attempt_ms, .exit_code] | @csv' \
  pw-observations.jsonl
```

If the deployment wraps log messages in JSON, extract the message field first. PW request,
process-start, and pool metrics should not increase from these observed actions; eligibility
metrics still report candidate decisions. Setting `observationOnly: false` allows actual PWs
for eligible marked actions, so it is an execution-mode change, not merely a logging switch.

## Aggregate observation metrics

Observation mode also exports Prometheus metrics without action/key labels. For metrics without
JSON log events, configure:

```yaml
worker:
  persistentWorkers:
    observationOnly: true
    observationSampleRate: 1.0
    observationLogEvents: false
    observationWindowSeconds: 900
    observationMaxKeys: 10000
```

The window is measured using a monotonic clock. The tracker retains only SHA-256 key fingerprints
and their last sampled start times, bounded by `observationMaxKeys`. The collector expires entries
on scrapes as well as observations, so counts decline even when no new work arrives. Existing
keys refresh their timestamps; when capacity is full, new keys are not stored. No background
thread or timer is required. Configuration changes require a worker restart.

| Metric | Meaning |
| --- | --- |
| `persistent_worker_observation_distinct_keys` | Distinct sampled candidate keys started on this worker within the rolling window. |
| `persistent_worker_observation_tracking_incomplete` | 1 if any keys could not be tracked due to capacity within that window; distinct count is then a lower bound. |
| `persistent_worker_observation_actions_total{outcome}` | Sampled observations classified as `new_key`, `seen_key`, `capacity_exceeded`, or `key_error`. |
| `persistent_worker_observation_in_flight` | Sampled candidate native attempts currently running, including native preparation/cleanup. |
| `persistent_worker_observation_key_arrival_gap_seconds` | Histogram of time between sampled starts for a retained key within the window. This is not completion-to-next-start idle time. |
| `persistent_worker_observation_native_seconds{outcome}` | Histogram of native attempt durations, classified as `success`, `action_failure`, or `execution_error`. |
| `persistent_worker_observation_window_seconds` | Configured window length. |
| `persistent_worker_observation_key_capacity` | Configured maximum tracked keys. |

`new_key` means absent from the current tracker, not necessarily never seen during worker lifetime.
Repeated arrivals beyond the window are new observations and do not populate the arrival-gap
histogram. The gap histogram is therefore window-truncated and cannot establish longer idle
retention requirements without using a longer window. `capacity_exceeded` counts affected actions,
not distinct omitted keys. Its rolling incompleteness indicator clears after an entire window
without overflow. All values reset on restart. With sampling below 1, counts and gaps describe
only sampled actions even when tracking is complete; use 1 for sizing analysis.

Metrics initialize when observation mode first handles an action, even if no eligible action
has arrived. Candidate metrics remain zero/absent for missing tool markers; use the existing
eligibility counters to diagnose that. JSON logging can be disabled independently of these metrics.

Useful raw-value queries (retain the target's `instance` label):

```promql
{__name__=~"persistent_worker_observation_distinct_keys|persistent_worker_observation_in_flight"}
```

```promql
persistent_worker_observation_actions_total
```

```promql
persistent_worker_observation_native_seconds_sum
/
persistent_worker_observation_native_seconds_count
```

The third query is cumulative average seconds by outcome since restart. Display
`persistent_worker_observation_tracking_incomplete` separately as a completeness check.
Do not sum distinct-key gauges and interpret them as fleet-wide unique keys: a compatible key
can appear on several workers. These metrics characterize each worker's reuse opportunity.

## Validation

Local tests cover stable resource profiles, failed launches, reuse across operations,
freeze-before-pool-return ordering, stale resource leases, per-request CPU accounting,
CPU-market quota reduction, cancellation, and cgroup-v2 control-file behavior.

A separate kernel-backed integration target verifies reuse of a real compiler across two
requests, frozen CPU inactivity, quota and memory limits, pool-shutdown cleanup, and retirement
of an orphaned child. It requires an explicitly delegated writable test parent with CPU and
memory subtree controllers already enabled. It creates and removes only uniquely named children;
it does not reconfigure the parent or move the development session into another cgroup.

```sh
bazel test //src/test/java/build/buildfarm/worker/cgroup:PersistentWorkerCgroupIntegrationTest \
  --test_tag_filters=integration \
  --jvmopt=-Dbuildfarm.pw.cgroupParent=/sys/fs/cgroup/DELEGATED_TEST_PARENT \
  --test_output=errors
```

The integration target compiles but has not run in this workspace: no writable delegated
cgroup is available, and the user systemd manager is inaccessible. Run it in the worker-image
validation environment before deployment.
