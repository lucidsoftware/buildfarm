---
layout: default
title: Workers
parent: Architecture
nav_order: 2
---

# Workers

Workers have two major roles in Buildfarm: Execution and CAS Shard. Either of these options can be disabled, though a worker with both disabled provides no value.

Regardless of role, a worker must have a local FILESYSTEM type [storage](https://buildfarm.github.io/buildfarm/docs/configuration/configuration/#worker-cas) to retain content. This storage serves both as a resident LRU cache for Execution I/O, and the local storage for a CAS Shard. Workers can delegate to successive storage declarations (FILESYSTEM or GRPC), with read-through or expiration waterfall if configured, but only the first storage entry will be used for Executions.

## Execution

Execution Workers are responsible for matching their environments against operations, presenting execution roots to those operations, fetching content from a CAS, executing processes required to complete the operations, and reporting the outputs and results of executions. Control and delivery of these behaviors is accomplished with several mechanisms:

* A CAS FileCache, which is capable of reading through content for Digests of files or directories, and efficiently presenting those contents based on usage and reference counting, as well as support for cascading into delegate CASs.
* ExecutionPolicies, which allow for explicit and implicit behaviors to control execution.
* Execution Resources to limit concurrent execution in installation-defined resource traunches.
* Concurrent pipelined execution of operations, with support for superscalar stages at input fetch and execution.
* Operation exclusivity, preventing the same operation from running through the worker pipeline concurrently.

## CAS Shard

Sharded workers interact with the shard backplane for both execution and CAS presentation. Their CAS FileCache serves a CAS gRPC interface as well as the execution root factory.

# Pipelines

A pipeline handles operations as they arrive and are processed on a worker. Each stage of the pipeline performs its task on an operation, and holds that task until the subsequent stage can take over. This creates backpressure to mitigate risk and limit resource consumption. Since a worker only takes on enough work to exhaust the most limited resource at a time (CPU/IO/Bandwidth), losing that worker due to unforeseen failures is not disruptive to the rest of the cluster. Keeping work in each stage holds resources for an operation as well, so preventing operations from piling up in an earlier stage due to a longer running later stage reduces the overall resource footprint required for the worker.

## Stages

![Stages]({{site.url}}{{site.baseurl}}/assets/images/WorkerStages.png)

Stages have access to a WorkerContext provided to them by their Worker implementation (OperationQueue or Shard) which is used for all activity common across Worker types. Each stage must claim the subsequent stage before submitting it for processing. This allows measurement of processing time and latency for each operation per stage without any interleaving.

Superscalar stages have a configurable number of slots for their activity. Claims on these stages block until they are full, and have a number of slots to claim exclusively for activity.

### Match

The Match stage is responsible for dequeuing an operation from the Ready-To-Run queue. This operation is dequeued as a QueueEntry which contains the ExecuteEntry and a Digest for the transformed QueuedOperation. The ExecuteEntry contains a Platform definition which must match the worker's provided platform manifest in order to proceed. A rejected QueueEntry for this reason will be reinserted into the Ready-To-Run queue.

The Match stage is unique in that it claims a slot in the Input Fetch stage prior to its iteration. This removes a polling requirement for the active operation present in other stages while waiting to feed the interstage, reducing the stage's complexity.

### Input Fetch

Input Fetch is a superscalar stage responsible for downloading the QueuedOperation from the CAS, and creating the execution directory for the Operation. This is the worker ingress bandwidth, and likely the disk IO write, consuming stage. Its configured concurrency is available in the worker config as `input_fetch_stage_width`. The ownership of output directories is configurable with [[exec_owner]].

### Execution

Execution is a superscalar stage which initiates operation executions, applying any ExecutionPolicies. The operation transitions to the EXECUTING state when it reaches this stage. After spawning the process, it intercepts writes to stdout and stderr, and will terminate the process if it runs longer than its Action specified timeout. Its configured concurrency is available in the worker config as `execute_stage_width`. [[Execution Limiting]] is available as a configuration option under cgroups, if supported.

### Report Result

The Report Result stage injects any outputs from the operation into the CAS, and populates the ActionResult from from the results of the execution. It can inject into the ActionCache for cacheable actions, and can record an action in the blocklist if it violates output policy. The operation transitions to COMPLETED state after the outputs are recorded. After this stage is complete, the execution directory is destroyed, and the Operation exits the worker.

# Exec Filesystem

Workers use ExecFileSystems to present content to actions, and manage their existence for the lifetime of an operation's presence within the pipeline. The realization of an operation's execution root with the execution filesystem constitutes a transaction that the operating directory for an action will appear, be writable for outputs, and released and be made unavailable as it proceeds and exits the pipeline.

This means that an action's entire input directory must be available on a filesystem from a unique location per operation - the _Operation Action Input Root_, or just _Root_. Each input file within the Root must contain the content of the inputs, its requested executability via FileNode, and each directory must contain at the outset, child input files and directories. The filesystem is free to handle unspecified outputs as it sees fit, but the directory hierarchy of output files from the Root must be created before execution, and writable during it. When execution and observation of the outputs is completed, the exec filesystem will be asked to destroy the Root and release any associated resources from its retention.

`exec_file_system_type` selects the execution filesystem independently of the configured storage.
`CFC` is the default. `FUSE` requires a filesystem-backed first storage entry, `/dev/fuse`, and
permission for the worker process to mount it. FUSE does not support `exec_owner`/`exec_owners`.

## CASFileCache/CFCExecFilesystem

The CASFileCache provides an Exec Filesystem via CFCExecFilesystem. The (CASFileCache)'s retention of paths is used to reflect individual files, with these paths hard-linked in CFCExecFilesystem under representative directories of the input root to signify usage. The CASFileCache directory retention system is also used to provide a configurable utilization of entire directory trees as a symlink, which was a heuristic optimization applied when substantial cost was observed setting up static trees of input links for operations compared to their execution time. `link_input_directories` in the common Worker configuration will enable this heuristic.
Outputs of actions are physically streamed into CAS writes when they are observed after an action execution.

The CASFileCache's persistence in the filesystem and the availability of common POSIX features like symlinks and inode-based reference counts on almost any filesystem implementation have made it a solid choice for extremely large CAS installations - it scales to multi-TB host attached storages containing millions of entries with relative ease.

There are plans to improve CASFileCache that will be reflected in improved performance and memory footprint for the features used by CFCExecFilesystem.

## Fuse

The FUSE execution filesystem represents complete action roots without creating a hard link for every
input. Inputs remain immutable files in the local CAS; output and scratch files use ordinary backing
files. A native Rust process owns the mount and all kernel callbacks, while Java sends compact root
manifests when actions enter and leave the worker. The process is mounted once and shared across action
roots for the worker lifetime. It uses concurrent request handling and persistent file handles, and on
supporting Linux kernels it negotiates FUSE passthrough so file data I/O can bypass the userspace
callback path. The implementation uses the kernel FUSE protocol directly and does not require a
`libfuse` package in the worker image.
