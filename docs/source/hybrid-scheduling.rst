Hybrid Scheduling for Small Tasks
=================================

This document outlines a narrow extension to Dask Distributed that preserves the
existing centralized scheduler model while reducing control-plane overhead on
tiny-task and short-chain workloads.

The proposal is intentionally conservative. It does not replace the scheduler,
the worker state machine, or the existing P2P data movement stack. Instead, it
adds semantic batching and a bounded worker-local execution mode for cases where
the scheduler is currently doing more per-task work than the workload justifies.

Problem Statement
-----------------

Dask already has several properties that should be preserved:

- centralized correctness and visibility
- locality-aware worker selection
- worker-side ready queues and dependency gathering
- work stealing for bounded rebalancing
- peer-to-peer bulk data movement
- cluster-wide memory management

The remaining bottleneck for very small tasks is mostly in the control path:

- one logical dispatch per task
- one logical completion handling path per task
- repeated Python allocation and transition work in scheduler hot paths
- repeated scheduler/worker round trips for frontiers that are already strongly local

This is visible in the current implementation:

- queue opening is per-task in `Scheduler.stimulus_queue_slots_maybe_opened`
- completion handling is per-task in `Scheduler.stimulus_task_finished`
- task dispatch is still single-task in `Scheduler.send_task_to_worker`

At the same time, the worker already has meaningful local machinery:

- `ready` and `constrained` priority heaps
- dependency gathering that batches by peer worker
- local execution slot filling

This suggests that the main missing feature is not local scheduling from
scratch. The missing feature is explicit, scheduler-bounded delegation.

Goals
-----

- improve tiny-task throughput and p95 latency
- reduce scheduler CPU at fixed throughput
- improve worker occupancy on short chains and locality-heavy frontiers
- preserve priorities, restrictions, retries, tracing, and failure semantics
- avoid introducing a second independent scheduler inside workers

Non-goals
---------

- replacing centralized scheduling with a decentralized scheduler
- redesigning the existing shuffle implementation
- introducing a new standalone transport or storage subsystem
- changing user-facing APIs for normal Dask users

Proposal
--------

Phase 1: Semantic batching
~~~~~~~~~~~~~~~~~~~~~~~~~~

Add semantic batching for compute dispatch and completion handling:

- ``ComputeTaskBatch``: scheduler to worker
- ``TaskFinishedBatch``: worker to scheduler

This is different from the existing transport-level `BatchedSend`. Dask already
coalesces multiple outbound messages on a stream, but it still largely reasons
about one task per dispatch and one task per completion path. Semantic batches
reduce hot-path scheduler work directly.

Candidate batch entry fields:

- key
- run_id
- priority
- run_spec
- dependency metadata
- annotations required for execution
- span or trace context

Phase 2: Tiny-task fast path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add a narrow common-case scheduler path for tasks that satisfy all of the
following:

- no actors
- no active retry path
- no unusual annotations
- no non-default resource restrictions
- dependencies already local or absent
- not participating in worker-specific special handling

This fast path must preserve the same externally visible semantics. It should
only bypass unnecessary intermediate bookkeeping on the common path.

Phase 3: Single-worker execution leases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Introduce a lease-like capability issued by the scheduler to a single worker for
a bounded frontier. Inside that envelope the worker may:

- order runnable tasks locally
- continue dependency prefetch using existing worker-side logic
- batch completion reports

Inside the initial version, the worker may not:

- place tasks on other workers
- violate resource or placement restrictions
- bypass scheduler-controlled retries
- execute actor tasks under lease mode
- create hidden task state that the scheduler cannot attribute

Why single-worker first:

- it reuses existing worker heaps and dependency-gathering logic
- it keeps fairness and revocation simpler
- it avoids turning work stealing into a second scheduler

Phase 4: Local successor placement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only after single-worker leases are stable should Dask consider allowing a
worker to keep tiny successor tasks local under a strict scheduler-provided
envelope. This should be limited to successors whose dependencies are already
local and that do not carry additional restrictions.

Architecture Fit
----------------

These changes fit Dask's current code shape well:

- scheduler truth remains in ``distributed/scheduler.py``
- worker execution remains governed by ``distributed/worker_state_machine.py``
- transport batching remains in ``distributed/batched.py``
- rebalancing still relies on ``distributed/stealing.py``

The new feature is best understood as a scheduler extension to the existing
architecture, not a competing runtime inside the runtime.

What This Should Improve
------------------------

- tiny-task graphs where scheduler overhead dominates compute time
- short dependency chains that are already strongly local
- bursty workloads that cause queue churn on the scheduler
- throughput ceilings caused by centralized per-task dispatch cost

What This Probably Won't Improve
--------------------------------

- long-running tasks
- actor-heavy applications
- workloads dominated by serialization or large payload movement
- tasks with strong placement or resource constraints
- broad shuffle performance independent of scheduler overhead

Main Risks
----------

- fairness drift if workers retain too much local discretion
- observability regressions if delegated work is not attributed cleanly
- semantic regressions around priorities, annotations, retries, and restrictions
- excessive complexity if worker-group leases are attempted too early

Success Criteria
----------------

- lower scheduler CPU on synthetic tiny-task benchmarks
- lower control messages per completed task
- improved p50 and p95 latency for sub-10ms tasks
- no regressions in retries, worker loss handling, or task visibility
- no material fairness regression relative to current scheduler policies
