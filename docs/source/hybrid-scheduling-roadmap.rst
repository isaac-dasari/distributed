Hybrid Scheduling Roadmap
=========================

This roadmap describes a practical order for implementing hybrid scheduling
ideas in Dask Distributed without destabilizing the current runtime.

Guiding Principle
-----------------

Do not begin with leases.

The first work should improve observability and semantic batching so that later
delegation changes can be measured and debugged against a stable baseline.

Stage 0: Benchmark and instrumentation
--------------------------------------

Add metrics that isolate scheduler control-path cost:

- tasks completed per second
- scheduler CPU
- transition count per completed task
- worker dispatch batch size
- completion batch size
- queue delay
- locality hit rate for tiny tasks
- p50 and p95 task latency by duration bucket

Benchmark families:

- independent tiny tasks
- tiny chains
- small fan-out/fan-in graphs
- mixed short and medium tasks
- multi-tenant priority contention

Initial harness:

- ``benchmarks/stage0_control_path_benchmark.py``

Example:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py --scenario all --tasks 200

Exit criteria:

- stable benchmarks exist in CI or a repeatable benchmark harness
- baseline metrics are recorded for the current scheduler

Stage 0 baseline run
~~~~~~~~~~~~~~~~~~~~

The current Stage 0 baseline was gathered with the local benchmark harness:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py --tasks 200

This runs five synthetic workload families on a local two-worker cluster with
one thread per worker and the benchmark defaults:

- ``--n-workers 2``
- ``--threads-per-worker 1``
- ``--tiny-delay-ms 2``
- ``--medium-delay-ms 20``

The resulting baseline numbers were:

.. list-table::
   :header-rows: 1

   * - Scenario
     - Tasks/s
     - p50 latency
     - p95 latency
     - Scheduler CPU mean
     - Transitions/task
     - Dispatch batch
     - Completion batch
     - Queue delay avg
   * - ``independent_tiny``
     - ``45.8``
     - ``3140.9 ms``
     - ``3894.1 ms``
     - ``63.3``
     - ``4.15``
     - ``1.0``
     - ``1.0``
     - ``5.17 ms``
   * - ``tiny_chain``
     - ``79.6``
     - ``1553.0 ms``
     - ``2138.1 ms``
     - ``56.2``
     - ``3.95``
     - ``1.0``
     - ``1.0``
     - ``0.0 ms``
   * - ``fanout_fanin``
     - ``83.0``
     - ``1536.3 ms``
     - ``1960.6 ms``
     - ``58.2``
     - ``3.84``
     - ``1.0``
     - ``1.0``
     - ``0.0 ms``
   * - ``mixed``
     - ``46.2``
     - ``3064.2 ms``
     - ``3880.3 ms``
     - ``69.5``
     - ``4.91``
     - ``1.0``
     - ``1.0``
     - ``208.1 ms``
   * - ``multi_tenant``
     - ``46.9``
     - ``2983.3 ms``
     - ``3826.3 ms``
     - ``62.8``
     - ``3.52``
     - ``1.0``
     - ``1.0``
     - ``6.07 ms``

Metric interpretation
~~~~~~~~~~~~~~~~~~~~~

The benchmark reports both workload-level and scheduler-control-path metrics.

Workload-level metrics:

- ``tasks_per_s`` is total completed tasks divided by wall-clock scenario time.
- ``latency_p50_ms`` and ``latency_p95_ms`` are end-to-end completion latencies
  observed by the client for the futures tracked by the scenario.
- ``scheduler_cpu_mean`` and ``scheduler_cpu_max`` come from the scheduler
  ``SystemMonitor`` samples collected during the scenario run.

Scheduler control-path metrics:

- ``transition_count`` and ``transitions_per_task`` capture how much scheduler
  state-machine work is being paid per completed task.
- ``compute_task_messages_total`` counts scheduler-to-worker compute messages.
- ``compute_task_dispatches_total`` counts tasks dispatched by the scheduler.
- ``average_dispatch_batch_size`` is dispatches divided by compute messages.
- ``task_finished_messages_total`` counts worker-to-scheduler task-finished
  messages.
- ``task_finished_tasks_total`` counts completed tasks reported back.
- ``average_completion_batch_size`` is completed tasks divided by completion
  messages.
- ``queue_delay_avg_ms`` and ``queue_delay_max_ms`` measure how long tasks sat
  in the scheduler queue before entering processing.
- ``locality_hit_rate`` estimates how often the assigned worker already had the
  task dependencies. For dependency-free tasks this is trivially ``1.0`` and
  should not be overinterpreted.

What the baseline says
~~~~~~~~~~~~~~~~~~~~~~

The baseline already shows the core motivation for Stage 1.

First, every scenario reported:

- ``compute_task_messages_total = 200``
- ``compute_task_dispatches_total = 200``
- ``task_finished_messages_total = 200``
- ``task_finished_tasks_total = 200``
- ``average_dispatch_batch_size = 1.0``
- ``average_completion_batch_size = 1.0``

This means the current baseline is strictly one logical dispatch message per
task and one logical completion message per task. There is no semantic compute
batching and no semantic completion batching yet.

Second, tiny-task throughput is low enough that scheduler overhead is clearly
material. The tiny-task scenarios run only at roughly ``46-83 tasks/s`` even
though the synthetic task bodies sleep for only ``2 ms``. This is a control-path
dominated workload, not a compute-dominated one.

Third, the ``mixed`` scenario is the clearest stress case for the current
scheduler. It shows:

- the highest ``transitions_per_task`` at ``4.91``
- the highest scheduler CPU pressure with mean ``69.5`` and max ``195.5``
- the heaviest queueing with average queue delay ``208.1 ms`` and max
  ``605.2 ms``

This makes ``mixed`` the most useful Stage 1 benchmark for proving that batched
compute dispatch and batched completion handling reduce scheduler pressure.

Fourth, ``tiny_chain`` and ``fanout_fanin`` show much lower queue delay than
``mixed`` and somewhat better throughput. That suggests the current scheduler is
not equally stressed by every small-task topology; the worst case is a bursty
mixed-duration workload that keeps pushing work back through central control
paths.

How to read Stage 1 progress against this baseline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If Stage 1 is working, the first numbers that should improve are:

- ``average_dispatch_batch_size`` should rise above ``1.0``
- ``average_completion_batch_size`` should rise above ``1.0``
- ``compute_task_messages_total`` should grow more slowly than task count
- ``task_finished_messages_total`` should grow more slowly than task count
- ``tasks_per_s`` should increase on ``independent_tiny`` and ``mixed``
- ``scheduler_cpu_mean`` should decrease for the same scenario size
- ``queue_delay_avg_ms`` should fall, especially on ``mixed``

If batching is implemented but these numbers do not move, then the change is
not attacking the real control-path cost.

Stage 1: Compute and completion batching
----------------------------------------

Add semantic batch messages between scheduler and worker.

Scheduler side:

- batch runnable tasks destined for the same worker
- preserve existing per-task state invariants initially
- keep single-task dispatch available as fallback

Worker side:

- accept ``ComputeTaskBatch`` alongside existing task messages
- emit ``TaskFinishedBatch`` on a short time or count threshold

Exit criteria:

- lower scheduler CPU on tiny-task benchmarks
- no behavior change in retries, cancellation, or diagnostics

Current implementation status
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stage 1 now exists behind configuration flags and preserves the single-message
path as the default.

Scheduler-side flags:

- ``distributed.scheduler.batching.compute``

Worker-side flags:

- ``distributed.worker.batching.task-finished``
- ``distributed.worker.batching.task-finished-size``
- ``distributed.worker.batching.task-finished-interval``

Implementation shape:

- the scheduler packs contiguous per-worker ``compute-task`` messages into
  ``compute-task-batch`` in ``Scheduler.send_all()``
- the worker accepts ``compute-task-batch`` and expands it into normal
  ``ComputeTaskEvent`` stimuli
- the worker buffers ``task-finished`` messages and emits
  ``task-finished-batch``

End-to-end comparison
~~~~~~~~~~~~~~~~~~~~~

The following commands show the current branch behavior on real benchmark runs
using ordinary Dask Distributed workloads with different feature combinations.

Baseline, no hybrid features:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario independent_tiny \
     --tasks 200

Fast path enabled:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario independent_tiny \
     --tasks 200 \
     --enable-tiny-task-fastpath \
     --tiny-task-fastpath-duration 10ms

Fast path plus single-worker leases:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario independent_tiny \
     --tasks 200 \
     --n-workers 1 \
     --threads-per-worker 1 \
     --enable-tiny-task-fastpath \
     --tiny-task-fastpath-duration 10ms \
     --enable-single-worker-leases \
     --single-worker-lease-task-budget 8 \
     --single-worker-lease-duration 10ms

Layered tiny graph with local successor retention:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario layered_tiny \
     --tasks 64 \
     --n-workers 1 \
     --threads-per-worker 1 \
     --worker-saturation 2.0 \
     --enable-tiny-task-fastpath \
     --tiny-task-fastpath-duration 10ms \
     --enable-single-worker-leases \
     --single-worker-lease-task-budget 8 \
     --single-worker-lease-duration 10ms \
     --enable-local-successor \
     --local-successor-task-budget 2

Observed results on this branch:

.. list-table::
   :header-rows: 1

   * - Scenario
     - Tasks/s
     - Compute messages
     - Dispatch batch
     - Tiny fast-path tasks
     - Lease tasks
     - Local successor tasks
   * - Baseline ``independent_tiny``
     - ``23.36``
     - ``200``
     - ``1.00``
     - ``0``
     - ``0``
     - ``0``
   * - Fast path ``independent_tiny``
     - ``25.20``
     - ``200``
     - ``1.00``
     - ``150``
     - ``0``
     - ``0``
   * - Fast path + lease ``independent_tiny``
     - ``30.31``
     - ``38``
     - ``5.26``
     - ``4``
     - ``191``
     - ``0``
   * - ``layered_tiny`` with local successor
     - ``114.30``
     - ``138``
     - ``1.39``
     - ``10``
     - ``62``
     - ``16``

Interpretation:

- the user-facing Dask API stays unchanged
- the fast path improves tiny-task admission without changing message shape
- leases are the strongest current reduction in scheduler dispatch traffic
- local successor placement activates only on dependency-layered workloads
- the scheduler accepts ``task-finished-batch`` and replays the existing
  per-task completion state transitions

Focused validation:

- scheduler wire-level batch test:
  ``distributed/tests/test_scheduler.py::test_compute_task_batch_wire_message``
- worker wire-level batch test:
  ``distributed/tests/test_worker.py::test_task_finished_batch_wire_message``
- Prometheus baseline and batched metric tests:
  ``distributed/http/scheduler/tests/test_scheduler_http.py``

Example benchmark run with Stage 1 enabled:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario independent_tiny \
     --tasks 200 \
     --enable-compute-batching \
     --enable-task-finished-batching \
     --task-finished-batch-size 8 \
     --task-finished-batch-interval 2ms

Stage 2: Tiny-task fast path
----------------------------

Add an internal fast path for the common small-task case.

Admission rules should stay strict:

- no actors
- no resource restrictions beyond default CPU slots
- no unusual annotations
- no worker-specific restrictions
- no retry path in progress
- dependencies already local or absent

Exit criteria:

- p95 latency improvement on tiny tasks
- no regression in state-machine validation

Current implementation status
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stage 2 now exists behind scheduler configuration flags.

Scheduler-side flags:

- ``distributed.scheduler.fast-path.enabled``
- ``distributed.scheduler.fast-path.duration``

Implementation shape:

- the scheduler admits only strict candidates to the fast path:
  - no actors
  - no retries
  - no annotations
  - no worker, host, or resource restrictions
  - estimated task duration at or below the configured threshold
- the scheduler only uses the fast path when an idle worker slot is already
  available
- tasks with dependencies are only admitted if at least one currently idle
  worker already holds all dependencies
- admitted tasks bypass the heavier worker-decision path and are dispatched
  directly through the existing ``_add_to_processing()`` flow

Observability:

- Prometheus exposes ``dask_scheduler_tiny_fastpath_tasks_total``
- the benchmark harness exposes ``tiny_fastpath_tasks_total``

Focused validation:

- scheduler fast-path positive test:
  ``distributed/tests/test_scheduler.py::test_tiny_task_fastpath_counter``
- scheduler fast-path restriction gate test:
  ``distributed/tests/test_scheduler.py::test_tiny_task_fastpath_rejects_worker_restrictions``
- Prometheus exposure test:
  ``distributed/http/scheduler/tests/test_scheduler_http.py::test_prometheus_scheduler_tiny_fastpath_counter``

Example benchmark run with Stage 2 enabled:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario independent_tiny \
     --tasks 200 \
     --enable-tiny-task-fastpath \
     --tiny-task-fastpath-duration 10ms

Stage 3: Single-worker leases
-----------------------------

Add scheduler-issued short-lived leases for one worker at a time.

Current implementation status:

- Scheduler-side single-worker lease issuance is wired behind
  ``distributed.scheduler.lease.*`` config.
- Workers accept ``compute-task-lease`` messages and attribute received leased work.
- Prometheus counters are exposed for lease issuance and leased task count.
- Focused unit and Prometheus coverage is passing.
- The earlier process-based benchmark hang was fixed by making lease formation
  atomic before transitioning queued tasks to ``processing``.
- The process-based benchmark harness now completes with Stage 3 enabled.

Focused verification:

- ``distributed/tests/test_scheduler.py::test_single_worker_lease_counters``
- ``distributed/tests/test_scheduler.py::test_single_worker_lease_fallback_does_not_strand_tasks``
- ``distributed/http/scheduler/tests/test_scheduler_http.py::test_prometheus_scheduler_single_worker_lease_counters``

Current focused status:

.. code-block:: text

   10 passed

Example benchmark run with Stage 3 enabled:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario independent_tiny \
     --tasks 200 \
     --n-workers 1 \
     --threads-per-worker 1 \
     --enable-tiny-task-fastpath \
     --enable-single-worker-leases \
     --single-worker-lease-task-budget 8 \
     --single-worker-lease-duration 10ms

Observed result highlights:

- ``single_worker_leases_issued_total = 28``
- ``single_worker_lease_tasks_total = 195``
- ``compute_task_messages_total = 33``
- ``average_dispatch_batch_size = 6.06``
- ``tasks_per_s = 56.6``

Scheduler responsibilities:

- determine lease eligibility
- define a bounded envelope
- issue, revoke, and expire leases
- attribute all leased work in logs and traces

Worker responsibilities:

- consume leased tasks with existing local ready queues
- batch progress and completions
- stop launching new leased tasks after revocation

Initial restrictions:

- no actors
- no worker-group leases
- no cross-worker local placement
- no bypass of scheduler retry logic

Exit criteria:

- reduced scheduler message rate on tiny-task frontiers
- equal or better fairness within measured tolerance
- correct recovery after worker loss or lease expiration

Stage 4: Local successor placement
----------------------------------

Allow a leased worker to retain tiny successors locally under a strict envelope.

Current implementation status:

- Scheduler-side local successor placement is wired behind
  ``distributed.scheduler.local-successor.*`` config.
- Placement is strict: direct successors only, tiny-task eligibility only,
  same-worker only, dependency-local only, and bounded by a per-finish task budget.
- The scheduler remains authoritative; local successor retention uses explicit
  scheduler-approved worker placement rather than worker-side autonomous spawning.
- Prometheus exposes ``dask_scheduler_local_successor_tasks_total``.

Focused verification:

- ``distributed/tests/test_scheduler.py::test_local_successor_counter``
- ``distributed/http/scheduler/tests/test_scheduler_http.py::test_prometheus_scheduler_local_successor_counter``

Current focused status:

.. code-block:: text

   12 passed

Example benchmark run with Stage 4 enabled:

.. code-block:: bash

   .venv/bin/python benchmarks/stage0_control_path_benchmark.py \
     --scenario layered_tiny \
     --tasks 64 \
     --n-workers 1 \
     --threads-per-worker 1 \
     --worker-saturation 2.0 \
     --enable-tiny-task-fastpath \
     --enable-single-worker-leases \
     --single-worker-lease-task-budget 8 \
     --single-worker-lease-duration 10ms \
     --enable-local-successor \
     --local-successor-task-budget 2

Observed result highlights:

- ``single_worker_leases_issued_total = 8``
- ``single_worker_lease_tasks_total = 62``
- ``local_successor_tasks_total = 24``
- ``average_dispatch_batch_size = 1.39``
- ``tasks_per_s = 111.9``

Requirements:

- scheduler must pre-approve the envelope
- dependencies must already be local
- restrictions and priorities must remain valid
- all successor tasks must remain visible in scheduler state

Exit criteria:

- lower idle gaps on short chains
- no hidden or untraceable worker-local work

Stage 5: Experimental worker-group leases
-----------------------------------------

This stage should remain experimental until the earlier stages are proven.

Potential benefits:

- better handling of locality-heavy frontiers spanning a small worker set
- less central involvement in tightly coupled small-task bursts

Main risk:

- fairness and debuggability can degrade quickly if this behaves like a second
  distributed scheduler

What Not To Do Early
--------------------

- do not redesign the P2P shuffle stack first
- do not introduce a new transport subsystem first
- do not add autotuning before the delegated execution semantics are stable
- do not expand lease scope to actors or unusual annotations in the first version

Suggested Code Entry Points
---------------------------

- ``distributed/scheduler.py``
  - queue opening
  - task finish handling
  - worker dispatch
- ``distributed/worker_state_machine.py``
  - compute task ingestion
  - ready-queue selection
  - dependency gather batching
- ``distributed/stealing.py``
  - fairness and overlap analysis with lease behavior
- ``distributed/batched.py``
  - reuse of stream-level batching

Rollout Strategy
----------------

- every stage behind configuration flags
- every stage benchmarked against baseline
- every stage validated with scheduler and worker state invariants enabled
- every stage accompanied by dashboard and trace visibility
