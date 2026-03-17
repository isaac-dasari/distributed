Hybrid Scheduling Usage
=======================

This page shows how to exercise the hybrid scheduling features on this branch
using normal ``dask.distributed`` user code.

The important point is that the Python API does not change. Users still write
the usual ``Client.submit()``, ``Client.map()``, ``Client.gather()``, and
``Client.persist()`` code. The new behavior is enabled through configuration.

Original Dask.distributed Usage
-------------------------------

This is standard Dask usage with no hybrid scheduling features enabled:

.. code-block:: python

   from distributed import Client, LocalCluster

   cluster = LocalCluster(n_workers=2, threads_per_worker=1)
   client = Client(cluster)

   def square(x):
       return x * x

   futures = client.map(square, range(200))
   results = client.gather(futures)

This is the baseline behavior:

- one scheduler compute message per task
- one completion report per task
- no tiny-task fast path
- no leases
- no local successor retention

Same User Code with Hybrid Scheduling Enabled
---------------------------------------------

The same user program can run with the new features enabled through config:

.. code-block:: python

   import dask

   from distributed import Client, LocalCluster

   config = {
       "distributed.scheduler.batching.compute": True,
       "distributed.scheduler.fast-path.enabled": True,
       "distributed.scheduler.fast-path.duration": "10ms",
       "distributed.scheduler.lease.enabled": True,
       "distributed.scheduler.lease.task-budget": 8,
       "distributed.scheduler.lease.duration": "10ms",
       "distributed.scheduler.local-successor.enabled": True,
       "distributed.scheduler.local-successor.task-budget": 2,
       "distributed.worker.batching.task-finished": True,
       "distributed.worker.batching.task-finished-size": 8,
       "distributed.worker.batching.task-finished-interval": "2ms",
   }

   with dask.config.set(config):
       cluster = LocalCluster(n_workers=1, threads_per_worker=1)
       client = Client(cluster)

       def square(x):
           return x * x

       futures = client.map(square, range(200))
       results = client.gather(futures)

The user-visible result is the same, but the scheduler and worker may now use:

- semantic compute batching
- tiny-task fast path
- bounded single-worker leases
- bounded local successor placement
- worker-side completion batching

Map, Submit, and Gather Example
-------------------------------

This mirrors the typical Dask ``map``/``submit`` documentation style:

.. code-block:: python

   import dask

   from distributed import Client, LocalCluster

   def square(x):
       return x ** 2

   def neg(x):
       return -x

   with dask.config.set(
       {
           "distributed.scheduler.fast-path.enabled": True,
           "distributed.scheduler.fast-path.duration": "10ms",
           "distributed.scheduler.lease.enabled": True,
           "distributed.scheduler.lease.task-budget": 8,
           "distributed.scheduler.lease.duration": "10ms",
       }
   ):
       cluster = LocalCluster(n_workers=1, threads_per_worker=1)
       client = Client(cluster)

       A = client.map(square, range(200))
       B = client.map(neg, A)
       total = client.submit(sum, B)

       print(total.result())

Observe Metrics
---------------

This branch adds scheduler-side Prometheus counters that show whether the new
paths are activating:

- ``dask_scheduler_compute_task_messages_total``
- ``dask_scheduler_compute_task_dispatches_total``
- ``dask_scheduler_task_finished_messages_total``
- ``dask_scheduler_task_finished_tasks_total``
- ``dask_scheduler_tiny_fastpath_tasks_total``
- ``dask_scheduler_single_worker_leases_issued_total``
- ``dask_scheduler_single_worker_lease_tasks_total``
- ``dask_scheduler_local_successor_tasks_total``

If the scheduler is running with its dashboard or HTTP server, inspect them
with:

.. code-block:: bash

   curl http://127.0.0.1:8787/metrics | rg 'dask_scheduler_(compute_task|task_finished|tiny_fastpath|single_worker_lease|local_successor)'

Runnable Demo
-------------

This repository includes a runnable example script:

.. code-block:: bash

   .venv/bin/python examples/hybrid_scheduling_demo.py \
     --enable-compute-batching \
     --enable-fastpath \
     --enable-leases \
     --enable-local-successor \
     --enable-task-finished-batching

The script runs a small ``map``/``submit`` workload and prints the resulting
value plus the scheduler metrics snapshot so you can see the hybrid paths in
action.

Realistic Examples
------------------

The repository also includes a larger example script with two more realistic
workloads:

- a futures-style NumPy pipeline using ``submit``, ``map``, ``submit``, and
  ``gather``
- a Dask Array workload using ``persist`` and ``compute``

Run the baseline version:

.. code-block:: bash

   .venv/bin/python examples/hybrid_scheduling_real_cases.py

Run the same workloads with the hybrid features enabled:

.. code-block:: bash

   .venv/bin/python examples/hybrid_scheduling_real_cases.py \
     --enable-compute-batching \
     --enable-task-finished-batching \
     --enable-fastpath \
     --enable-leases \
     --enable-local-successor

The output includes:

- total wall-clock time per workload
- scheduler dispatch and completion message counts
- fast-path task count
- leased task count
- local successor task count

Futures-style NumPy pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The futures example uses normal ``dask.distributed`` APIs:

.. code-block:: python

   sources = [client.submit(load_block, i, rows, cols, pure=False) for i in range(blocks)]
   normalized = client.map(normalize_block, sources)
   scored = client.map(score_block, normalized)
   total = client.submit(sum_scores, scored)
   sample = client.gather(scored[:2])
   result = total.result()

This is useful for seeing whether the hybrid scheduler paths help workloads
that are built directly from futures rather than from Dask collections.

Dask Array example
~~~~~~~~~~~~~~~~~~

The Dask Array example keeps the user code in the standard collection style:

.. code-block:: python

   x = da.random.random((size, size), chunks=(chunks, chunks))
   y = ((x - x.mean(axis=1)[:, None]) / (x.std(axis=1)[:, None] + 1e-9)).persist()
   z = (y.T @ y).sum(axis=1)
   result = client.compute(z.mean()).result()

This is useful for checking whether the new scheduler behavior activates
cleanly underneath collection-backed workloads, not just manual ``submit`` and
``map`` code.
