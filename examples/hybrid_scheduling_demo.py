from __future__ import annotations

import argparse
import json
from operator import neg

import dask

from distributed import Client, LocalCluster


def square(x: int) -> int:
    return x * x


def inc(x: int) -> int:
    return x + 1


def build_config(args: argparse.Namespace) -> dict[str, object]:
    return {
        "distributed.scheduler.batching.compute": args.enable_compute_batching,
        "distributed.scheduler.fast-path.enabled": args.enable_fastpath,
        "distributed.scheduler.fast-path.duration": args.fastpath_duration,
        "distributed.scheduler.lease.enabled": args.enable_leases,
        "distributed.scheduler.lease.task-budget": args.lease_task_budget,
        "distributed.scheduler.lease.duration": args.lease_duration,
        "distributed.scheduler.local-successor.enabled": args.enable_local_successor,
        "distributed.scheduler.local-successor.task-budget": args.local_successor_task_budget,
        "distributed.worker.batching.task-finished": args.enable_task_finished_batching,
        "distributed.worker.batching.task-finished-size": args.task_finished_batch_size,
        "distributed.worker.batching.task-finished-interval": args.task_finished_batch_interval,
    }


def metrics_snapshot(client: Client) -> dict[str, object]:
    def read_metrics(dask_scheduler):  # type: ignore[no-untyped-def]
        return {
            "compute_task_messages_total": dask_scheduler.compute_task_messages_total,
            "compute_task_dispatches_total": dask_scheduler.compute_task_dispatches_total,
            "task_finished_messages_total": dask_scheduler.task_finished_messages_total,
            "task_finished_tasks_total": dask_scheduler.task_finished_tasks_total,
            "tiny_fastpath_tasks_total": dask_scheduler.tiny_fastpath_tasks_total,
            "single_worker_leases_issued_total": dask_scheduler.single_worker_leases_issued_total,
            "single_worker_lease_tasks_total": dask_scheduler.single_worker_lease_tasks_total,
            "local_successor_tasks_total": dask_scheduler.local_successor_tasks_total,
        }

    return client.run_on_scheduler(read_metrics)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a small Dask.distributed workload with hybrid scheduling flags."
    )
    parser.add_argument("--tasks", type=int, default=200)
    parser.add_argument("--n-workers", type=int, default=1)
    parser.add_argument("--threads-per-worker", type=int, default=1)
    parser.add_argument("--enable-compute-batching", action="store_true")
    parser.add_argument("--enable-task-finished-batching", action="store_true")
    parser.add_argument("--task-finished-batch-size", type=int, default=8)
    parser.add_argument("--task-finished-batch-interval", default="2ms")
    parser.add_argument("--enable-fastpath", action="store_true")
    parser.add_argument("--fastpath-duration", default="10ms")
    parser.add_argument("--enable-leases", action="store_true")
    parser.add_argument("--lease-task-budget", type=int, default=8)
    parser.add_argument("--lease-duration", default="10ms")
    parser.add_argument("--enable-local-successor", action="store_true")
    parser.add_argument("--local-successor-task-budget", type=int, default=2)
    args = parser.parse_args()

    config = build_config(args)

    with dask.config.set(config):
        cluster = LocalCluster(
            n_workers=args.n_workers,
            threads_per_worker=args.threads_per_worker,
            processes=True,
            dashboard_address=":0",
        )
        client = Client(cluster)
        try:
            roots = client.map(square, range(args.tasks))
            mids = client.map(inc, roots)
            tails = client.map(neg, mids)
            total = client.submit(sum, tails)
            result = total.result()

            output = {
                "config": config,
                "result": result,
                "sample": client.gather(tails[:5]),
                "metrics": metrics_snapshot(client),
            }
            print(json.dumps(output, indent=2, sort_keys=True))
        finally:
            client.close()
            cluster.close()


if __name__ == "__main__":
    main()
