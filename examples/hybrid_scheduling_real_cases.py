from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass

import dask
import dask.array as da
import numpy as np

from distributed import Client, LocalCluster


def make_config(args: argparse.Namespace) -> dict[str, object]:
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


def scheduler_metrics(client: Client) -> dict[str, object]:
    def _read(dask_scheduler):  # type: ignore[no-untyped-def]
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

    return client.run_on_scheduler(_read)


def diff_metrics(before: dict[str, object], after: dict[str, object]) -> dict[str, object]:
    return {k: after[k] - before[k] for k in before}


@dataclass
class CaseResult:
    name: str
    wall_time_s: float
    metrics: dict[str, object]
    sample: object


def load_block(seed: int, rows: int, cols: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.random((rows, cols), dtype=np.float64)


def normalize_block(block: np.ndarray) -> np.ndarray:
    centered = block - block.mean(axis=0, keepdims=True)
    scale = centered.std(axis=0, keepdims=True) + 1e-9
    return centered / scale


def score_block(block: np.ndarray) -> np.ndarray:
    return (block @ block.T).mean(axis=1)


def futures_numpy_case(client: Client, blocks: int, rows: int, cols: int) -> CaseResult:
    before = scheduler_metrics(client)
    start = time.perf_counter()

    sources = [client.submit(load_block, i, rows, cols, pure=False) for i in range(blocks)]
    normalized = client.map(normalize_block, sources)
    scored = client.map(score_block, normalized)
    total = client.submit(
        lambda parts: float(sum(float(np.sum(p)) for p in parts)),
        scored,
    )
    sample = client.gather(scored[:2])
    result = total.result()

    wall = time.perf_counter() - start
    after = scheduler_metrics(client)
    return CaseResult(
        name="futures_numpy_pipeline",
        wall_time_s=wall,
        metrics=diff_metrics(before, after),
        sample={
            "block_count": blocks,
            "result": result,
            "sample_shapes": [list(arr.shape) for arr in sample],
        },
    )


def dask_array_case(client: Client, size: int, chunks: int) -> CaseResult:
    before = scheduler_metrics(client)
    start = time.perf_counter()

    x = da.random.random((size, size), chunks=(chunks, chunks))
    y = ((x - x.mean(axis=1)[:, None]) / (x.std(axis=1)[:, None] + 1e-9)).persist()
    z = (y.T @ y).sum(axis=1)
    result = client.compute(z.mean()).result()

    wall = time.perf_counter() - start
    after = scheduler_metrics(client)
    return CaseResult(
        name="dask_array_linear_algebra",
        wall_time_s=wall,
        metrics=diff_metrics(before, after),
        sample={
            "shape": [size, size],
            "chunks": [chunks, chunks],
            "result": float(result),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run realistic Dask futures and Dask Array workloads with hybrid scheduling config."
    )
    parser.add_argument("--n-workers", type=int, default=2)
    parser.add_argument("--threads-per-worker", type=int, default=1)
    parser.add_argument("--blocks", type=int, default=24)
    parser.add_argument("--rows", type=int, default=256)
    parser.add_argument("--cols", type=int, default=64)
    parser.add_argument("--array-size", type=int, default=2048)
    parser.add_argument("--array-chunks", type=int, default=256)
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

    config = make_config(args)

    with dask.config.set(config):
        cluster = LocalCluster(
            n_workers=args.n_workers,
            threads_per_worker=args.threads_per_worker,
            processes=True,
            dashboard_address=":0",
        )
        client = Client(cluster)
        try:
            results = [
                asdict(futures_numpy_case(client, args.blocks, args.rows, args.cols)),
                asdict(dask_array_case(client, args.array_size, args.array_chunks)),
            ]
            print(json.dumps({"config": config, "results": results}, indent=2, sort_keys=True))
        finally:
            client.close()
            cluster.close()


if __name__ == "__main__":
    main()
