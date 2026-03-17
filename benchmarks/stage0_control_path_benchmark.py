from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import dask
from distributed import Client, LocalCluster, as_completed


def tiny_sleep(x: int, delay_ms: float) -> int:
    time.sleep(delay_ms / 1000.0)
    return x + 1


@dataclass
class ScenarioResult:
    name: str
    task_count: int
    wall_time_s: float
    tasks_per_s: float
    latency_p50_ms: float
    latency_p95_ms: float
    duration_bucket_ms: str
    scheduler_cpu_mean: float
    scheduler_cpu_max: float
    transition_count: int
    transitions_per_task: float
    compute_task_messages_total: int
    compute_task_dispatches_total: int
    average_dispatch_batch_size: float
    task_finished_messages_total: int
    task_finished_tasks_total: int
    average_completion_batch_size: float
    tiny_fastpath_tasks_total: int
    single_worker_leases_issued_total: int
    single_worker_lease_tasks_total: int
    local_successor_tasks_total: int
    locality_hit_rate: float
    queue_delay_avg_ms: float
    queue_delay_max_ms: float
    queue_delay_samples_total: int


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    idx = (len(values) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    frac = idx - lo
    return values[lo] * (1 - frac) + values[hi] * frac


def scheduler_metrics(dask_scheduler: Any) -> dict[str, Any]:
    dask_scheduler.monitor.update()
    cpu_samples = list(dask_scheduler.monitor.quantities["cpu"])
    return {
        "transition_count": dask_scheduler.transition_counter,
        "compute_task_messages_total": dask_scheduler.compute_task_messages_total,
        "compute_task_dispatches_total": dask_scheduler.compute_task_dispatches_total,
        "compute_task_locality_hits_total": dask_scheduler.compute_task_locality_hits_total,
        "task_finished_messages_total": dask_scheduler.task_finished_messages_total,
        "task_finished_tasks_total": dask_scheduler.task_finished_tasks_total,
        "tiny_fastpath_tasks_total": dask_scheduler.tiny_fastpath_tasks_total,
        "single_worker_leases_issued_total": dask_scheduler.single_worker_leases_issued_total,
        "single_worker_lease_tasks_total": dask_scheduler.single_worker_lease_tasks_total,
        "local_successor_tasks_total": dask_scheduler.local_successor_tasks_total,
        "queue_delay_seconds_total": dask_scheduler.queue_delay_seconds_total,
        "queue_delay_max": dask_scheduler.queue_delay_max,
        "queue_delay_samples_total": dask_scheduler.queue_delay_samples_total,
        "scheduler_cpu_mean": statistics.fmean(cpu_samples) if cpu_samples else 0.0,
        "scheduler_cpu_max": max(cpu_samples) if cpu_samples else 0.0,
    }


def metric_delta(after: dict[str, Any], before: dict[str, Any], key: str) -> float:
    return float(after[key] - before[key])


def independent_tiny(client: Client, tasks: int, delay_ms: float) -> tuple[int, list[float], str]:
    submitted_at: dict[str, float] = {}
    futures = []
    for i in range(tasks):
        fut = client.submit(tiny_sleep, i, delay_ms, pure=False)
        submitted_at[fut.key] = time.perf_counter()
        futures.append(fut)

    latencies = []
    for fut in as_completed(futures):
        fut.result()
        latencies.append((time.perf_counter() - submitted_at[fut.key]) * 1000.0)
    return tasks, latencies, f"{delay_ms:g}"


def tiny_chain(client: Client, chains: int, chain_length: int, delay_ms: float) -> tuple[int, list[float], str]:
    final_futures = []
    submitted_at: dict[str, float] = {}
    task_count = 0
    for i in range(chains):
        fut = client.submit(tiny_sleep, i, delay_ms, pure=False)
        task_count += 1
        for _ in range(chain_length - 1):
            fut = client.submit(tiny_sleep, fut, delay_ms, pure=False)
            task_count += 1
        submitted_at[fut.key] = time.perf_counter()
        final_futures.append(fut)

    latencies = []
    for fut in as_completed(final_futures):
        fut.result()
        latencies.append((time.perf_counter() - submitted_at[fut.key]) * 1000.0)
    return task_count, latencies, f"{delay_ms:g}"


def layered_tiny(client: Client, tasks: int, delay_ms: float) -> tuple[int, list[float], str]:
    roots = client.map(tiny_sleep, range(tasks), [delay_ms] * tasks, pure=False)
    mids = client.map(tiny_sleep, roots, [delay_ms] * tasks, pure=False)
    finals = client.map(tiny_sleep, mids, [delay_ms] * tasks, pure=False)

    submitted_at = {fut.key: time.perf_counter() for fut in finals}
    latencies = []
    for fut in as_completed(finals):
        fut.result()
        latencies.append((time.perf_counter() - submitted_at[fut.key]) * 1000.0)
    return tasks * 3, latencies, f"{delay_ms:g}"


def fanout_fanin(client: Client, groups: int, fanout: int, delay_ms: float) -> tuple[int, list[float], str]:
    final_futures = []
    submitted_at: dict[str, float] = {}
    task_count = 0
    for i in range(groups):
        root = client.submit(tiny_sleep, i, delay_ms, pure=False)
        task_count += 1
        leaves = [client.submit(tiny_sleep, root, delay_ms, pure=False) for _ in range(fanout)]
        task_count += fanout
        fut = client.submit(sum, leaves, pure=False)
        task_count += 1
        submitted_at[fut.key] = time.perf_counter()
        final_futures.append(fut)

    latencies = []
    for fut in as_completed(final_futures):
        fut.result()
        latencies.append((time.perf_counter() - submitted_at[fut.key]) * 1000.0)
    return task_count, latencies, f"{delay_ms:g}"


def mixed_latency(client: Client, tasks: int, tiny_delay_ms: float, medium_delay_ms: float) -> tuple[int, list[float], str]:
    submitted_at: dict[str, float] = {}
    futures = []
    for i in range(tasks):
        delay_ms = tiny_delay_ms if i % 2 == 0 else medium_delay_ms
        fut = client.submit(tiny_sleep, i, delay_ms, pure=False, priority=0 if i % 2 == 0 else -1)
        submitted_at[fut.key] = time.perf_counter()
        futures.append(fut)

    latencies = []
    for fut in as_completed(futures):
        fut.result()
        latencies.append((time.perf_counter() - submitted_at[fut.key]) * 1000.0)
    return tasks, latencies, f"{tiny_delay_ms:g}-{medium_delay_ms:g}"


def multi_tenant_priority(client: Client, tasks: int, delay_ms: float) -> tuple[int, list[float], str]:
    submitted_at: dict[str, float] = {}
    futures = []
    for i in range(tasks):
        fut = client.submit(tiny_sleep, i, delay_ms, pure=False, priority=10 if i % 2 == 0 else 0)
        submitted_at[fut.key] = time.perf_counter()
        futures.append(fut)

    latencies = []
    for fut in as_completed(futures):
        fut.result()
        latencies.append((time.perf_counter() - submitted_at[fut.key]) * 1000.0)
    return tasks, latencies, f"{delay_ms:g}"


def run_scenario(
    client: Client,
    name: str,
    tasks: int,
    tiny_delay_ms: float,
    medium_delay_ms: float,
) -> ScenarioResult:
    before = client.run_on_scheduler(scheduler_metrics)
    start = time.perf_counter()

    if name == "independent_tiny":
        task_count, latencies, bucket = independent_tiny(client, tasks, tiny_delay_ms)
    elif name == "tiny_chain":
        task_count, latencies, bucket = tiny_chain(client, max(1, tasks // 10), 10, tiny_delay_ms)
    elif name == "layered_tiny":
        task_count, latencies, bucket = layered_tiny(client, tasks, tiny_delay_ms)
    elif name == "fanout_fanin":
        task_count, latencies, bucket = fanout_fanin(client, max(1, tasks // 8), 6, tiny_delay_ms)
    elif name == "mixed":
        task_count, latencies, bucket = mixed_latency(client, tasks, tiny_delay_ms, medium_delay_ms)
    elif name == "multi_tenant":
        task_count, latencies, bucket = multi_tenant_priority(client, tasks, tiny_delay_ms)
    else:
        raise ValueError(f"Unknown scenario: {name}")

    wall_time_s = time.perf_counter() - start
    after = client.run_on_scheduler(scheduler_metrics)
    metrics = {
        "transition_count": metric_delta(after, before, "transition_count"),
        "compute_task_messages_total": metric_delta(after, before, "compute_task_messages_total"),
        "compute_task_dispatches_total": metric_delta(after, before, "compute_task_dispatches_total"),
        "compute_task_locality_hits_total": metric_delta(after, before, "compute_task_locality_hits_total"),
        "task_finished_messages_total": metric_delta(after, before, "task_finished_messages_total"),
        "task_finished_tasks_total": metric_delta(after, before, "task_finished_tasks_total"),
        "tiny_fastpath_tasks_total": metric_delta(after, before, "tiny_fastpath_tasks_total"),
        "single_worker_leases_issued_total": metric_delta(after, before, "single_worker_leases_issued_total"),
        "single_worker_lease_tasks_total": metric_delta(after, before, "single_worker_lease_tasks_total"),
        "local_successor_tasks_total": metric_delta(after, before, "local_successor_tasks_total"),
        "queue_delay_seconds_total": metric_delta(after, before, "queue_delay_seconds_total"),
        "queue_delay_max": after["queue_delay_max"],
        "queue_delay_samples_total": metric_delta(after, before, "queue_delay_samples_total"),
        "scheduler_cpu_mean": after["scheduler_cpu_mean"],
        "scheduler_cpu_max": after["scheduler_cpu_max"],
    }
    locality_hit_rate = (
        metrics["compute_task_locality_hits_total"] / metrics["compute_task_dispatches_total"]
        if metrics["compute_task_dispatches_total"]
        else 0.0
    )
    average_dispatch_batch_size = (
        metrics["compute_task_dispatches_total"] / metrics["compute_task_messages_total"]
        if metrics["compute_task_messages_total"]
        else 0.0
    )
    average_completion_batch_size = (
        metrics["task_finished_tasks_total"] / metrics["task_finished_messages_total"]
        if metrics["task_finished_messages_total"]
        else 0.0
    )
    queue_delay_avg_ms = (
        (metrics["queue_delay_seconds_total"] / metrics["queue_delay_samples_total"]) * 1000.0
        if metrics["queue_delay_samples_total"]
        else 0.0
    )

    latencies.sort()
    return ScenarioResult(
        name=name,
        task_count=task_count,
        wall_time_s=wall_time_s,
        tasks_per_s=(task_count / wall_time_s) if wall_time_s else 0.0,
        latency_p50_ms=percentile(latencies, 0.5),
        latency_p95_ms=percentile(latencies, 0.95),
        duration_bucket_ms=bucket,
        scheduler_cpu_mean=metrics["scheduler_cpu_mean"],
        scheduler_cpu_max=metrics["scheduler_cpu_max"],
        transition_count=metrics["transition_count"],
        transitions_per_task=(metrics["transition_count"] / task_count) if task_count else 0.0,
        compute_task_messages_total=metrics["compute_task_messages_total"],
        compute_task_dispatches_total=metrics["compute_task_dispatches_total"],
        average_dispatch_batch_size=average_dispatch_batch_size,
        task_finished_messages_total=metrics["task_finished_messages_total"],
        task_finished_tasks_total=metrics["task_finished_tasks_total"],
        average_completion_batch_size=average_completion_batch_size,
        tiny_fastpath_tasks_total=int(metrics["tiny_fastpath_tasks_total"]),
        single_worker_leases_issued_total=int(metrics["single_worker_leases_issued_total"]),
        single_worker_lease_tasks_total=int(metrics["single_worker_lease_tasks_total"]),
        local_successor_tasks_total=int(metrics["local_successor_tasks_total"]),
        locality_hit_rate=locality_hit_rate,
        queue_delay_avg_ms=queue_delay_avg_ms,
        queue_delay_max_ms=metrics["queue_delay_max"] * 1000.0,
        queue_delay_samples_total=metrics["queue_delay_samples_total"],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 0 control-path benchmark for Dask Distributed")
    parser.add_argument(
        "--scenario",
        action="append",
        choices=["independent_tiny", "tiny_chain", "layered_tiny", "fanout_fanin", "mixed", "multi_tenant", "all"],
        default=None,
        help="Scenario(s) to run",
    )
    parser.add_argument("--tasks", type=int, default=200, help="Approximate task count per scenario")
    parser.add_argument("--n-workers", type=int, default=2)
    parser.add_argument("--threads-per-worker", type=int, default=1)
    parser.add_argument("--worker-saturation", type=float, default=None)
    parser.add_argument("--tiny-delay-ms", type=float, default=2.0)
    parser.add_argument("--medium-delay-ms", type=float, default=20.0)
    parser.add_argument("--enable-compute-batching", action="store_true")
    parser.add_argument("--enable-task-finished-batching", action="store_true")
    parser.add_argument("--task-finished-batch-size", type=int, default=8)
    parser.add_argument("--task-finished-batch-interval", type=str, default="2ms")
    parser.add_argument("--enable-tiny-task-fastpath", action="store_true")
    parser.add_argument("--tiny-task-fastpath-duration", type=str, default="10ms")
    parser.add_argument("--enable-single-worker-leases", action="store_true")
    parser.add_argument("--single-worker-lease-task-budget", type=int, default=8)
    parser.add_argument("--single-worker-lease-duration", type=str, default="10ms")
    parser.add_argument("--enable-local-successor", action="store_true")
    parser.add_argument("--local-successor-task-budget", type=int, default=4)
    parser.add_argument("--output", type=Path, default=None, help="Write JSON results to this path")
    args = parser.parse_args()

    scenarios = args.scenario or ["all"]
    if "all" in scenarios:
        scenarios = ["independent_tiny", "tiny_chain", "layered_tiny", "fanout_fanin", "mixed", "multi_tenant"]

    config = {
        "distributed.scheduler.batching.compute": args.enable_compute_batching,
        "distributed.worker.batching.task-finished": args.enable_task_finished_batching,
        "distributed.worker.batching.task-finished-size": args.task_finished_batch_size,
        "distributed.worker.batching.task-finished-interval": args.task_finished_batch_interval,
        "distributed.scheduler.fast-path.enabled": args.enable_tiny_task_fastpath,
        "distributed.scheduler.fast-path.duration": args.tiny_task_fastpath_duration,
        "distributed.scheduler.lease.enabled": args.enable_single_worker_leases,
        "distributed.scheduler.lease.task-budget": args.single_worker_lease_task_budget,
        "distributed.scheduler.lease.duration": args.single_worker_lease_duration,
        "distributed.scheduler.local-successor.enabled": args.enable_local_successor,
        "distributed.scheduler.local-successor.task-budget": args.local_successor_task_budget,
        "distributed.scheduler.default-task-durations.tiny_sleep": f"{args.tiny_delay_ms}ms",
    }
    if args.worker_saturation is not None:
        config["distributed.scheduler.worker-saturation"] = args.worker_saturation

    results = []
    with dask.config.set(config):
        for name in scenarios:
            with LocalCluster(
                n_workers=args.n_workers,
                threads_per_worker=args.threads_per_worker,
                processes=True,
                dashboard_address=None,
            ) as cluster, Client(cluster) as client:
                results.append(
                    asdict(
                        run_scenario(
                            client=client,
                            name=name,
                            tasks=args.tasks,
                            tiny_delay_ms=args.tiny_delay_ms,
                            medium_delay_ms=args.medium_delay_ms,
                        )
                    )
                )

    payload = {"results": results}
    text = json.dumps(payload, indent=2)
    if args.output:
        args.output.write_text(text)
    print(text)


if __name__ == "__main__":
    main()
