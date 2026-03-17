from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "examples" / "hybrid_scheduling_real_cases.py"


PROFILES: dict[str, list[str]] = {
    "baseline": [],
    "hybrid": [
        "--enable-compute-batching",
        "--enable-task-finished-batching",
        "--enable-fastpath",
        "--enable-leases",
        "--enable-local-successor",
    ],
}


def run_profile(profile: str, args: argparse.Namespace) -> dict[str, object]:
    cmd = [
        sys.executable,
        str(SCRIPT),
        "--blocks",
        str(args.blocks),
        "--rows",
        str(args.rows),
        "--cols",
        str(args.cols),
        "--array-size",
        str(args.array_size),
        "--array-chunks",
        str(args.array_chunks),
        "--n-workers",
        str(args.n_workers),
        "--threads-per-worker",
        str(args.threads_per_worker),
        *PROFILES[profile],
    ]
    completed = subprocess.run(
        cmd,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def metric_value(result: dict[str, object], key: str) -> float:
    metrics = result["metrics"]
    assert isinstance(metrics, dict)
    return float(metrics[key])


def fmt_pct(new: float, old: float) -> str:
    if old == 0:
        return "n/a"
    pct = ((new - old) / old) * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare realistic Dask workloads with baseline and hybrid scheduling configs."
    )
    parser.add_argument("--blocks", type=int, default=12)
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--cols", type=int, default=32)
    parser.add_argument("--array-size", type=int, default=1024)
    parser.add_argument("--array-chunks", type=int, default=256)
    parser.add_argument("--n-workers", type=int, default=2)
    parser.add_argument("--threads-per-worker", type=int, default=1)
    args = parser.parse_args()

    baseline = run_profile("baseline", args)
    hybrid = run_profile("hybrid", args)

    by_name = {
        payload["name"]: payload
        for payload in baseline["results"]  # type: ignore[index]
    }
    by_name_hybrid = {
        payload["name"]: payload
        for payload in hybrid["results"]  # type: ignore[index]
    }

    print("Real workload comparison")
    print()
    print(
        f"{'case':<28} {'base time':>10} {'hybrid time':>12} {'delta':>9} "
        f"{'base msgs':>10} {'hybrid msgs':>12} {'fastpath':>10} {'lease':>8}"
    )
    for name in by_name:
        base = by_name[name]
        hyb = by_name_hybrid[name]
        base_time = float(base["wall_time_s"])
        hyb_time = float(hyb["wall_time_s"])
        base_msgs = metric_value(base, "compute_task_messages_total")
        hyb_msgs = metric_value(hyb, "compute_task_messages_total")
        fastpath = int(metric_value(hyb, "tiny_fastpath_tasks_total"))
        lease = int(metric_value(hyb, "single_worker_lease_tasks_total"))
        print(
            f"{name:<28} "
            f"{base_time:>10.2f} "
            f"{hyb_time:>12.2f} "
            f"{fmt_pct(hyb_time, base_time):>9} "
            f"{int(base_msgs):>10} "
            f"{int(hyb_msgs):>12} "
            f"{fastpath:>10} "
            f"{lease:>8}"
        )

    print()
    print("Details")
    for name in by_name:
        base = by_name[name]
        hyb = by_name_hybrid[name]
        print(f"- {name}")
        print(f"  baseline wall_time_s: {float(base['wall_time_s']):.3f}")
        print(f"  hybrid wall_time_s: {float(hyb['wall_time_s']):.3f}")
        print(
            f"  compute messages: {int(metric_value(base, 'compute_task_messages_total'))} -> "
            f"{int(metric_value(hyb, 'compute_task_messages_total'))}"
        )
        print(
            f"  completion messages: {int(metric_value(base, 'task_finished_messages_total'))} -> "
            f"{int(metric_value(hyb, 'task_finished_messages_total'))}"
        )
        print(
            f"  tiny fastpath tasks: {int(metric_value(hyb, 'tiny_fastpath_tasks_total'))}"
        )
        print(
            f"  lease tasks: {int(metric_value(hyb, 'single_worker_lease_tasks_total'))}"
        )
        print(
            f"  local successor tasks: {int(metric_value(hyb, 'local_successor_tasks_total'))}"
        )


if __name__ == "__main__":
    main()
