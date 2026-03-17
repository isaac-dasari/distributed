from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HARNESS = ROOT / "benchmarks" / "stage0_control_path_benchmark.py"


PROFILES: dict[str, list[str]] = {
    "baseline": [],
    "fastpath": [
        "--enable-tiny-task-fastpath",
        "--tiny-task-fastpath-duration",
        "10ms",
    ],
    "lease": [
        "--n-workers",
        "1",
        "--threads-per-worker",
        "1",
        "--enable-tiny-task-fastpath",
        "--tiny-task-fastpath-duration",
        "10ms",
        "--enable-single-worker-leases",
        "--single-worker-lease-task-budget",
        "8",
        "--single-worker-lease-duration",
        "10ms",
    ],
    "collective": [
        "--n-workers",
        "1",
        "--threads-per-worker",
        "1",
        "--enable-compute-batching",
        "--enable-task-finished-batching",
        "--enable-tiny-task-fastpath",
        "--tiny-task-fastpath-duration",
        "10ms",
        "--enable-single-worker-leases",
        "--single-worker-lease-task-budget",
        "8",
        "--single-worker-lease-duration",
        "10ms",
        "--enable-local-successor",
        "--local-successor-task-budget",
        "2",
    ],
}


def run_profile(profile: str, scenario: str, tasks: int) -> dict[str, object]:
    cmd = [
        sys.executable,
        str(HARNESS),
        "--scenario",
        scenario,
        "--tasks",
        str(tasks),
        *PROFILES[profile],
    ]
    completed = subprocess.run(
        cmd,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    return payload["results"][0]


def fmt_delta(new: float, old: float, suffix: str = "") -> str:
    if old == 0:
        return f"{new:.2f}{suffix}"
    pct = ((new - old) / old) * 100
    sign = "+" if pct >= 0 else ""
    return f"{new:.2f}{suffix} ({sign}{pct:.1f}%)"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare baseline Dask scheduling against hybrid scheduling profiles."
    )
    parser.add_argument("--scenario", default="independent_tiny")
    parser.add_argument("--tasks", type=int, default=200)
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=["baseline", "fastpath", "lease", "collective"],
        choices=sorted(PROFILES),
    )
    args = parser.parse_args()

    results = {profile: run_profile(profile, args.scenario, args.tasks) for profile in args.profiles}
    baseline = results[args.profiles[0]]

    print(f"Scenario: {args.scenario}")
    print(f"Tasks: {args.tasks}")
    print()
    print(
        f"{'profile':<12} {'time(s)':>10} {'tasks/s':>10} {'dispatch-msgs':>14} "
        f"{'finish-msgs':>13} {'fastpath':>10} {'lease-tasks':>12} {'local-succ':>11}"
    )
    for profile in args.profiles:
        r = results[profile]
        print(
            f"{profile:<12} "
            f"{r['wall_time_s']:>10.2f} "
            f"{r['tasks_per_s']:>10.2f} "
            f"{int(r['compute_task_messages_total']):>14} "
            f"{int(r['task_finished_messages_total']):>13} "
            f"{int(r['tiny_fastpath_tasks_total']):>10} "
            f"{int(r['single_worker_lease_tasks_total']):>12} "
            f"{int(r['local_successor_tasks_total']):>11}"
        )

    print()
    print("Deltas vs baseline")
    for profile in args.profiles[1:]:
        r = results[profile]
        print(f"- {profile}")
        print(f"  tasks/s: {fmt_delta(r['tasks_per_s'], baseline['tasks_per_s'])}")
        print(f"  wall time: {fmt_delta(r['wall_time_s'], baseline['wall_time_s'], 's')}")
        print(
            f"  compute messages: {fmt_delta(r['compute_task_messages_total'], baseline['compute_task_messages_total'])}"
        )
        print(
            f"  finished messages: {fmt_delta(r['task_finished_messages_total'], baseline['task_finished_messages_total'])}"
        )
        print(
            f"  transitions/task: {fmt_delta(r['transitions_per_task'], baseline['transitions_per_task'])}"
        )


if __name__ == "__main__":
    main()
