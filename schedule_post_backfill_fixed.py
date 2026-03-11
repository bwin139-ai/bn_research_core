#!/usr/bin/env python3
import argparse
import importlib.util
import json
import sys
from pathlib import Path


def load_schedule_module(repo_root: Path):
    sched_path = repo_root / "schedule_backtests.py"
    if not sched_path.exists():
        raise FileNotFoundError(f"schedule_backtests.py not found: {sched_path}")

    module_name = "schedule_backtests_backfill"
    spec = importlib.util.spec_from_file_location(module_name, str(sched_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to build import spec for {sched_path}")

    mod = importlib.util.module_from_spec(spec)
    # Python 3.12 dataclass 依赖模块已注册到 sys.modules
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill scheduler post-run artifacts without rerunning batches")
    ap.add_argument("--summary-json", required=True, help="Path to scheduler summary json")
    ap.add_argument("--repo-root", required=True, help="Repository root, e.g. /root/bn_research_core")
    ap.add_argument("--post-merge", action="store_true", help="Run merge/viz/_ALL summary generation")
    ap.add_argument("--build-equity", action="store_true", help="Run equity curve generation")
    args = ap.parse_args()

    summary_path = Path(args.summary_json)
    repo_root = Path(args.repo_root)

    if not summary_path.exists():
        raise FileNotFoundError(f"summary json not found: {summary_path}")
    if not repo_root.exists():
        raise FileNotFoundError(f"repo root not found: {repo_root}")

    mod = load_schedule_module(repo_root)

    with summary_path.open("r", encoding="utf-8") as f:
        scheduler_summary = json.load(f)

    tasks = []
    for item in scheduler_summary.get("tasks", []):
        tasks.append(
            mod.Task(
                batch_id=item["batch_id"],
                start_iso=item["start"],
                end_iso=item["end"],
                run_id=item["run_id"],
                log_path=item["log_path"],
                cmd=item["cmd"],
            )
        )

    finished = scheduler_summary.get("finished", [])

    ns = argparse.Namespace(
        strategy=scheduler_summary["strategy"],
        start=scheduler_summary["start"],
        end=scheduler_summary["end"],
        batch_days=scheduler_summary["batch_days"],
        max_parallel=scheduler_summary["max_parallel"],
        python_bin=scheduler_summary["python_bin"],
        config=scheduler_summary["config"],
        out_dir=scheduler_summary["out_dir"],
        logs_dir=scheduler_summary["logs_dir"],
        # post-run 所需参数
        post_merge=bool(args.post_merge),
        build_equity=bool(args.build_equity),
        equity_script="core/analysis/top1_equity_curve.py",
        kline_root="data/klines_1m",
        equity_initial=100.0,
        equity_fee_side=0.0005,
    )

    scheduler_name = summary_path.stem
    if scheduler_name.startswith("scheduler_"):
        scheduler_name = scheduler_name[len("scheduler_"):]

    scheduler_log = Path(ns.logs_dir) / f"scheduler.{scheduler_name}.console.log"

    artifacts, errors = mod.run_post_processing(
        args=ns,
        scheduler_name=scheduler_name,
        tasks=tasks,
        finished=finished,
        scheduler_summary=scheduler_summary,
        scheduler_log=scheduler_log,
    )

    scheduler_summary["artifacts"] = artifacts
    scheduler_summary["artifacts_errors"] = errors

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(scheduler_summary, f, ensure_ascii=False, indent=2)

    print(f"Wrote updated scheduler summary: {summary_path}")
    if artifacts:
        print("Artifacts:")
        for k, v in artifacts.items():
            print(f"  {k}: {v}")
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  {e}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
