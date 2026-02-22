#!/usr/bin/env python3
"""Run full assessment workflow: pipeline + scalability profiler."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run_cmd(cmd: list[str], cwd: Path) -> int:
    print("Executing:", " ".join(cmd))
    return subprocess.call(cmd, cwd=str(cwd))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full water-bodies assessment workflow")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))
    parser.add_argument("--water-resource-url", type=str, default=None)
    parser.add_argument("--cv-parallelism", type=int, default=2)
    parser.add_argument("--partitions", type=str, default="2,4,8")
    parser.add_argument("--fixed-rows", type=int, default=100000)
    parser.add_argument("--rows-per-partition", type=int, default=30000)
    args = parser.parse_args()

    base = args.output_dir.resolve()

    pipeline_cmd = [
        sys.executable,
        "scripts/run_data_platform.py",
        "--dataset",
        "water_bodies",
        "--output-dir",
        str(base),
        "--config",
        str(args.config),
        "--cv-parallelism",
        str(args.cv_parallelism),
    ]
    if args.water_resource_url:
        pipeline_cmd += ["--water-resource-url", args.water_resource_url]

    scale_cmd = [
        sys.executable,
        "scripts/water_scalability_profiler.py",
        "--output-dir",
        str(base),
        "--config",
        str(args.config),
        "--partitions",
        args.partitions,
        "--fixed-rows",
        str(args.fixed_rows),
        "--rows-per-partition",
        str(args.rows_per_partition),
    ]

    if run_cmd(pipeline_cmd, base) != 0:
        return 1
    if run_cmd(scale_cmd, base) != 0:
        return 1

    print("Full assessment workflow completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
