#!/usr/bin/env python3
"""Unified runner for water-bodies data platform pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import run_water_bodies_pipeline as water  # type: ignore

def run_water(args: argparse.Namespace) -> int:
    argv = [
        "run_water_bodies_pipeline.py",
        "--output-dir",
        str(args.output_dir),
        "--config",
        str(args.config),
        "--dataset-url",
        args.water_dataset_url,
    ]
    if args.water_resource_url:
        argv += ["--resource-url", args.water_resource_url]
    if args.skip_ml:
        argv += ["--skip-ml"]
    argv += ["--cv-parallelism", str(args.cv_parallelism)]

    old = sys.argv
    try:
        sys.argv = argv
        return water.main()
    finally:
        sys.argv = old


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run water-bodies dataset pipeline")
    parser.add_argument("--dataset", choices=["water_bodies", "all"], default="all")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))

    parser.add_argument(
        "--water-dataset-url",
        type=str,
        default="https://catalog.data.gov/dataset/water-bodies-07739",
    )
    parser.add_argument("--water-resource-url", type=str, default=None)
    parser.add_argument("--skip-ml", action="store_true")
    parser.add_argument("--cv-parallelism", type=int, default=2)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print("Running water_bodies pipeline...")
    return run_water(args)


if __name__ == "__main__":
    raise SystemExit(main())
