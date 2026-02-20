#!/usr/bin/env python3
"""Convenience wrapper: run optimized pipeline only."""

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.pipeline_compare_modes import main  # type: ignore


if __name__ == "__main__":
    if "--mode" not in sys.argv:
        sys.argv.extend(["--mode", "optimized"])
    raise SystemExit(main())
