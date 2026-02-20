#!/usr/bin/env python3
"""Backward-compatible entrypoint for the pipeline."""

from scripts.run_pipeline import main


if __name__ == "__main__":
    raise SystemExit(main())
