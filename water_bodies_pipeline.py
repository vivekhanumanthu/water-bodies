#!/usr/bin/env python3
"""Compatibility entrypoint for the water-bodies pipeline."""

from scripts.run_water_bodies_pipeline import main


if __name__ == "__main__":
    raise SystemExit(main())
