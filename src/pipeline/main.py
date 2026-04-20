"""
Pipeline entry point.

This module serves as the CLI entry point and delegates fully to the
Prefect flow. Run with:

    uv run python -m src.pipeline.main
    uv run python -m src.pipeline.main --years 2024

For Prefect-tracked runs (recommended):
    uv run python -m src.orchestration.flows
"""

import argparse
import logging
import sys

# Configure structured logging before importing flow (avoids Prefect log noise)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)

from src.orchestration.flows import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SNIES Academic Capacity Pipeline",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help="Years to process (e.g. --years 2023 2024). Defaults to all configured years.",
    )
    args = parser.parse_args()
    run_pipeline(years=args.years)


if __name__ == "__main__":
    main()
