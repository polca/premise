"""Compile the TRAILS compatibility CSV from its canonical YAML registry."""

from __future__ import annotations

import argparse
from pathlib import Path

from premise.temporal_legacy import write_legacy_csv


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("premise/data/trails/temporal"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("premise/data/trails/temporal_distributions.csv"),
    )
    args = parser.parse_args()
    write_legacy_csv(args.registry, args.output)


if __name__ == "__main__":
    main()
