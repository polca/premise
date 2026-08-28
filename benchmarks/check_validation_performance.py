"""Enforce the reviewed validation runtime and memory acceptance gates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--incremental-limit", type=float, default=0.01)
    parser.add_argument("--certification-limit", type=float, default=0.03)
    parser.add_argument("--cache-limit-seconds", type=float, default=0.1)
    parser.add_argument("--rss-limit", type=float, default=0.03)
    return parser.parse_args()


def phase_seconds(profile: dict, label: str) -> float:
    return sum(
        float(phase.get("wall_seconds", 0.0))
        for phase in profile.get("phases", ())
        if phase.get("name") == label
    )


def evaluate_performance(
    baseline: dict,
    current: dict,
    *,
    incremental_limit: float = 0.01,
    certification_limit: float = 0.03,
    cache_limit_seconds: float = 0.1,
    rss_limit: float = 0.03,
) -> tuple[dict, list[str]]:
    """Compare a certified build with the validator it replaced.

    The full graph pass replaces existing end-of-sector validation work. Its
    acceptance criterion is therefore the change in total update time, not the
    graph phase divided by the new update time. Incremental sector validation
    is additional work and is measured directly against the baseline update.
    """
    baseline_update_seconds = phase_seconds(baseline, "update-total")
    current_update_seconds = phase_seconds(current, "update-total")
    if baseline_update_seconds <= 0:
        raise RuntimeError("Baseline profile has no positive update-total phase.")
    if current_update_seconds <= 0:
        raise RuntimeError("Current profile has no positive update-total phase.")

    validation = current.get("validation", {})
    sector_seconds = float(validation.get("sector_seconds", 0.0))
    sector_fraction = sector_seconds / baseline_update_seconds
    certification_fraction = current_update_seconds / baseline_update_seconds - 1
    cache_seconds = phase_seconds(current, "validation:cache-reuse")
    baseline_rss = baseline.get("sampled_peak_rss_bytes")
    current_rss = current.get("sampled_peak_rss_bytes")
    rss_growth = (
        current_rss / baseline_rss - 1 if baseline_rss and current_rss else None
    )

    failures = []
    if sector_fraction > incremental_limit:
        failures.append(
            f"incremental validation {sector_fraction:.3%} > {incremental_limit:.3%}"
        )
    if certification_fraction > certification_limit:
        failures.append(
            "certified update overhead "
            f"{certification_fraction:.3%} > {certification_limit:.3%}"
        )
    if cache_seconds >= cache_limit_seconds:
        failures.append(
            f"certificate reuse {cache_seconds:.6f}s >= {cache_limit_seconds:.6f}s"
        )
    if rss_growth is not None and rss_growth > rss_limit:
        failures.append(f"peak RSS growth {rss_growth:.3%} > {rss_limit:.3%}")

    return {
        "baseline_update_seconds": baseline_update_seconds,
        "current_update_seconds": current_update_seconds,
        "incremental_fraction": sector_fraction,
        "certification_overhead_fraction": certification_fraction,
        "cache_seconds": cache_seconds,
        "rss_growth": rss_growth,
    }, failures


def main() -> None:
    args = parse_args()
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    current = json.loads(args.current.read_text(encoding="utf-8"))
    metrics, failures = evaluate_performance(
        baseline,
        current,
        incremental_limit=args.incremental_limit,
        certification_limit=args.certification_limit,
        cache_limit_seconds=args.cache_limit_seconds,
        rss_limit=args.rss_limit,
    )
    if failures:
        raise SystemExit("Validation performance gate failed: " + "; ".join(failures))

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
