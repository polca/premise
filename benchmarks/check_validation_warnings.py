"""Reject validation warnings beyond a reviewed per-case baseline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--case", required=True)
    return parser.parse_args()


def warning_regressions(
    baseline_counts: dict[str, int], current_counts: dict[str, int]
) -> dict[str, dict[str, int]]:
    """Return only rule counts which exceed the reviewed baseline."""

    return {
        rule_id: {"baseline": int(baseline_counts.get(rule_id, 0)), "current": count}
        for rule_id, count in sorted(current_counts.items())
        if int(count) > int(baseline_counts.get(rule_id, 0))
    }


def main() -> None:
    args = parse_args()
    baseline = yaml.safe_load(args.baseline.read_text(encoding="utf-8")) or {}
    profile = json.loads(args.profile.read_text(encoding="utf-8"))
    cases = baseline.get("cases", {})
    if args.case not in cases:
        raise SystemExit(
            f"Validation warning baseline has no reviewed case {args.case!r}."
        )
    baseline_counts = cases[args.case].get("warning_rule_counts", {})
    validation = profile.get("validation", {})
    expected_ruleset = baseline.get("metadata", {}).get("validation_ruleset_version")
    current_ruleset = validation.get("ruleset_version")
    if expected_ruleset is not None and current_ruleset != expected_ruleset:
        raise SystemExit(
            "Validation warning baseline ruleset does not match the profile: "
            f"{expected_ruleset!r} != {current_ruleset!r}."
        )
    current_counts = validation.get("warning_rule_counts", {})
    reported_total = int(validation.get("warning_count", 0))
    counted_total = sum(int(value) for value in current_counts.values())
    if reported_total != counted_total:
        raise SystemExit(
            "Validation profile warning total disagrees with its per-rule counts: "
            f"{reported_total} != {counted_total}."
        )

    regressions = warning_regressions(baseline_counts, current_counts)
    if regressions:
        raise SystemExit(
            "New validation warnings require a reviewed baseline update or a narrow "
            "versioned suppression: " + json.dumps(regressions, sort_keys=True)
        )
    print(
        json.dumps(
            {
                "case": args.case,
                "warning_count": reported_total,
                "warning_rule_counts": current_counts,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
