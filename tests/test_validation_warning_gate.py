import json
from pathlib import Path

import pytest
import yaml

from benchmarks.check_validation_warnings import main, warning_regressions
from premise.validation.validation_framework import VALIDATION_RULESET_VERSION

BASELINE_PATH = Path(__file__).parent / "data" / "validation_warning_baselines.yaml"


def test_packaged_warning_baseline_matches_current_ruleset():
    baseline = yaml.safe_load(BASELINE_PATH.read_text(encoding="utf-8"))
    assert (
        baseline["metadata"]["validation_ruleset_version"] == VALIDATION_RULESET_VERSION
    ), "Review the certification warning baseline when changing validation rules."


@pytest.mark.parametrize("extra_warning", [False, True])
def test_cli_enforces_packaged_warning_limits(extra_warning, tmp_path, monkeypatch):
    baseline = yaml.safe_load(BASELINE_PATH.read_text(encoding="utf-8"))
    case = "ecoinvent-3.12-cutoff-image-SSP2-M-2050"
    counts = dict(baseline["cases"][case]["warning_rule_counts"])
    if extra_warning:
        counts["LEGACY.CO2_EMISSIONS_POSSIBLY_INCORRECT"] += 1
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "validation": {
                    "ruleset_version": VALIDATION_RULESET_VERSION,
                    "warning_count": sum(counts.values()),
                    "warning_rule_counts": counts,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "check_validation_warnings.py",
            "--baseline",
            str(BASELINE_PATH),
            "--profile",
            str(profile),
            "--case",
            case,
        ],
    )
    if extra_warning:
        with pytest.raises(SystemExit, match="New validation warnings"):
            main()
    else:
        main()


def test_warning_gate_accepts_equal_or_reduced_counts():
    assert (
        warning_regressions(
            {"RULE.ONE": 2, "RULE.TWO": 1},
            {"RULE.ONE": 1, "RULE.TWO": 1},
        )
        == {}
    )


def test_warning_gate_rejects_new_rules_and_increased_counts():
    assert warning_regressions(
        {"RULE.ONE": 1},
        {"RULE.ONE": 2, "RULE.NEW": 1},
    ) == {
        "RULE.NEW": {"baseline": 0, "current": 1},
        "RULE.ONE": {"baseline": 1, "current": 2},
    }


def test_warning_gate_rejects_a_stale_ruleset(tmp_path, monkeypatch):
    baseline = tmp_path / "baseline.yaml"
    profile = tmp_path / "profile.json"
    baseline.write_text(
        yaml.safe_dump(
            {
                "metadata": {"validation_ruleset_version": 4},
                "cases": {"case": {"warning_rule_counts": {}}},
            }
        ),
        encoding="utf-8",
    )
    profile.write_text(
        json.dumps(
            {
                "validation": {
                    "ruleset_version": 3,
                    "warning_count": 0,
                    "warning_rule_counts": {},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "check_validation_warnings.py",
            "--baseline",
            str(baseline),
            "--profile",
            str(profile),
            "--case",
            "case",
        ],
    )

    with pytest.raises(SystemExit, match="ruleset"):
        main()
