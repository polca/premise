import json

import pytest
import yaml

from benchmarks.check_validation_warnings import main, warning_regressions


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
