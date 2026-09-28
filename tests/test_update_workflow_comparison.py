from copy import deepcopy

import pytest

from benchmarks.compare_update_workflows import (
    compare_simapro,
    differences,
    normalized_metadata,
)


def test_simapro_comparison_checks_amounts_and_consistent_supplier_ids(tmp_path):
    old, new = tmp_path / "old.csv", tmp_path / "new.csv"
    old.write_text(f"supplier | ID: {'a' * 32}\nflow;1.0 | ID = {'a' * 32}\n")
    new.write_text(f"supplier | ID: {'b' * 32}\nflow;1.0 | ID = {'b' * 32}\n")
    assert compare_simapro(old, new) == 1
    new.write_text(f"supplier | ID: {'b' * 32}\nflow;1.1 | ID = {'b' * 32}\n")
    with pytest.raises(AssertionError):
        compare_simapro(old, new)
    new.write_text(f"supplier | ID: {'b' * 32}\nflow;1.0 | ID = {'c' * 32}\n")
    with pytest.raises(AssertionError, match="inconsistent ID"):
        compare_simapro(old, new)


def test_metadata_comparison_retains_validation_counts_and_numeric_results():
    baseline = {
        "build_id": "a",
        "validation": {
            "certificate_key": "a",
            "store_generation": 0,
            "phase_results": [
                {
                    "kind": "sector",
                    "elapsed_seconds": 1.0,
                    "rule_results": [{"checked_object_count": 3, "actual": 1.0}],
                }
            ],
        },
    }
    candidate = deepcopy(baseline)
    candidate["build_id"] = "b"
    candidate["validation"]["phase_results"][0]["elapsed_seconds"] = 0.5
    assert normalized_metadata(baseline) == normalized_metadata(candidate)
    rule = candidate["validation"]["phase_results"][0]["rule_results"][0]
    for field, value in [("checked_object_count", 2), ("actual", 1.0 + 1e-13)]:
        previous = rule[field]
        rule[field] = value
        assert list(
            differences(normalized_metadata(baseline), normalized_metadata(candidate))
        )
        rule[field] = previous
