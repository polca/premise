from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
BENCHMARKS = ROOT / "benchmarks"
sys.path.insert(0, str(BENCHMARKS))
SPEC = importlib.util.spec_from_file_location(
    "check_factorization_mode", BENCHMARKS / "check_factorization_mode.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def arguments():
    return SimpleNamespace(
        project="ecoinvent-3.12-cutoff",
        source_db="ecoinvent-3.12-cutoff",
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        sectors=["electricity", "emissions"],
        max_standard_baseline_ratio=1.05,
        max_declarative_baseline_ratio=1.10,
        absolute_max_declarative_baseline_ratio=1.20,
        max_peak_rss_baseline_ratio=1.20,
    )


def baseline():
    return {
        "baseline_commit": "abc123",
        "scenario": {
            "project": "ecoinvent-3.12-cutoff",
            "source_database": "ecoinvent-3.12-cutoff",
            "model": "remind",
            "pathway": "SSP2-NPi",
            "year": 2050,
            "sectors": ["electricity", "emissions"],
            "inventory_backend": "compact",
        },
        "measurement": {
            "standard_median_seconds": 100.0,
            "first_standard_peak_rss_bytes": 1_000,
        },
    }


def test_factorization_performance_gate_applies_goal_limits():
    comparison = MODULE.compare_baseline(
        args=arguments(),
        baseline=baseline(),
        standard_median=102.0,
        factorized_median=109.0,
        first_factorized_peak_rss=1_100,
    )

    assert comparison["current_standard_accepted"]
    assert comparison["declarative_accepted"]
    assert not comparison["declarative_absolute_failure"]
    assert comparison["peak_rss_accepted"]

    failed = MODULE.compare_baseline(
        args=arguments(),
        baseline=baseline(),
        standard_median=106.0,
        factorized_median=121.0,
        first_factorized_peak_rss=1_201,
    )
    assert not failed["current_standard_accepted"]
    assert not failed["declarative_accepted"]
    assert failed["declarative_absolute_failure"]
    assert not failed["peak_rss_accepted"]


def test_factorization_performance_gate_rejects_unlike_baseline():
    payload = baseline()
    payload["scenario"]["sectors"] = ["electricity"]

    with pytest.raises(ValueError, match="Baseline scenario does not match"):
        MODULE.compare_baseline(
            args=arguments(),
            baseline=payload,
            standard_median=100.0,
            factorized_median=100.0,
            first_factorized_peak_rss=1_000,
        )
