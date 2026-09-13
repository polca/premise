import pytest

from benchmarks.check_validation_performance import evaluate_performance


def profile(update, *, sector=0.0, cache=0.0, rss=1_000):
    return {
        "phases": [
            {"name": "update-total", "wall_seconds": update},
            {"name": "validation:cache-reuse", "wall_seconds": cache},
        ],
        "validation": {"sector_seconds": sector},
        "sampled_peak_rss_bytes": rss,
    }


def test_performance_gate_compares_certified_total_with_baseline():
    metrics, failures = evaluate_performance(
        profile(100), profile(102.5, sector=0.8, cache=0.05, rss=1_020)
    )

    assert failures == []
    assert metrics["incremental_fraction"] == pytest.approx(0.008)
    assert metrics["certification_overhead_fraction"] == pytest.approx(0.025)


def test_performance_gate_reports_each_limit():
    _, failures = evaluate_performance(
        profile(100), profile(104, sector=1.1, cache=0.1, rss=1_031)
    )

    assert len(failures) == 4
    assert failures[0].startswith("incremental validation")
    assert failures[1].startswith("certified update overhead")
    assert failures[2].startswith("certificate reuse")
    assert failures[3].startswith("peak RSS growth")


@pytest.mark.parametrize("which", ["baseline", "current"])
def test_performance_gate_requires_update_phase(which):
    baseline = profile(100)
    current = profile(100)
    (baseline if which == "baseline" else current)["phases"] = []

    with pytest.raises(RuntimeError, match=which.capitalize()):
        evaluate_performance(baseline, current)
