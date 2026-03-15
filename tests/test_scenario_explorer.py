from copy import deepcopy

import pytest

from premise_ui.core import scenario_explorer


def _fake_summary():
    return {
        "sector": "Electricity - generation",
        "label": "Exajoules (EJ)",
        "explanation": "Generated volumes of electricity.",
        "offset": 3,
        "group_by": "region",
        "regions": ["World"],
        "subscenarios": [],
        "years": [2020, 2030, 2040],
        "variables": ["Wind"],
        "scenarios": [
            {
                "scenario_id": "remind::SSP2-Base::remind_SSP2-Base",
                "model": "remind",
                "pathway": "SSP2-Base",
                "group_by": "region",
                "regions": ["World"],
                "subscenarios": [],
                "variables": ["Wind"],
                "years": [2020, 2030, 2040],
                "groups": [
                    {
                        "name": "World",
                        "group_type": "region",
                        "region": "World",
                        "variables": ["Wind"],
                        "years": [2020, 2030, 2040],
                        "series": [
                            {
                                "variable": "Wind",
                                "unit": "Exajoules (EJ)",
                                "points": [
                                    {"year": 2020, "value": 10.0},
                                    {"year": 2030, "value": 20.0},
                                    {"year": 2040, "value": 40.0},
                                ],
                            }
                        ],
                    }
                ],
            },
            {
                "scenario_id": "image::SSP2-NPi::image_SSP2-NPi",
                "model": "image",
                "pathway": "SSP2-NPi",
                "group_by": "region",
                "regions": ["World"],
                "subscenarios": [],
                "variables": ["Wind"],
                "years": [2020, 2030, 2040],
                "groups": [
                    {
                        "name": "World",
                        "group_type": "region",
                        "region": "World",
                        "variables": ["Wind"],
                        "years": [2020, 2030, 2040],
                        "series": [
                            {
                                "variable": "Wind",
                                "unit": "Exajoules (EJ)",
                                "points": [
                                    {"year": 2020, "value": 15.0},
                                    {"year": 2030, "value": 30.0},
                                    {"year": 2040, "value": 60.0},
                                ],
                            }
                        ],
                    }
                ],
            },
        ],
    }


def test_compare_scenario_explorer_indexed_mode(monkeypatch):
    monkeypatch.setattr(
        scenario_explorer,
        "summarize_scenario_explorer_sector",
        lambda *args, **kwargs: deepcopy(_fake_summary()),
    )

    payload = scenario_explorer.compare_scenario_explorer_sector(
        ["/tmp/remind.csv"],
        "Electricity - generation",
        compare_mode="indexed",
        baseline_year=2030,
    )

    assert payload["compare_mode"] == "indexed"
    assert payload["baseline_year"] == 2030
    assert payload["summary"]["label"] == "Index (baseline = 100)"
    first_points = payload["summary"]["scenarios"][0]["groups"][0]["series"][0][
        "points"
    ]
    assert first_points == [
        {"year": 2020, "value": 50.0},
        {"year": 2030, "value": 100.0},
        {"year": 2040, "value": 200.0},
    ]


def test_compare_scenario_explorer_delta_mode(monkeypatch):
    monkeypatch.setattr(
        scenario_explorer,
        "summarize_scenario_explorer_sector",
        lambda *args, **kwargs: deepcopy(_fake_summary()),
    )

    payload = scenario_explorer.compare_scenario_explorer_sector(
        ["/tmp/remind.csv", "/tmp/image.csv"],
        "Electricity - generation",
        compare_mode="delta",
        baseline_scenario_id="remind::SSP2-Base::remind_SSP2-Base",
    )

    assert payload["compare_mode"] == "delta"
    assert payload["baseline_scenario_id"] == "remind::SSP2-Base::remind_SSP2-Base"
    assert payload["baseline_scenario_label"] == "REMIND / SSP2-Base"
    assert len(payload["summary"]["scenarios"]) == 1
    compared = payload["summary"]["scenarios"][0]
    assert compared["comparison_label"] == "IMAGE / SSP2-NPi vs REMIND / SSP2-Base"
    assert compared["groups"][0]["series"][0]["points"] == [
        {"year": 2020, "value": 5.0},
        {"year": 2030, "value": 10.0},
        {"year": 2040, "value": 20.0},
    ]


def test_compare_scenario_explorer_percent_change_mode(monkeypatch):
    monkeypatch.setattr(
        scenario_explorer,
        "summarize_scenario_explorer_sector",
        lambda *args, **kwargs: deepcopy(_fake_summary()),
    )

    payload = scenario_explorer.compare_scenario_explorer_sector(
        ["/tmp/remind.csv", "/tmp/image.csv"],
        "Electricity - generation",
        compare_mode="percent_change",
        baseline_scenario_id="remind::SSP2-Base::remind_SSP2-Base",
    )

    assert payload["summary"]["label"] == "%"
    assert payload["baseline_scenario_label"] == "REMIND / SSP2-Base"
    assert payload["summary"]["scenarios"][0]["groups"][0]["series"][0]["points"] == [
        {"year": 2020, "value": 50.0},
        {"year": 2030, "value": 50.0},
        {"year": 2040, "value": 50.0},
    ]


def test_compare_scenario_explorer_rejects_unknown_baseline(monkeypatch):
    monkeypatch.setattr(
        scenario_explorer,
        "summarize_scenario_explorer_sector",
        lambda *args, **kwargs: deepcopy(_fake_summary()),
    )

    with pytest.raises(ValueError, match="Baseline scenario not found: missing"):
        scenario_explorer.compare_scenario_explorer_sector(
            ["/tmp/remind.csv", "/tmp/image.csv"],
            "Electricity - generation",
            compare_mode="delta",
            baseline_scenario_id="missing",
        )
