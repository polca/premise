import csv
from types import SimpleNamespace

import pytest

import premise.trails as trails
from premise.lifecycle_timing import (
    load_lifecycle_timing,
    remaining_life_profile,
    retirement_profile,
)
from premise.trails import TrailsDataPackage


def test_remaining_life_preserves_age_weights_and_merges_annual_dates():
    assert remaining_life_profile(20, [-5, -15], [1, 1]) == ([5, 15], [0.5, 0.5])
    assert remaining_life_profile(20, [-5, -5, -20], [1, 1, 2]) == ([0, 15], [0.5, 0.5])
    assert remaining_life_profile(20.5, [-5], [1]) == ([16], [1.0])


@pytest.mark.parametrize(
    "lifetime,ages,weights",
    [
        (0, [-1], [1]),
        (20, [-21], [1]),
        (20, [1], [1]),
        (20, [-1], [-1]),
        (20, [-1], [0]),
        (20, [-1], []),
        (20, [], []),
        (float("nan"), [-1], [1]),
        (20, [-1], [float("inf")]),
    ],
)
def test_invalid_fleets_are_not_silently_clipped(lifetime, ages, weights):
    with pytest.raises(ValueError):
        remaining_life_profile(lifetime, ages, weights)


def test_reference_event_controls_retirement_without_reapplying_vintage():
    assert retirement_profile("new_asset", 20) == ([20], [1.0])
    for reference in ["current_output", "lifecycle_event"]:
        assert retirement_profile(reference, 20) == ([0], [1.0])
    # Two historic production years each receive the same fixed service life.
    # Their retirement years are already spread without another age profile.
    offset = retirement_profile("new_asset", 20)[0][0]
    assert [year + offset for year in [2010, 2020]] == [2030, 2040]
    with pytest.raises(ValueError, match="explicit"):
        retirement_profile("existing_fleet", 20)
    # The explicitly declared fleet life belongs with its age weights, even if
    # an unrelated legacy stock classification supplied another caller life.
    assert retirement_profile(
        "existing_fleet",
        30,
        {"lifetime": 20, "age_offsets": [-5, -15], "age_weights": [1, 1]},
    ) == ([5, 15], [0.5, 0.5])


def test_loader_requires_explicit_age_weights_and_rejects_conflicts(tmp_path):
    path = tmp_path / "timing.csv"
    fields = [
        "name",
        "reference product",
        "lifetime",
        "timing_reference",
        "fleet_age_offsets",
        "fleet_age_weights",
    ]
    row = dict(
        zip(
            fields,
            [
                "fleet service",
                "service",
                "20",
                "existing_fleet",
                "[-5, -15]",
                "[0.5, 0.5]",
            ],
        )
    )

    def write(rows):
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    write([row])
    assert load_lifecycle_timing(path)["fleet service", "service"]["offsets"] == [5, 15]
    write([{**row, "fleet_age_offsets": ""}])
    with pytest.raises(ValueError, match="Invalid fleet"):
        load_lifecycle_timing(path)
    write([row, {**row, "timing_reference": "new_asset"}])
    with pytest.raises(ValueError, match="Conflicting"):
        load_lifecycle_timing(path)


def test_assignment_applies_current_new_and_fleet_reference_events(
    monkeypatch, tmp_path
):
    definitions = [
        ("new asset", "asset", "new_asset"),
        ("fleet service", "service", "existing_fleet"),
        ("current production", "product", "current_output"),
        ("market for used asset", "used asset", "lifecycle_event"),
    ]
    datasets = [
        {
            "name": name,
            "reference product": product,
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "treatment",
                    "product": "used asset",
                    "amount": 2.0,
                }
            ],
        }
        for name, product, _ in definitions
    ]
    scenario = {"model": "test", "pathway": "test", "year": 2030, "database": datasets}
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = {("treatment", "used asset")}
    obj.maintenance_suppliers = set()
    obj.biomass_growth_params = {}
    obj.long_term_biosphere_params = []
    obj.dataset_lifetimes = {(name, product): 20 for name, product, _ in definitions}
    obj.lifecycle_timing = {
        (name, product): {"timing_reference": reference}
        for name, product, reference in definitions
    }
    obj.lifecycle_timing["fleet service", "service"].update(
        age_offsets=[-5, -15], age_weights=[0.25, 0.75]
    )
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)
    obj.add_temporal_distributions()
    exchanges = [ds["exchanges"][0] for ds in datasets]
    assert [e["temporal_offsets"] for e in exchanges] == [
        [20.0],
        [5.0, 15.0],
        [0.0],
        [0.0],
    ]
    assert exchanges[1]["temporal_weights"] == [0.75, 0.25]
    assert all(e["amount"] == 2.0 for e in exchanges)
    report = list(
        csv.DictReader((tmp_path / "trails_temp/end_of_life_timing.csv").open())
    )
    assert len(report) == 4
    assert {row["timing_reference"] for row in report} == {x[2] for x in definitions}


def test_shipped_references_distinguish_declared_stationary_fleet_proxies():
    records = load_lifecycle_timing(trails.FILEPATH_TEMPORAL_PARAMETERS)
    assert len(records) > 9000
    assert {r["timing_reference"] for r in records.values()} == {
        "new_asset",
        "current_output",
        "lifecycle_event",
        "existing_fleet",
    }
    assert (
        records["market for used cable", "used cable"]["timing_reference"]
        == "lifecycle_event"
    )


def test_shipped_stationary_fleets_have_valid_normalised_remaining_lives():
    records = load_lifecycle_timing(trails.FILEPATH_TEMPORAL_PARAMETERS)
    fleets = [r for r in records.values() if r["timing_reference"] == "existing_fleet"]
    assert fleets
    for record in fleets:
        assert min(record["offsets"]) >= 0
        assert max(record["offsets"]) <= record["lifetime"]
        assert sum(record["weights"]) == pytest.approx(1)
        assert len(record["age_offsets"]) == int(record["lifetime"])
