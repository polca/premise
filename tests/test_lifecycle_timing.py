import csv
from types import SimpleNamespace

import pytest

import premise.trails as trails
from premise.lifecycle_timing import (
    construction_age_profile,
    load_retirement_asset_links,
    load_lifecycle_timing,
    remaining_life_profile,
    retirement_profile,
)
from premise.trails import TrailsDataPackage


def test_remaining_life_preserves_age_weights_and_merges_annual_dates():
    assert remaining_life_profile(20, [-5, -15], [1, 1]) == ([5, 15], [0.5, 0.5])
    assert remaining_life_profile(20, [-5, -5, -20], [1, 1, 2]) == ([0, 15], [0.5, 0.5])
    assert remaining_life_profile(20.5, [-5], [1]) == ([16], [1.0])


def test_construction_profiles_keep_their_annual_weights_at_retirement():
    ages, weights = construction_age_profile(
        {
            "temporal_distribution": 6,
            "temporal_offsets": [-5, -15],
            "temporal_weights": [1, 3],
        }
    )
    assert ages == [-5, -15]
    assert remaining_life_profile(20, ages, weights) == ([5, 15], [0.75, 0.25])
    ages, weights = construction_age_profile(
        {
            "temporal_distribution": 5,
            "temporal_min": -4,
            "temporal_max": 0,
            "temporal_loc": -1.5,
        }
    )
    offsets, retirement_weights = remaining_life_profile(4, ages, weights)
    assert offsets == [1, 2, 3, 4]
    assert retirement_weights == pytest.approx([1 / 6, 2 / 6, 2 / 6, 1 / 6])
    assert construction_age_profile(
        {
            "temporal_distribution": 4,
            "temporal_min": -3,
            "temporal_max": -1,
        }
    ) == ([-3, -2, -1], [1 / 3, 1 / 3, 1 / 3])


@pytest.mark.parametrize(
    "params",
    [
        {"temporal_distribution": 3},
        {"temporal_distribution": 5, "temporal_loc": -5},
        {"temporal_distribution": 6, "temporal_offsets": [1], "temporal_weights": [1]},
        {
            "temporal_distribution": 6,
            "temporal_offsets": [-1],
            "temporal_weights": [-1],
        },
        {"temporal_distribution": 6, "temporal_offsets": [-1], "temporal_weights": [0]},
        {"temporal_distribution": 6, "temporal_offsets": [-1], "temporal_weights": []},
    ],
)
def test_invalid_or_unsupported_construction_is_not_invented(params):
    with pytest.raises(ValueError):
        construction_age_profile(params)


@pytest.mark.parametrize("retirement_first", [False, True])
@pytest.mark.parametrize("overage", [False, True])
@pytest.mark.parametrize("explicit_ages", [False, True])
def test_retirement_uses_matching_asset_not_factory_or_unrelated_caller_life(
    monkeypatch, tmp_path, retirement_first, overage, explicit_ages
):
    capital = {
        "type": "technosphere",
        "name": "asset",
        "product": "asset",
        "amount": 0.001,
    }
    treatment = {
        "type": "technosphere",
        "name": "treatment",
        "product": "used asset",
        "amount": 2.0,
    }
    exchanges = [treatment, capital] if retirement_first else [capital, treatment]
    exchanges.append(
        {"type": "technosphere", "name": "factory", "product": "factory", "amount": 0.1}
    )
    ds = {
        "name": "fleet service",
        "reference product": "service",
        "location": "GLO",
        "exchanges": exchanges,
    }
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.stock_asset_params = {
        ("asset", "asset"): {
            "temporal_distribution": 6,
            "temporal_offsets": [-5, -25 if overage else -15],
            "temporal_weights": [0.25, 0.75],
            "lifetime": 20,
        },
        ("factory", "factory"): {
            "temporal_distribution": 6,
            "temporal_offsets": [-1],
            "temporal_weights": [1],
            "lifetime": 100,
        },
    }
    obj.end_of_life_suppliers = {("treatment", "used asset")}
    obj.retirement_asset_links = {("treatment", "used asset"): (("asset", "asset"),)}
    obj.dataset_lifetimes = {("fleet service", "service"): 30}
    obj.lifecycle_timing = {
        ("fleet service", "service"): {
            "timing_reference": "existing_fleet",
            "lifetime": 30,
            "age_offsets": [-1, -29],
            "age_weights": [0.5, 0.5],
            "assumption": (
                "User-specified fleet ages"
                if explicit_ages
                else "Explicit stationary-stock proxy with fixed life and uniform annual ages 1..floor(L)."
            ),
        }
    }
    obj.datapackage = SimpleNamespace(
        scenarios=[
            {"model": "test", "pathway": "test", "year": 2030, "database": [ds]}
        ],
        database="db",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)
    obj.add_temporal_distributions()
    record = next(
        csv.DictReader((tmp_path / "trails_temp/end_of_life_timing.csv").open())
    )
    assert capital["amount"] == 0.001 and treatment["amount"] == 2
    if explicit_ages:
        assert treatment["temporal_offsets"] == [1.0, 29.0]
        assert record["retirement_basis"] == "explicit_fleet_age_profile"
        assert record["fallback_reason"] == ""
    elif overage:
        assert treatment["temporal_offsets"] == [1.0, 29.0]
        assert record["retirement_basis"] == "stationary_fleet_fallback"
        assert "ages" in record["fallback_reason"]
    else:
        assert treatment["temporal_offsets"] == [5.0, 15.0]
        assert treatment["temporal_weights"] == [0.75, 0.25]
        assert record["fixed_lifetime"] == "20"
        assert record["retirement_basis"] == "matched_construction_profile"
        assert record["construction_dataset_name"] == "asset"
        assert record["fallback_reason"] == ""


def test_reviewed_asset_links_are_explicit_and_reject_duplicates(tmp_path):
    links = load_retirement_asset_links(
        trails.TEMPORAL_REGISTRY_DIR / "retirement_links.yaml"
    )
    assert links[("EPR decommissioning", "EPR decommissioning")] == (
        ("EPR construction", "EPR construction"),
    )
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "schema_version: 1\nretirement_asset_links:\n- retirement_supplier: {name: x, reference_product: x}\n  construction_suppliers: []\n"
    )
    with pytest.raises(ValueError):
        load_retirement_asset_links(bad)


def test_retirement_provenance_is_readable_as_a_datapackage_resource(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    folder = tmp_path / "trails_temp"
    folder.mkdir()
    path = folder / "end_of_life_timing.csv"
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "scenario",
                "temporal_offsets",
                "temporal_weights",
                "retirement_basis",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "scenario": "test | pathway | 2025",
                "temporal_offsets": "[5, 15]",
                "temporal_weights": "[0.75, 0.25]",
                "retirement_basis": "matched_construction_profile",
            }
        )
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.scenario_names = []
    monkeypatch.setattr(obj, "reorder_matrices", lambda: None)
    obj._build_datapackage("test")
    package = trails.Package(str(tmp_path / "test.zip"))
    resource = package.get_resource("end_of_life_timing")
    assert resource.descriptor["dialect"]["delimiter"] == ","
    records = resource.read(keyed=True)
    assert records[0]["retirement_basis"] == "matched_construction_profile"


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
