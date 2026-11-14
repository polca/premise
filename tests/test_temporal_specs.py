import csv
from types import SimpleNamespace

import pytest

from premise import trails
from premise.temporal_legacy import compile_legacy_rows
from premise.temporal_specs import (
    TemporalRegistry,
    TemporalSpecError,
    profile_to_temporal_parameters,
)

SOURCES = {
    "fleet-register": {
        "title": "Fleet register",
        "uri": "https://example.com/fleet",
    }
}


def profile(quantity="service_life", source_ids=None):
    return {
        "quantity": quantity,
        "reference_event": "current_service",
        "distribution": {
            "kind": "discrete",
            "offsets_years": [-1, -2],
            "weights": [0.5, 0.5],
        },
        "assumptions": [],
        "evidence": {
            "status": "empirical",
            "source_ids": source_ids or ["fleet-register"],
            "population": "Test fleet",
            "weighting": "Equal test weights",
            "reference_period": "2025",
            "censoring": "None in fixture",
            "sample_size": {"count": 2, "unit": "records"},
        },
        "review": {
            "activation_status": "active",
            "rationale": "Test fixture",
        },
    }


def selector(
    assignment_id,
    profile_id,
    value,
    *,
    kind="selector",
    versions=None,
    priority=0,
    expected=None,
):
    result = {
        "id": assignment_id,
        "profile": profile_id,
        "target": "activity",
        "kind": kind,
        "priority": priority,
        "match": {
            "all": [
                {"field": "name", "operator": "contains", "value": value},
                {
                    "field": "reference_product",
                    "operator": "equals",
                    "value": "electricity, high voltage",
                },
            ]
        },
    }
    if versions:
        result["applicability"] = {"ecoinvent_versions": versions}
    if expected:
        result["expected_matches"] = expected
    return result


def test_registry_loads_packaged_evidence_and_reviewed_assignment():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    assert "eia-860-generator-2025" in registry.sources
    assert "ec-light-duty-vehicle-survival-2015" in registry.sources
    assert "eu-passenger-car-survival-uk-mot" in registry.profiles
    assert "eu-road-passenger-car-age-2024" in registry.profiles
    assert len(registry.assignments) == 29
    assert all(
        item["id"].startswith(("reviewed-", "proxy-")) for item in registry.assignments
    )


def test_packaged_cereal_selector_has_behavior_equivalent_coverage():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    registry = TemporalRegistry(
        registry.sources,
        registry.profiles,
        [
            assignment
            for assignment in registry.assignments
            if registry.profiles[assignment["profile"]]["quantity"] == "biomass_growth"
        ],
    )
    records = compile_legacy_rows(DATA_DIR / "trails" / "temporal")
    report = registry.audit(
        records,
        target="activity",
        context={"ecoinvent_version": "3.12"},
    )

    assert report["covered"] == 144
    assert sum(report["selected_match_counts"].values()) == 144
    assert set(report["selected_match_counts"]) == {
        item["id"]
        for item in registry.assignments
        if "3.12" in item["applicability"]["ecoinvent_versions"]
    }
    for record in records:
        resolution = registry.resolve(
            record,
            target="activity",
            context={"ecoinvent_version": "3.12"},
        )
        if resolution is None:
            continue
        assert record["temporal_tag"] == "biomass_growth"
        assert record["age distribution type"] == "4"
        assert record["minimum"] == "-1"
        assert record["maximum"] == "0"


@pytest.mark.parametrize(
    ("version", "name", "assignment_id"),
    [
        (
            "3.8",
            "barley production, organic",
            "reviewed-legacy-barley-preharvest-uptake",
        ),
        (
            "3.9",
            "wheat production, organic",
            "reviewed-legacy-wheat-preharvest-uptake",
        ),
        ("3.9.1", "maize grain production", "reviewed-maize-grain-preharvest-uptake"),
        ("3.10", "rye production", "reviewed-rye-legacy-name-preharvest-uptake"),
        (
            "3.10.1",
            "oat grain production",
            "reviewed-oat-preharvest-uptake",
        ),
        ("3.11", "maize silage production", "reviewed-maize-silage-preharvest-uptake"),
        (
            "3.12",
            "wheat grain, feed production, organic, mountain region",
            "reviewed-wheat-feed-preharvest-uptake",
        ),
    ],
)
def test_packaged_cereal_selectors_cover_version_specific_names(
    version, name, assignment_id
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {"name": name, "reference product": "test product"},
        target="activity",
        context={"ecoinvent_version": version},
    )

    assert resolution is not None
    assert resolution.assignment_id == assignment_id


@pytest.mark.parametrize(
    "name",
    [
        "ethanol production from maize",
        "wheat grain, feed production",
        "maize seed production, organic, for sowing",
    ],
)
def test_packaged_cereal_selectors_exclude_non_crop_processing(name):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    assert (
        registry.resolve(
            {"name": name, "reference product": "test product"},
            target="activity",
            context={"ecoinvent_version": "3.12"},
        )
        is None
    )


def test_selector_supports_fields_exclusions_and_case_policy():
    assignment = selector("wind", "fleet", "wind")
    assignment["match"]["all"][0]["case_sensitive"] = False
    assignment["exclude"] = {
        "any": [{"field": "name", "operator": "contains", "value": "offshore"}]
    }
    registry = TemporalRegistry(SOURCES, {"fleet": profile()}, [assignment])
    assert (
        registry.resolve(
            {
                "name": "Electricity production, WIND, onshore",
                "reference product": "electricity, high voltage",
            },
            target="activity",
        ).profile_id
        == "fleet"
    )
    assert (
        registry.resolve(
            {
                "name": "electricity production, wind, offshore",
                "reference product": "electricity, high voltage",
            },
            target="activity",
        )
        is None
    )


def test_selector_can_match_classification_members():
    assignment = selector("power", "fleet", "wind")
    assignment["match"]["all"].append(
        {
            "field": "classification",
            "operator": "contains",
            "value": "Electric power generation",
        }
    )
    registry = TemporalRegistry(SOURCES, {"fleet": profile()}, [assignment])
    assert registry.resolve(
        {
            "name": "electricity production, wind",
            "reference product": "electricity, high voltage",
            "classifications": [
                ("ISIC rev.4 ecoinvent", "3510:Electric power generation")
            ],
        },
        target="activity",
    )


def test_precedence_is_override_then_versioned_then_general_then_fallback():
    profiles = {name: profile() for name in ["base", "general", "versioned", "exact"]}
    assignments = [
        {
            "id": "fallback",
            "profile": "base",
            "target": "activity",
            "kind": "fallback",
        },
        selector("general", "general", "wind"),
        selector("versioned", "versioned", "wind", versions=["3.12"]),
        selector("exact", "exact", "wind", kind="override"),
    ]
    registry = TemporalRegistry(SOURCES, profiles, assignments)
    record = {
        "name": "electricity production, wind",
        "reference product": "electricity, high voltage",
    }
    assert (
        registry.resolve(
            record,
            target="activity",
            context={"ecoinvent_version": "3.12"},
        ).assignment_id
        == "exact"
    )

    registry = TemporalRegistry(SOURCES, profiles, assignments[:-1])
    assert (
        registry.resolve(
            record,
            target="activity",
            context={"ecoinvent_version": "3.12"},
        ).assignment_id
        == "versioned"
    )
    assert (
        registry.resolve(
            record,
            target="activity",
            context={"ecoinvent_version": "3.11"},
        ).assignment_id
        == "general"
    )


def test_relationship_selector_uses_caller_and_direct_evidence_beats_proxy():
    construction = profile(quantity="construction_timing")
    direct = {
        "id": "direct-us",
        "profile": "construction",
        "target": "supplier",
        "kind": "selector",
        "exchange_match": {
            "all": [{"field": "name", "operator": "contains", "value": "wind turbine"}]
        },
        "caller_match": {
            "all": [{"field": "location", "operator": "glob", "value": "US-*"}]
        },
    }
    proxy = {
        "id": "proxy-global",
        "profile": "construction",
        "target": "supplier",
        "kind": "proxy",
        "exchange_match": direct["exchange_match"],
        "caller_match": {
            "all": [
                {
                    "field": "name",
                    "operator": "contains",
                    "value": "electricity production, wind",
                }
            ]
        },
    }
    registry = TemporalRegistry(
        SOURCES, {"construction": construction}, [proxy, direct]
    )
    exchange = {"name": "wind turbine, 2MW, onshore"}

    assert (
        registry.resolve(
            exchange,
            target="supplier",
            caller_record={
                "name": "electricity production, wind, onshore",
                "location": "US-WECC",
            },
        ).assignment_id
        == "direct-us"
    )
    assert (
        registry.resolve(
            exchange,
            target="supplier",
            caller_record={
                "name": "electricity production, wind, onshore",
                "location": "DE",
            },
        ).assignment_id
        == "proxy-global"
    )
    assert registry.resolve(exchange, target="supplier") is None


def test_equal_precedence_with_different_profiles_is_an_error():
    registry = TemporalRegistry(
        SOURCES,
        {"one": profile(), "two": profile()},
        [selector("a", "one", "wind"), selector("b", "two", "wind")],
    )
    with pytest.raises(TemporalSpecError, match="Conflicting temporal selectors"):
        registry.resolve(
            {
                "name": "electricity production, wind",
                "reference product": "electricity, high voltage",
            },
            target="activity",
        )


def test_audit_reports_coverage_and_enforces_expected_selected_matches():
    registry = TemporalRegistry(
        SOURCES,
        {"fleet": profile()},
        [
            selector(
                "wind",
                "fleet",
                "wind",
                expected={"minimum": 1, "maximum": 1},
            )
        ],
    )
    records = [
        {
            "name": "electricity production, wind",
            "reference product": "electricity, high voltage",
            "unit": "kilowatt hour",
        },
        {
            "name": "electricity production, solar",
            "reference product": "electricity, high voltage",
            "unit": "kilowatt hour",
        },
    ]
    report = registry.audit(records, target="activity")
    assert report["records"] == 2
    assert report["covered"] == 1
    assert report["selected_match_counts"] == {"wind": 1}
    assert report["uncovered"][0]["name"] == "electricity production, solar"

    registry.assignments[0]["expected_matches"] = {"minimum": 2}
    with pytest.raises(TemporalSpecError, match="match-count violations"):
        registry.audit(records, target="activity")


def test_registry_rejects_unknown_sources_and_invalid_discrete_profiles():
    bad_source = profile(source_ids=["missing"])
    with pytest.raises(TemporalSpecError, match="unknown source"):
        TemporalRegistry(SOURCES, {"bad": bad_source}, [])

    bad_distribution = profile()
    bad_distribution["distribution"]["weights"] = [1]
    with pytest.raises(TemporalSpecError, match="aligned"):
        TemporalRegistry(SOURCES, {"bad": bad_distribution}, [])


def test_registry_rejects_assignment_to_evidence_only_profile():
    evidence_only = profile()
    evidence_only["review"]["activation_status"] = "evidence_only"
    with pytest.raises(TemporalSpecError, match="not approved for active assignment"):
        TemporalRegistry(
            SOURCES,
            {"fleet": evidence_only},
            [selector("wind", "fleet", "wind")],
        )


def test_registry_requires_structured_empirical_evidence_metadata():
    incomplete = profile()
    incomplete["evidence"].pop("censoring")
    with pytest.raises(TemporalSpecError, match="evidence.censoring"):
        TemporalRegistry(SOURCES, {"bad": incomplete}, [])


def test_registry_checks_small_sample_flag_against_threshold():
    inconsistent = profile()
    inconsistent["evidence"]["sample_size"]["minimum_for_generalisation"] = 30
    with pytest.raises(TemporalSpecError, match="small-sample"):
        TemporalRegistry(SOURCES, {"bad": inconsistent}, [])


@pytest.mark.parametrize(
    ("version", "name", "profile_id", "years"),
    [
        (
            "3.8",
            "electricity production, wind, 1-3MW turbine, onshore",
            "nrel-wind-technical-life-30-years",
            30,
        ),
        (
            "3.10",
            "electricity production, photovoltaic, 570kWp open ground installation, multi-Si",
            "nrel-solar-pv-technical-life-30-years",
            30,
        ),
        (
            "3.11",
            "electricity production, hydro, run-of-river",
            "nrel-hydropower-technical-life-100-years",
            100,
        ),
        (
            "3.12",
            "electricity production, nuclear, pressure water reactor",
            "nrel-nuclear-technical-life-60-years",
            60,
        ),
    ],
)
def test_packaged_electricity_service_life_selectors(version, name, profile_id, years):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": name,
            "reference product": "electricity, high voltage",
            "unit": "kilowatt hour",
        },
        target="activity",
        context={"ecoinvent_version": version},
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assert registry.profiles[profile_id]["service_life_years"] == years


def test_registry_validates_known_derived_survival_generators():
    survival = profile(quantity="service_life")
    survival["distribution"] = {
        "kind": "derived",
        "generator": "right_censored_survival_curve",
        "parameters": {
            "ages_years": [1, 2, 3],
            "survival_probabilities": [0.9, 0.8, 0.85],
            "right_censored_after_years": 3,
        },
    }

    with pytest.raises(TemporalSpecError, match="nonincreasing"):
        TemporalRegistry(SOURCES, {"bad": survival}, [])


def test_profile_translation_uses_named_distribution_fields():
    triangular = profile(quantity="construction_timing")
    triangular["service_life_years"] = 30
    triangular["distribution"] = {
        "kind": "triangular",
        "minimum_years": -30,
        "mode_years": -12,
        "maximum_years": -1,
    }
    registry = TemporalRegistry(SOURCES, {"asset": triangular}, [])
    params = profile_to_temporal_parameters(registry.profiles["asset"])
    assert params == {
        "temporal_distribution": 5,
        "temporal_loc": -12.0,
        "temporal_scale": None,
        "temporal_offsets": None,
        "temporal_weights": None,
        "temporal_min": -30.0,
        "temporal_max": -1.0,
        "lifetime": 30,
    }


def test_registry_rejects_profile_quantity_for_wrong_target():
    with pytest.raises(TemporalSpecError, match="cannot apply"):
        TemporalRegistry(
            SOURCES,
            {"fleet": profile(quantity="fleet_age")},
            [selector("wrong-target", "fleet", "wind")],
        )


def test_trails_applies_supplier_selector_and_writes_match_audit(monkeypatch, tmp_path):
    construction = profile(quantity="construction_timing")
    construction["reference_event"] = "current_service"
    assignment = selector("wind-construction", "wind-fleet", "wind turbine")
    assignment["target"] = "supplier"
    registry = TemporalRegistry(SOURCES, {"wind-fleet": construction}, [assignment])
    exchange = {
        "type": "technosphere",
        "name": "wind turbine construction",
        "product": "electricity, high voltage",
        "unit": "unit",
        "amount": 1,
    }
    scenario = {
        "model": "test",
        "pathway": "test",
        "year": 2030,
        "database": [
            {
                "name": "electricity production, wind",
                "reference product": "electricity, high voltage",
                "exchanges": [exchange],
            }
        ],
    }
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = set()
    obj.biomass_growth_params = {}
    obj.maintenance_suppliers = set()
    obj.long_term_biosphere_params = []
    obj.dataset_lifetimes = {}
    obj.lifecycle_timing = {}
    obj.temporal_registry = registry
    obj.source_version = "3.12"
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    assert exchange["temporal_distribution"] == 6
    assert exchange["temporal_offsets"] == [-1.0, -2.0]
    assert exchange["temporal_weights"] == [0.5, 0.5]
    with (tmp_path / "trails_temp/temporal_selector_matches.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert rows[0]["assignment_id"] == "wind-construction"
    assert rows[0]["profile_id"] == "wind-fleet"


def test_trails_applies_packaged_reviewed_cereal_selector(monkeypatch, tmp_path):
    from premise.core.filesystem_constants import DATA_DIR

    exchange = {
        "type": "biosphere",
        "name": "Carbon dioxide, in air",
        "unit": "kilogram",
        "amount": -1,
    }
    scenario = {
        "model": "test",
        "pathway": "test",
        "year": 2030,
        "database": [
            {
                "name": "wheat grain production, organic",
                "reference product": "wheat grain, organic",
                "exchanges": [exchange],
            }
        ],
    }
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = set()
    obj.biomass_growth_params = {}
    obj.maintenance_suppliers = set()
    obj.long_term_biosphere_params = []
    obj.dataset_lifetimes = {}
    obj.lifecycle_timing = {}
    obj.temporal_registry = TemporalRegistry.from_directory(
        DATA_DIR / "trails" / "temporal"
    )
    obj.source_version = "3.12"
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    assert exchange["temporal_distribution"] == 4
    assert exchange["temporal_min"] == -1
    assert exchange["temporal_max"] == 0
    with (tmp_path / "trails_temp/temporal_selector_matches.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert rows[0]["assignment_id"] == "reviewed-wheat-preharvest-uptake"


def test_trails_uses_reviewed_service_life_for_maintenance_timing(
    monkeypatch, tmp_path
):
    from premise.core.filesystem_constants import DATA_DIR

    exchange = {
        "type": "technosphere",
        "name": "maintenance, wind power plant",
        "product": "maintenance service",
        "unit": "unit",
        "amount": 1,
    }
    scenario = {
        "model": "test",
        "pathway": "test",
        "year": 2030,
        "database": [
            {
                "name": "electricity production, wind, 1-3MW turbine, onshore",
                "reference product": "electricity, high voltage",
                "unit": "kilowatt hour",
                "exchanges": [exchange],
            }
        ],
    }
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = set()
    obj.biomass_growth_params = {}
    obj.maintenance_suppliers = {
        ("maintenance, wind power plant", "maintenance service")
    }
    obj.long_term_biosphere_params = []
    obj.dataset_lifetimes = {
        (
            "electricity production, wind, 1-3MW turbine, onshore",
            "electricity, high voltage",
        ): 20
    }
    obj.lifecycle_timing = {}
    obj.temporal_registry = TemporalRegistry.from_directory(
        DATA_DIR / "trails" / "temporal"
    )
    obj.source_version = "3.12"
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    assert exchange["temporal_distribution"] == 4
    assert exchange["temporal_min"] == 0
    assert exchange["temporal_max"] == 30


@pytest.mark.parametrize(
    ("location", "assignment_id"),
    [
        ("US-WECC", "reviewed-us-eia-onshore-wind-construction-vintage"),
        ("DE", "proxy-eia-onshore-wind-construction-vintage"),
    ],
)
def test_trails_applies_relationship_aware_construction_vintage(
    monkeypatch, tmp_path, location, assignment_id
):
    from premise.core.filesystem_constants import DATA_DIR

    exchange = {
        "type": "technosphere",
        "name": "wind turbine, 2MW, onshore",
        "product": "wind turbine, 2MW, onshore",
        "unit": "unit",
        "amount": 1,
    }
    scenario = {
        "model": "test",
        "pathway": "test",
        "year": 2030,
        "database": [
            {
                "name": "electricity production, wind, 1-3MW turbine, onshore",
                "reference product": "electricity, high voltage",
                "unit": "kilowatt hour",
                "location": location,
                "exchanges": [exchange],
            }
        ],
    }
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = set()
    obj.biomass_growth_params = {}
    obj.maintenance_suppliers = set()
    obj.long_term_biosphere_params = []
    obj.dataset_lifetimes = {}
    obj.lifecycle_timing = {}
    obj.temporal_registry = TemporalRegistry.from_directory(
        DATA_DIR / "trails" / "temporal"
    )
    obj.source_version = "3.12"
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    profile = obj.temporal_registry.profiles[
        "eia-us-onshore-wind-construction-vintage-2025"
    ]
    assert exchange["temporal_distribution"] == 6
    assert exchange["temporal_offsets"] == profile["distribution"]["offsets_years"]
    with (tmp_path / "trails_temp/temporal_selector_matches.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    supplier_row = next(row for row in rows if row["target"] == "supplier")
    assert supplier_row["assignment_id"] == assignment_id
    assert supplier_row["assignment_kind"] == (
        "proxy" if assignment_id.startswith("proxy-") else "selector"
    )
