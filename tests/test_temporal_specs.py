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
    assert "eurostat-avia-eq-arc-age-2024" in registry.sources
    assert (
        "eurostat-eu27-commercial-passenger-aircraft-construction-vintage-2024"
        in registry.profiles
    )
    assert "ccnr-rhine-dry-cargo-vessel-construction-vintage-2024" in registry.profiles
    assert "ccnr-rhine-tanker-vessel-construction-vintage-2024" in registry.profiles
    assert "gem-global-solar-power-tracker-february-2026" in registry.sources
    assert "gem-gspt-world-csp-construction-vintage-2026" in registry.profiles
    assert len(registry.assignments) == 148
    assert all(
        item["id"].startswith(("reviewed-", "proxy-")) for item in registry.assignments
    )


def test_all_legacy_passenger_cars_have_fuel_aware_service_assignments():
    from premise.core.filesystem_constants import DATA_DIR
    from premise.temporal_legacy import load_legacy_registry

    directory = DATA_DIR / "trails" / "temporal"
    registry = TemporalRegistry.from_directory(directory)
    _, mappings = load_legacy_registry(directory)
    cars = {
        (name, product)
        for mapping in mappings
        for name, product in mapping["datasets"]
        if name.casefold().startswith("passenger car,")
    }
    assert len(cars) == 201
    for version in ("3.8", "3.12"):
        for location, kind in (("GB", "selector"), ("RER", "proxy"), ("RoW", "proxy")):
            for name, product in cars:
                result = registry.resolve(
                    {"name": name, "reference product": product, "unit": "unit"},
                    target="supplier",
                    context={"ecoinvent_version": version},
                    caller_record={
                        "name": f"transport, {name.lower()}",
                        "location": location,
                    },
                )
                assert result is not None, (version, location, name)
                assert result.profile_id.startswith("dft-uk-")
                assignment = next(
                    a for a in registry.assignments if a["id"] == result.assignment_id
                )
                assert assignment["kind"] == kind
                technology = name.split(",")[1].strip()
                fuel = (
                    "gas"
                    if technology == "compressed gas"
                    else technology.replace(" ", "-")
                )
                assert (
                    result.profile_id == f"dft-uk-{fuel}-car-construction-vintage-2025"
                )


def test_actual_passenger_car_workbook_has_one_fleet_assignment_per_service():
    from dev.audit_trails_temporal_selectors import workbook_relationships
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    relationships = workbook_relationships(
        [DATA_DIR / "additional_inventories" / "lci-pass_cars.xlsx"]
    )
    services = [
        (caller, exchanges)
        for _, caller, exchanges in relationships
        if caller["name"].startswith("transport, passenger car,")
    ]
    assert len(services) == 74
    for caller, exchanges in services:
        matches = [
            registry.resolve(
                e,
                target="supplier",
                caller_record=caller,
                context={"ecoinvent_version": "3.12"},
            )
            for e in exchanges
        ]
        matches = [m for m in matches if m is not None]
        assert len(matches) == 1, caller
        assert matches[0].profile_id.startswith("dft-uk-")


def test_construction_substeps_exclude_operating_factories_chargers_and_maintenance():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    cases = [
        (
            "Passenger car, fuel cell electric, Medium",
            "fuel tank assembly, compressed hydrogen gas, 700bar",
            True,
        ),
        (
            "Passenger bus, diesel, 13m single deck urban bus, EURO-VI",
            "gearbox, for lorry",
            True,
        ),
        (
            "lorry production, diesel, 26 metric ton",
            "tires and wheels, for lorry",
            True,
        ),
        (
            "photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof",
            "photovoltaics, electric installation for 3kWp module, at building",
            True,
        ),
        (
            "market for photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof",
            "photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof",
            True,
        ),
        (
            "heat and power co-generation unit construction, 50kW electrical, common components for heat+electricity",
            "construction work, heat and power co-generation unit, 160kW electrical",
            True,
        ),
        (
            "photovoltaic cell production, single-Si wafer",
            "photovoltaic cell factory",
            False,
        ),
        (
            "lorry production, battery electric, 26 metric ton",
            "EV charger, level 3, plugin, 200 kW",
            False,
        ),
        (
            "maintenance, passenger car",
            "fuel tank assembly, compressed hydrogen gas, 700bar",
            False,
        ),
        (
            "Passenger car, gasoline, Medium, EURO-6d",
            "Passenger car, gasoline, Medium, EURO-6d",
            False,
        ),
    ]
    for caller, supplier, expected in cases:
        result = registry.resolve(
            {"name": supplier, "reference product": supplier, "unit": "unit"},
            target="supplier",
            context={"ecoinvent_version": "3.12"},
            caller_record={"name": caller, "location": "RER"},
        )
        is_substep = (
            result is not None
            and result.profile_id == "reviewed-new-asset-construction-substep"
        )
        assert is_substep == expected, (caller, supplier)
        if expected:
            profile = registry.profiles[result.profile_id]
            assert profile["distribution"] == {"kind": "fixed", "offset_years": 0}
            assert profile["reference_event"] == "construction"
            assert profile["evidence"]["status"] == "assumption"


def test_runtime_car_fleet_and_assembly_substep_preserve_exchange_amounts(
    monkeypatch, tmp_path
):
    fleet_exchange = {
        "type": "technosphere",
        "name": "Passenger car, battery electric, Medium",
        "product": "Passenger car",
        "unit": "unit",
        "amount": 1e-6,
    }
    component = {
        "type": "technosphere",
        "name": "fuel tank assembly, compressed hydrogen gas, 700bar",
        "product": "fuel tank, compressed hydrogen gas, 700bar",
        "unit": "unit",
        "amount": 2,
    }
    scenario = {
        "model": "test",
        "pathway": "test",
        "year": 2030,
        "database": [
            {
                "name": "transport, passenger car, battery electric, Medium",
                "reference product": "transport, passenger car",
                "location": "RER",
                "exchanges": [fleet_exchange],
            },
            {
                "name": "Passenger car, fuel cell electric, Medium",
                "reference product": "Passenger car",
                "location": "RER",
                "exchanges": [component],
            },
        ],
    }
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj._load_canonical_temporal_data()
    obj.source_version = "3.12"
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)
    obj.add_temporal_distributions()
    expected = obj.temporal_registry.profiles[
        "dft-uk-battery-electric-car-construction-vintage-2025"
    ]["distribution"]
    assert fleet_exchange["temporal_offsets"] == expected["offsets_years"]
    assert fleet_exchange["temporal_weights"] == expected["weights"]
    assert fleet_exchange["amount"] == 1e-6
    assert component["temporal_offsets"] == [0.0]
    assert component["temporal_weights"] == [1.0]
    assert component["amount"] == 2


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


@pytest.mark.parametrize(
    ("caller_name", "location", "exchange_name", "profile_id", "kind"),
    [
        (
            "electricity production, hard coal",
            "US-WECC",
            "hard coal power plant",
            "eia-us-steam-coal-construction-vintage-2025",
            "selector",
        ),
        (
            "electricity production, natural gas, combined cycle power plant",
            "DE",
            "gas power plant, combined cycle, 400MW electrical",
            "eia-us-natural-gas-combined-cycle-construction-vintage-2025",
            "proxy",
        ),
        (
            "electricity production, natural gas, conventional power plant",
            "US-SERC",
            "gas power plant, 100MW electrical",
            "eia-us-natural-gas-steam-turbine-construction-vintage-2025",
            "selector",
        ),
        (
            "electricity production, natural gas, 10MW",
            "CH",
            "gas turbine, 10MW electrical",
            "eia-us-natural-gas-combustion-turbine-construction-vintage-2025",
            "proxy",
        ),
        (
            "electricity production, deep geothermal",
            "US-WECC",
            "geothermal power plant, 5.5MWel",
            "eia-us-geothermal-construction-vintage-2025",
            "selector",
        ),
        (
            "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
            "US-WECC",
            "furnace, wood chips, with silo, 5000kW",
            "eia-us-wood-biomass-construction-vintage-2025",
            "selector",
        ),
    ],
)
def test_packaged_additional_electricity_construction_selectors(
    caller_name, location, exchange_name, profile_id, kind
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {"name": exchange_name, "reference product": exchange_name, "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={"name": caller_name, "location": location},
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assignment = next(
        item for item in registry.assignments if item["id"] == resolution.assignment_id
    )
    assert assignment["kind"] == kind


def test_us_wood_biomass_construction_profile_is_not_a_global_proxy():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": "furnace, wood chips, with silo, 5000kW",
            "reference product": "furnace, wood chips, with silo, 5000kW",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": "heat and power co-generation, wood chips, 6667 kW",
            "location": "DE",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == (
        "gem-gbpt-europe-wood-biomass-construction-vintage-2026"
    )
    assert resolution.profile_id != "eia-us-wood-biomass-construction-vintage-2025"


@pytest.mark.parametrize(
    ("caller_name", "exchange_name", "profile_id"),
    [
        (
            "transport, freight, sea, bulk carrier for dry goods, heavy fuel oil",
            "bulk carrier, for dry goods",
            "unctad-global-bulk-carrier-construction-vintage-2024",
        ),
        (
            "transport, freight, sea, container ship, heavy fuel oil",
            "container ship",
            "unctad-global-container-ship-construction-vintage-2024",
        ),
        (
            "transport, freight, sea, tanker for petroleum, heavy fuel oil",
            "tanker, for petroleum",
            "unctad-global-oil-tanker-construction-vintage-2024",
        ),
        (
            "transport, freight, sea, ferry, heavy fuel oil",
            "ferry",
            "unctad-global-other-ship-types-construction-vintage-2024",
        ),
        (
            "transport, freight, sea, tanker for liquefied natural gas, heavy fuel oil",
            "tanker, for liquefied natural gas",
            "igu-world-lng-carrier-construction-vintage-2026",
        ),
        (
            "transport, freight, sea, tanker for liquid goods other than petroleum and liquefied natural gas, HFO",
            "tanker, for liquid goods other than petroleum and liquefied natural gas",
            "unctad-global-other-ship-types-construction-vintage-2024",
        ),
    ],
)
def test_packaged_global_ship_construction_selectors(
    caller_name, exchange_name, profile_id
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={"name": caller_name, "location": "GLO"},
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    expected_source = "igu" if profile_id.startswith("igu-") else "unctad"
    assert resolution.assignment_id.startswith(f"reviewed-global-{expected_source}-")


@pytest.mark.parametrize(
    ("caller_name", "caller_location", "exchange_name", "unit", "profile_id", "kind"),
    [
        (
            "textile production, cotton, weaving",
            "DE",
            "building, hall, steel construction",
            "square meter",
            "moderate-eu-nonresidential-building-construction-vintage-2020",
            "selector",
        ),
        (
            "textile production, cotton, weaving",
            "IN",
            "building, hall",
            "square meter",
            "moderate-eu-nonresidential-building-construction-vintage-2020",
            "proxy",
        ),
        (
            "building operation, luxury hotel",
            "RER",
            "building, luxury hotel",
            "unit",
            "moderate-eu-hotel-restaurant-building-construction-vintage-2020",
            "selector",
        ),
        (
            "building operation, budget hotel",
            "BR",
            "building, budget hotel",
            "unit",
            "moderate-eu-hotel-restaurant-building-construction-vintage-2020",
            "proxy",
        ),
    ],
)
def test_packaged_european_building_construction_selectors(
    caller_name, caller_location, exchange_name, unit, profile_id, kind
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {"name": exchange_name, "reference product": exchange_name, "unit": unit},
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": caller_name,
            "location": caller_location,
            "unit": "kilogram" if "operation" not in caller_name else "guest night",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assignment = next(
        item for item in registry.assignments if item["id"] == resolution.assignment_id
    )
    assert assignment["kind"] == kind


@pytest.mark.parametrize(
    ("caller_name", "caller_unit"),
    [
        ("aluminium casting facility construction", "unit"),
        ("market for building, hall", "square meter"),
        ("absorption chiller production, 100kW", "unit"),
    ],
)
def test_building_stock_profile_excludes_new_asset_construction(
    caller_name, caller_unit
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": "building, hall, steel construction",
            "reference product": "building, hall, steel construction",
            "unit": "square meter",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": caller_name,
            "location": "RER",
            "unit": caller_unit,
        },
    )

    assert resolution is None


@pytest.mark.parametrize(
    ("location", "exchange_name", "profile_id"),
    [
        (
            "Europe without Switzerland",
            "cement factory",
            "gem-gcct-europe-without-switzerland-cement-plant-construction-vintage-2026",
        ),
        (
            "CH",
            "cement factory",
            "gem-gcct-switzerland-cement-plant-construction-vintage-2026",
        ),
        (
            "CA-QC",
            "integrated cement plant",
            "gem-gcct-canada-cement-plant-construction-vintage-2026",
        ),
        (
            "IN",
            "cement factory",
            "gem-gcct-india-cement-plant-construction-vintage-2026",
        ),
        (
            "RoW",
            "cement factory",
            "gem-gcct-world-cement-plant-construction-vintage-2026",
        ),
    ],
)
def test_packaged_cement_plant_construction_selectors(
    location, exchange_name, profile_id
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": "clinker production",
            "reference product": "clinker",
            "location": location,
            "unit": "kilogram",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assert resolution.assignment_id.startswith("reviewed-")


@pytest.mark.parametrize(
    "caller_name",
    [
        "cement factory construction",
        "market for cement factory",
        "concrete production",
    ],
)
def test_cement_fleet_profiles_exclude_non_operating_callers(caller_name):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": "cement factory",
            "reference product": "cement factory",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": caller_name,
            "location": "CH",
            "unit": "unit",
        },
    )

    assert resolution is None


@pytest.mark.parametrize(
    ("caller_name", "location", "exchange_name", "profile_id"),
    [
        (
            "pig iron production",
            "RER",
            "blast furnace",
            "gem-gist-europe-blast-furnace-construction-vintage-2026",
        ),
        (
            "pig iron production",
            "IN",
            "blast furnace",
            "gem-gist-india-blast-furnace-construction-vintage-2026",
        ),
        (
            "pig iron production",
            "RoW",
            "blast furnace",
            "gem-gist-world-blast-furnace-construction-vintage-2026",
        ),
        (
            "steel production, converter, low-alloyed",
            "RER",
            "blast oxygen furnace converter",
            "gem-gist-europe-basic-oxygen-furnace-construction-vintage-2026",
        ),
        (
            "steel production, converter, low-alloyed",
            "IN",
            "blast oxygen furnace converter",
            "gem-gist-india-basic-oxygen-furnace-construction-vintage-2026",
        ),
        (
            "steel production, converter, unalloyed",
            "RoW",
            "blast oxygen furnace converter",
            "gem-gist-world-basic-oxygen-furnace-construction-vintage-2026",
        ),
        (
            "steel production, electric, low-alloyed",
            "AT",
            "electric arc furnace",
            "gem-gist-europe-electric-arc-furnace-construction-vintage-2026",
        ),
        (
            "steel production, electric, low-alloyed",
            "CA-QC",
            "electric arc furnace",
            "gem-gist-canada-electric-arc-furnace-construction-vintage-2026",
        ),
        (
            "steel production, electric, low-alloyed",
            "IN",
            "electric arc furnace",
            "gem-gist-india-electric-arc-furnace-construction-vintage-2026",
        ),
        (
            "cast iron production",
            "RoW",
            "electric arc furnace",
            "gem-gist-world-electric-arc-furnace-construction-vintage-2026",
        ),
        (
            "sponge iron production",
            "IN",
            "blast furnace",
            "gem-gist-india-direct-reduced-iron-furnace-construction-vintage-2026",
        ),
    ],
)
def test_packaged_iron_and_steel_furnace_construction_selectors(
    caller_name, location, exchange_name, profile_id
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": caller_name,
            "location": location,
            "unit": "kilogram",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assert resolution.assignment_id.startswith("reviewed-")


@pytest.mark.parametrize(
    "caller_name",
    [
        "blast furnace construction",
        "market for electric arc furnace",
        "steel rolling production",
    ],
)
def test_steel_fleet_profiles_exclude_non_operating_callers(caller_name):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": "electric arc furnace",
            "reference product": "electric arc furnace",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": caller_name,
            "location": "RER",
            "unit": "unit",
        },
    )

    assert resolution is None


def test_steel_profiles_flag_sparse_age_coverage_and_small_samples():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    europe_bof = registry.profiles[
        "gem-gist-europe-basic-oxygen-furnace-construction-vintage-2026"
    ]
    canada_eaf = registry.profiles[
        "gem-gist-canada-electric-arc-furnace-construction-vintage-2026"
    ]

    assert "low-known-age-capacity-coverage" in europe_bof["evidence"]["quality_flags"]
    assert "small-sample" in europe_bof["evidence"]["quality_flags"]
    assert "small-sample" in canada_eaf["evidence"]["quality_flags"]


@pytest.mark.parametrize(
    ("caller_name", "location", "exchange_name", "profile_id"),
    [
        (
            "transport, pipeline, long distance, natural gas",
            "DE",
            "pipeline, natural gas, long distance, low capacity, onshore",
            "gem-ggit-europe-natural-gas-transmission-pipeline-construction-vintage-2025",
        ),
        (
            "transport, pipeline, offshore, long distance, natural gas",
            "NO",
            "pipeline, natural gas, long distance, high capacity, offshore",
            "gem-ggit-europe-natural-gas-transmission-pipeline-construction-vintage-2025",
        ),
        (
            "transport, pipeline, onshore, long distance, natural gas",
            "AZ",
            "pipeline, natural gas, long distance, high capacity, onshore",
            "gem-ggit-world-natural-gas-transmission-pipeline-construction-vintage-2025",
        ),
        (
            "transport, pipeline, onshore, petroleum",
            "RER",
            "pipeline, petroleum",
            "gem-goit-europe-oil-ngl-transmission-pipeline-construction-vintage-2026",
        ),
        (
            "transport, pipeline, onshore, petroleum",
            "RoW",
            "pipeline, petroleum",
            "gem-goit-world-oil-ngl-transmission-pipeline-construction-vintage-2026",
        ),
        (
            "transport, pipeline, offshore, petroleum",
            "GLO",
            "pipeline, petroleum, offshore",
            "gem-goit-world-oil-ngl-transmission-pipeline-construction-vintage-2026",
        ),
    ],
)
def test_packaged_transmission_pipeline_construction_selectors(
    caller_name, location, exchange_name, profile_id
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "kilometer",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": caller_name,
            "location": location,
            "unit": "ton kilometer",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assert resolution.assignment_id.startswith("reviewed-")


@pytest.mark.parametrize(
    ("location", "version", "kind"),
    [
        ("DE", "3.10", "selector"),
        ("CN", "3.10", "proxy"),
    ],
)
def test_packaged_freight_wagon_construction_selectors(location, version, kind):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": "goods wagon",
            "reference product": "goods wagon",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": version},
        caller_record={
            "name": "transport, freight train, diesel",
            "reference product": "transport, freight train",
            "location": location,
            "unit": "ton kilometer",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == "ec-eu-freight-wagon-construction-vintage-2019"
    assignment = next(
        item for item in registry.assignments if item["id"] == resolution.assignment_id
    )
    assert assignment["kind"] == kind


def test_freight_wagon_profile_excludes_maintenance_and_newer_inventory_versions():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    caller = {
        "name": "transport, freight train, electricity",
        "reference product": "transport, freight train",
        "location": "DE",
    }
    maintenance = registry.resolve(
        {
            "name": "maintenance, goods wagon",
            "reference product": "maintenance, goods wagon",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.10"},
        caller_record=caller,
    )
    removed_inventory = registry.resolve(
        {"name": "goods wagon", "reference product": "goods wagon", "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record=caller,
    )

    assert maintenance is None
    assert removed_inventory is None


@pytest.mark.parametrize(
    ("caller_name", "location", "exchange_name", "profile_id"),
    [
        (
            "hard coal mine operation and hard coal preparation",
            "PL",
            "mine infrastructure, underground, hard coal",
            "gem-gcmt-europe-underground-coal-mine-construction-vintage-2026",
        ),
        (
            "hard coal mine operation, open cast, dragline",
            "ZA",
            "mine infrastructure, open cast, hard coal",
            "gem-gcmt-world-surface-coal-mine-construction-vintage-2026",
        ),
        (
            "lignite mine operation",
            "IN",
            "mine infrastructure, open cast, lignite",
            "gem-gcmt-world-surface-coal-mine-construction-vintage-2026",
        ),
    ],
)
def test_packaged_coal_mine_construction_selectors(
    caller_name, location, exchange_name, profile_id
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": "hard coal",
            "location": location,
            "unit": "kilogram",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id


@pytest.mark.parametrize(
    "caller_name",
    [
        "market for hard coal",
        "hard coal mine construction",
        "hard coal preparation",
    ],
)
def test_coal_mine_profiles_exclude_non_operation_callers(caller_name):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": "mine infrastructure, underground, hard coal",
            "reference product": "mine infrastructure, underground, hard coal",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": "hard coal",
            "location": "PL",
        },
    )

    assert resolution is None


@pytest.mark.parametrize(
    ("caller_name", "exchange_name"),
    [
        ("pipeline construction, petroleum", "pipeline, petroleum"),
        (
            "market for transport, pipeline, natural gas",
            "pipeline, natural gas, long distance, high capacity, onshore",
        ),
        (
            "transport, pipeline, supercritical carbon dioxide",
            "pipeline, supercritical carbon dioxide",
        ),
        ("transport, pipeline, hydrogen", "pipeline, for hydrogen transmission"),
    ],
)
def test_pipeline_fleet_profiles_exclude_non_transmission_service_relationships(
    caller_name, exchange_name
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "kilometer",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": caller_name,
            "location": "RER",
            "unit": "ton kilometer",
        },
    )

    assert resolution is None


@pytest.mark.parametrize(
    (
        "caller_name",
        "reference_product",
        "location",
        "exchange_name",
        "profile_id",
        "kind",
    ),
    [
        (
            "heat production, natural gas, at boiler atmospheric non-modulating <100kW",
            "heat, central or small-scale, natural gas",
            "Europe without Switzerland",
            "gas boiler",
            "fraunhofer-eu28-natural-gas-boiler-construction-vintage-2012",
            "selector",
        ),
        (
            "heat production, natural gas, at boiler modulating <100kW",
            "heat, central or small-scale, natural gas",
            "RoW",
            "gas boiler",
            "fraunhofer-eu28-natural-gas-boiler-construction-vintage-2012",
            "proxy",
        ),
        (
            "heat production, light fuel oil, at boiler 100kW, non-modulating",
            "heat, central or small-scale, other than natural gas",
            "Europe without Switzerland",
            "oil boiler, 100kW",
            "fraunhofer-eu28-fuel-oil-boiler-construction-vintage-2012",
            "selector",
        ),
        (
            "heat production, hardwood chips from forest, at furnace 50kW",
            "heat, central or small-scale, other than natural gas",
            "CH",
            "furnace, wood chips, with silo, 50kW",
            "fraunhofer-eu28-biomass-furnace-construction-vintage-2012",
            "proxy",
        ),
        (
            "heat production, mixed logs, at wood heater 6kW",
            "heat, central or small-scale, other than natural gas",
            "RoW",
            "furnace, logs, 6kW",
            "fraunhofer-eu28-biomass-stove-construction-vintage-2012",
            "proxy",
        ),
        (
            "heat production, air-water heat pump 10kW",
            "heat, air-water heat pump 10kW",
            "Europe without Switzerland",
            "heat pump, brine-water, 10kW",
            "fraunhofer-eu28-aerial-heat-pump-construction-vintage-2012",
            "selector",
        ),
        (
            "heat production, borehole heat exchanger, brine-water heat pump 10kW",
            "heat, brine-water heat pump 10kW",
            "CH",
            "heat pump, brine-water, 10kW",
            "fraunhofer-eu28-ground-heat-pump-construction-vintage-2012",
            "proxy",
        ),
    ],
)
def test_packaged_european_heating_construction_selectors(
    caller_name, reference_product, location, exchange_name, profile_id, kind
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": reference_product,
            "location": location,
            "unit": "megajoule",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assignment = next(
        item for item in registry.assignments if item["id"] == resolution.assignment_id
    )
    assert assignment["kind"] == kind


@pytest.mark.parametrize(
    ("caller_name", "reference_product", "exchange_name"),
    [
        (
            "heat production, natural gas, at boiler condensing modulating <100kW",
            "heat, central or small-scale, natural gas",
            "gas boiler",
        ),
        (
            "heat production, light fuel oil, at boiler 10kW condensing, non-modulating",
            "heat, central or small-scale, other than natural gas",
            "oil boiler, 10kW",
        ),
        (
            "heat production, wood chips from industry, at furnace 50kW, state-of-the-art 2014",
            "heat, central or small-scale, other than natural gas",
            "furnace, wood chips, with silo, 50kW",
        ),
        (
            "heat production, hardwood chips from forest, at furnace 5000kW",
            "heat, district or industrial, other than natural gas",
            "furnace, wood chips, with silo, 5000kW",
        ),
        (
            "heat production, borehole heat exchanger, brine-water heat pump 10kW",
            "heat, brine-water heat pump 10kW",
            "borehole heat exchanger, 150m",
        ),
        (
            "heat production, at heat pump 30kW, allocation exergy",
            "heat, central or small-scale, other than natural gas",
            "heat pump, 30kW",
        ),
        (
            "heat production, natural gas, at diffusion absorption heat pump 4kW, future",
            "heat, future",
            "heat pump, diffusion absorption, 4kW, future",
        ),
    ],
)
def test_heating_stock_profiles_exclude_unreviewed_relationships(
    caller_name, reference_product, exchange_name
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": reference_product,
            "location": "Europe without Switzerland",
            "unit": "megajoule",
        },
    )

    assert resolution is None


@pytest.mark.parametrize(
    (
        "caller_name",
        "reference_product",
        "location",
        "exchange_name",
        "profile_id",
        "kind",
    ),
    [
        (
            "transport, passenger bus, diesel, 13m single deck urban bus, EURO-VI",
            "transport, passenger bus",
            "RER",
            "Passenger bus, diesel, 13m single deck urban bus, EURO-VI",
            "eu-road-bus-construction-vintage-2024",
            "selector",
        ),
        (
            "transport, passenger bus, diesel, 13m single deck urban bus, EURO-VI",
            "transport, passenger bus",
            "US-WECC",
            "Passenger bus, diesel, 13m single deck urban bus, EURO-VI",
            "eu-road-bus-construction-vintage-2024",
            "proxy",
        ),
        (
            "transport, freight, lorry, diesel, 18 metric ton",
            "transport, freight, lorry",
            "RER",
            "lorry production, diesel, 18 metric ton",
            "eu-road-lorry-over-3.5t-construction-vintage-2024",
            "selector",
        ),
        (
            "transport, freight, lorry, diesel, 3.5 metric ton",
            "transport, freight, lorry",
            "RER",
            "lorry production, diesel, 3.5 metric ton",
            "eu-road-light-commercial-construction-vintage-2024",
            "selector",
        ),
        (
            "transport, freight, lorry, diesel, 18 metric ton",
            "transport, freight, lorry",
            "GLO",
            "lorry production, diesel, 18 metric ton",
            "eu-road-lorry-over-3.5t-construction-vintage-2024",
            "proxy",
        ),
    ],
)
def test_packaged_european_road_vehicle_construction_selectors(
    caller_name,
    reference_product,
    location,
    exchange_name,
    profile_id,
    kind,
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": exchange_name,
            "reference product": exchange_name,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": caller_name,
            "reference product": reference_product,
            "location": location,
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assignment = next(
        item for item in registry.assignments if item["id"] == resolution.assignment_id
    )
    assert assignment["kind"] == kind


def test_trails_applies_european_bus_construction_vintage(monkeypatch, tmp_path):
    from premise.core.filesystem_constants import DATA_DIR

    exchange = {
        "type": "technosphere",
        "name": "Passenger bus, diesel, 13m single deck urban bus, EURO-VI",
        "product": "Passenger bus, EURO-VI",
        "unit": "unit",
        "amount": 1e-7,
    }
    scenario = {
        "model": "test",
        "pathway": "test",
        "year": 2030,
        "database": [
            {
                "name": "transport, passenger bus, diesel, 13m single deck urban bus, EURO-VI",
                "reference product": "transport, passenger bus, EURO-VI",
                "unit": "person kilometer",
                "location": "RER",
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

    profile = obj.temporal_registry.profiles["eu-road-bus-construction-vintage-2024"]
    assert exchange["temporal_distribution"] == 6
    assert exchange["temporal_offsets"] == profile["distribution"]["offsets_years"]
    with (tmp_path / "trails_temp/temporal_selector_matches.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    supplier_row = next(row for row in rows if row["target"] == "supplier")
    assert (
        supplier_row["assignment_id"] == "reviewed-eu-eurostat-bus-construction-vintage"
    )


@pytest.mark.parametrize(
    ("version", "caller_name", "exchange_name"),
    [
        (
            "3.10",
            "transport, passenger aircraft, long haul",
            "aircraft, passenger, long haul",
        ),
        (
            "3.12",
            "transport, passenger, aircraft, long haul",
            "aircraft, passenger, long haul",
        ),
        (
            "3.12",
            "transport, freight, aircraft, belly-freight, long haul",
            "aircraft, belly freight, long haul",
        ),
    ],
)
def test_packaged_aircraft_construction_selector(version, caller_name, exchange_name):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {"name": exchange_name, "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": version},
        caller_record={
            "name": caller_name,
            "reference product": "transport service",
            "location": "GLO",
        },
    )

    assert resolution is not None
    assert resolution.profile_id == (
        "eurostat-eu27-commercial-passenger-aircraft-construction-vintage-2024"
    )
    assert resolution.assignment_id == (
        "proxy-eu-eurostat-commercial-passenger-aircraft-construction-vintage"
    )


@pytest.mark.parametrize(
    ("caller_name", "exchange_name"),
    [
        (
            "transport, freight, aircraft, dedicated freighter, long haul",
            "aircraft, freight, long haul",
        ),
        ("aircraft maintenance, long haul", "aircraft, passenger, long haul"),
        ("airport operation", "aircraft, passenger, long haul"),
    ],
)
def test_aircraft_selector_excludes_unreviewed_relationships(
    caller_name, exchange_name
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {"name": exchange_name, "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={"name": caller_name, "location": "GLO"},
    )

    assert resolution is None


@pytest.mark.parametrize(
    ("version", "caller_name", "location", "exchange_name", "profile_id", "kind"),
    [
        (
            "3.10",
            "transport, freight, inland waterways, barge",
            "RER",
            "barge",
            "ccnr-rhine-dry-cargo-vessel-construction-vintage-2024",
            "selector",
        ),
        (
            "3.12",
            "transport, freight, inland waterways, barge, diesel",
            "RoW",
            "barge",
            "ccnr-rhine-dry-cargo-vessel-construction-vintage-2024",
            "proxy",
        ),
        (
            "3.10",
            "transport, freight, inland waterways, barge tanker",
            "RER",
            "barge tanker",
            "ccnr-rhine-tanker-vessel-construction-vintage-2024",
            "selector",
        ),
        (
            "3.12",
            "transport, freight, inland waterways, barge tanker, diesel",
            "RoW",
            "barge tanker",
            "ccnr-rhine-tanker-vessel-construction-vintage-2024",
            "proxy",
        ),
    ],
)
def test_packaged_inland_vessel_construction_selectors(
    version, caller_name, location, exchange_name, profile_id, kind
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {"name": exchange_name, "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": version},
        caller_record={"name": caller_name, "location": location},
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assignment = next(
        item for item in registry.assignments if item["id"] == resolution.assignment_id
    )
    assert assignment["kind"] == kind


def test_inland_vessel_selector_excludes_maintenance_relationship():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {"name": "barge", "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={"name": "maintenance, barge", "location": "RER"},
    )

    assert resolution is None


@pytest.mark.parametrize(
    ("location", "profile_id"),
    [
        ("ES", "gem-gspt-spain-csp-construction-vintage-2026"),
        ("US-WECC", "gem-gspt-united-states-csp-construction-vintage-2026"),
        ("US-SERC", "gem-gspt-united-states-csp-construction-vintage-2026"),
        ("ZA", "gem-gspt-south-africa-csp-construction-vintage-2026"),
        ("RoW", "gem-gspt-world-csp-construction-vintage-2026"),
    ],
)
def test_packaged_csp_construction_selectors(location, profile_id):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    supplier_name = (
        "concentrated solar power plant, solar thermal parabolic trough, 50 MW"
    )
    resolution = registry.resolve(
        {"name": supplier_name, "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": "electricity production, solar thermal parabolic trough, 50 MW",
            "reference product": "electricity, high voltage",
            "location": location,
        },
    )

    assert resolution is not None
    assert resolution.profile_id == profile_id
    assert resolution.assignment_id.startswith("reviewed-")


@pytest.mark.parametrize("version", ["3.8", "3.12"])
@pytest.mark.parametrize("location,kind", [("NO", "reviewed"), ("BR", "proxy")])
def test_platform_selector_geography_and_offshore_boundary(version, location, kind):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    supplier = {"name": "offshore platform, petroleum", "unit": "unit"}
    caller = {
        "name": "petroleum and gas production, off-shore"
        if version == "3.8"
        else "petroleum and gas production, offshore",
        "location": location,
    }
    result = registry.resolve(
        supplier,
        target="supplier",
        context={"ecoinvent_version": version},
        caller_record=caller,
    )
    assert result is not None
    assert result.assignment_id.startswith(kind + "-")
    caller["name"] = "petroleum and gas production, on-shore"
    assert (
        registry.resolve(
            supplier,
            target="supplier",
            context={"ecoinvent_version": version},
            caller_record=caller,
        )
        is None
    )


@pytest.mark.parametrize(
    "version,caller_name",
    [
        ("3.8", "transport, passenger train"),
        ("3.12", "transport, passenger, train, fleet average"),
        ("3.12", "transport, passenger, train, electric, regional"),
    ],
)
def test_passenger_rail_proxy_and_service_exclusions(version, caller_name):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    supplier = {"name": "train, passenger, long-distance", "unit": "unit"}
    caller = {"name": caller_name, "location": "CH"}
    result = registry.resolve(
        supplier,
        target="supplier",
        context={"ecoinvent_version": version},
        caller_record=caller,
    )
    assert result is not None
    assert result.assignment_id == "proxy-gb-orr-passenger-rail-construction-vintage"
    for excluded in [
        caller_name + ", urban",
        caller_name + ", high-speed",
        "transport, passenger, tram, electric",
        "transport, freight train, diesel",
        "market for train, passenger, long-distance",
    ]:
        caller["name"] = excluded
        assert (
            registry.resolve(
                supplier,
                target="supplier",
                context={"ecoinvent_version": version},
                caller_record=caller,
            )
            is None
        )
    caller["name"] = caller_name
    supplier["name"] = "maintenance, train, passenger, long distance"
    assert (
        registry.resolve(
            supplier,
            target="supplier",
            context={"ecoinvent_version": version},
            caller_record=caller,
        )
        is None
    )


@pytest.mark.parametrize(
    "location,expected",
    [
        ("CH", "gem-gbpt-europe-wood-biomass-construction-vintage-2026"),
        ("RoW", "gem-gbpt-world-wood-biomass-construction-vintage-2026"),
        ("US-WECC", "eia-us-wood-biomass-construction-vintage-2025"),
    ],
)
def test_wood_biomass_geography_precedence_and_component_boundary(location, expected):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    caller = {
        "name": "heat and power co-generation, wood chips, 6667 kW",
        "location": location,
    }
    supplier = {"name": "furnace, wood chips, with silo, 5000kW", "unit": "unit"}
    result = registry.resolve(
        supplier,
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record=caller,
    )
    assert result is not None and result.profile_id == expected
    supplier["name"] = "dust collector, electrostatic precipitator"
    assert (
        registry.resolve(
            supplier,
            target="supplier",
            context={"ecoinvent_version": "3.12"},
            caller_record=caller,
        )
        is None
    )
    if location == "CH":
        supplier["name"] = "furnace, wood chips, with silo, 5000kW"
        caller["name"] += ", state-of-the-art 2014"
        assert (
            registry.resolve(
                supplier,
                target="supplier",
                context={"ecoinvent_version": "3.12"},
                caller_record=caller,
            )
            is None
        )


@pytest.mark.parametrize("location,geography", [("CH", "europe"), ("RoW", "world")])
def test_waste_incineration_profiles_exclude_no_recovery_and_hazardous(
    location, geography
):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    caller = {
        "name": "treatment of municipal solid waste, municipal incineration",
        "location": location,
    }
    supplier = {"name": "municipal waste incineration facility", "unit": "unit"}
    result = registry.resolve(
        supplier,
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record=caller,
    )
    assert result is not None
    assert (
        result.profile_id
        == f"gem-gbpt-{geography}-waste-incineration-construction-vintage-2026"
    )
    caller["name"] += ", without energy recovery"
    assert (
        registry.resolve(
            supplier,
            target="supplier",
            context={"ecoinvent_version": "3.12"},
            caller_record=caller,
        )
        is None
    )
    caller["name"] = "treatment of spent solvent mixture, hazardous waste incineration"
    supplier["name"] = "hazardous waste incineration facility"
    assert (
        registry.resolve(
            supplier,
            target="supplier",
            context={"ecoinvent_version": "3.12"},
            caller_record=caller,
        )
        is None
    )


@pytest.mark.parametrize(
    "location,geography",
    [
        ("GB", "united-kingdom"),
        ("DE", "germany"),
        ("CN-FJ", "china"),
        ("DK", "europe"),
        ("RoW", "world"),
    ],
)
def test_offshore_structure_geography_and_moving_parts_boundary(location, geography):
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    caller = {
        "name": "electricity production, wind, 1-3MW turbine, offshore",
        "location": location,
    }
    supplier = {"name": "wind power plant, 2MW, offshore, fixed parts", "unit": "unit"}
    result = registry.resolve(
        supplier,
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record=caller,
    )
    assert result is not None
    assert (
        result.profile_id
        == f"gem-gwpt-{geography}-offshore-fixed-structure-vintage-2026"
    )
    supplier["name"] = "wind power plant, 2MW, offshore, moving parts"
    assert (
        registry.resolve(
            supplier,
            target="supplier",
            context={"ecoinvent_version": "3.12"},
            caller_record=caller,
        )
        is None
    )


def test_lng_carrier_specific_profile_overrides_other_ships_and_chemical_sites_unassigned():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    result = registry.resolve(
        {"name": "tanker, for liquefied natural gas", "unit": "unit"},
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": "transport, freight, sea, tanker for liquefied natural gas, heavy fuel oil",
            "location": "GLO",
        },
    )
    assert (
        result is not None
        and result.profile_id == "igu-world-lng-carrier-construction-vintage-2026"
    )
    assert (
        registry.profiles[result.profile_id]["evidence"]["coverage"][
            "included_carrier_count"
        ]
        == 734
    )
    assert not any("gem-gcht" in a["profile"] for a in registry.assignments)
    assert all(
        p["review"]["activation_status"] == "evidence_only"
        for k, p in registry.profiles.items()
        if k.startswith("gem-gcht-")
    )


def test_csp_selector_excludes_component_construction():
    from premise.core.filesystem_constants import DATA_DIR

    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    resolution = registry.resolve(
        {
            "name": "concentrated solar power plant, solar thermal parabolic trough, 50 MW",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": "collector field area construction, solar thermal parabolic trough, 50 MW",
            "location": "ZA",
        },
    )

    assert resolution is None
