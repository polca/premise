"""Regressions for normalized Premise supplier identities and IAM regions."""

from types import SimpleNamespace
import importlib
import pytest
from premise.core.filesystem_constants import DATA_DIR
from premise.temporal_specs import TemporalRegistry

# Activity names and reference products are intentionally different here.
NORMALIZED_RELATIONSHIPS = [
    (
        "market for wind power plant, 800kW, fixed parts",
        "wind power plant, 800kW, fixed parts",
        "unit",
        "GLO",
        "electricity production, wind, <1MW turbine, onshore",
        "electricity, high voltage",
        "kilowatt hour",
        "RoW",
        "eia-us-onshore-wind-construction-vintage-2025",
    ),
    (
        "market for hydropower plant, run-of-river",
        "hydropower plant, run-of-river",
        "unit",
        "GLO",
        "electricity production, hydro, run-of-river",
        "electricity, high voltage",
        "kilowatt hour",
        "CN-SH",
        "eia-us-conventional-hydropower-construction-vintage-2025",
    ),
    (
        "market for nuclear power plant, pressure water reactor, 1000MW",
        "nuclear power plant, pressure water reactor, 1000MW",
        "unit",
        "GLO",
        "electricity production, nuclear, pressure water reactor",
        "electricity, high voltage",
        "kilowatt hour",
        "GB",
        "eia-us-nuclear-construction-vintage-2025",
    ),
    (
        "market for gas power plant, combined cycle, 400MW electrical",
        "gas power plant, combined cycle, 400MW electrical",
        "unit",
        "GLO",
        "electricity production, natural gas, combined cycle power plant",
        "electricity, high voltage",
        "kilowatt hour",
        "CA-ON",
        "eia-us-natural-gas-combined-cycle-construction-vintage-2025",
    ),
    (
        "market for container ship",
        "container ship",
        "unit",
        "GLO",
        "transport, freight, sea, container ship, powered with hydrogen",
        "transport, freight, sea, container ship",
        "ton kilometer",
        "RER",
        "unctad-global-container-ship-construction-vintage-2024",
    ),
    (
        "market for tanker, for liquefied natural gas",
        "tanker, for liquefied natural gas",
        "unit",
        "GLO",
        "transport, freight, sea, tanker for liquefied natural gas, heavy fuel oil",
        "transport, freight, sea, tanker for liquefied natural gas, heavy fuel oil",
        "ton kilometer",
        "GLO",
        "igu-world-lng-carrier-construction-vintage-2026",
    ),
    (
        "market for pipeline, natural gas, long distance, low capacity, onshore",
        "pipeline, natural gas, long distance, low capacity, onshore",
        "kilometer",
        "GLO",
        "transport, pipeline, long distance, natural gas",
        "transport, pipeline, long distance, natural gas",
        "ton kilometer",
        "NL",
        "gem-ggit-europe-natural-gas-transmission-pipeline-construction-vintage-2025",
    ),
    (
        "market for gas boiler",
        "gas boiler",
        "unit",
        "GLO",
        "heat production, natural gas, at boiler modulating <100kW",
        "heat, central or small-scale, natural gas",
        "megajoule",
        "Europe without Switzerland",
        "fraunhofer-eu28-natural-gas-boiler-construction-vintage-2012",
    ),
    (
        "market for heat pump, brine-water, 10kW",
        "heat pump, brine-water, 10kW",
        "unit",
        "GLO",
        "heat production, air-water heat pump 10kW",
        "heat, air-water heat pump 10kW",
        "megajoule",
        "CAZ",
        "fraunhofer-eu28-aerial-heat-pump-construction-vintage-2012",
    ),
    (
        "market for train, passenger, regional",
        "train, passenger, regional",
        "unit",
        "GLO",
        "transport, passenger, train, electric, regional",
        "transport, passenger, train, fleet average",
        "person kilometer",
        "CH",
        "orr-gb-passenger-rail-construction-vintage-2026",
    ),
    (
        "market for mine infrastructure, open cast, lignite",
        "mine infrastructure, open cast, lignite",
        "unit",
        "GLO",
        "lignite mine operation",
        "lignite",
        "kilogram",
        "IN",
        "gem-gcmt-world-surface-coal-mine-construction-vintage-2026",
    ),
    (
        "market for furnace, wood chips, with silo, 5000kW",
        "furnace, wood chips, with silo, 5000kW",
        "unit",
        "GLO",
        "heat and power co-generation, wood chips, 6667 kW",
        "electricity, high voltage",
        "kilowatt hour",
        "EE",
        "gem-gbpt-europe-wood-biomass-construction-vintage-2026",
    ),
]


@pytest.mark.parametrize(
    "supplier_name,product,unit,supplier_location,caller_name,caller_product,caller_unit,caller_location,profile_id",
    NORMALIZED_RELATIONSHIPS,
)
def test_export_assigns_reviewed_profile_to_normalized_supplier(
    supplier_name,
    product,
    unit,
    supplier_location,
    caller_name,
    caller_product,
    caller_unit,
    caller_location,
    profile_id,
    monkeypatch,
    tmp_path,
):
    trails = importlib.import_module("premise.trails")
    exchange = {
        "type": "technosphere",
        "name": supplier_name,
        "product": product,
        "unit": unit,
        "location": supplier_location,
        "amount": 0.25,
    }
    scenario = {
        "model": "test",
        "pathway": "test",
        "year": 2025,
        "database": [
            {
                "name": caller_name,
                "reference product": caller_product,
                "unit": caller_unit,
                "location": caller_location,
                "exchanges": [exchange],
            }
        ],
    }
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj._load_canonical_temporal_data()
    obj.source_version = "3.12"
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="test")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)
    obj.add_temporal_distributions()
    expected = obj.temporal_registry.profiles[profile_id]["distribution"]
    assert exchange["temporal_distribution"] == 6
    assert exchange["temporal_offsets"] == expected["offsets_years"]
    assert exchange["temporal_weights"] == expected["weights"]
    assert exchange["amount"] == 0.25


@pytest.mark.parametrize("location", ["World", "EUR", "CHA"])
@pytest.mark.parametrize(
    "caller_name,product,profile",
    [
        (
            "clinker production",
            "cement factory",
            "gem-gcct-world-cement-plant-construction-vintage-2026",
        ),
        (
            "pig iron production",
            "blast furnace",
            "gem-gist-world-blast-furnace-construction-vintage-2026",
        ),
        (
            "steel production, converter, unalloyed",
            "blast oxygen furnace converter",
            "gem-gist-world-basic-oxygen-furnace-construction-vintage-2026",
        ),
        (
            "steel production, electric, low-alloyed",
            "electric arc furnace",
            "gem-gist-world-electric-arc-furnace-construction-vintage-2026",
        ),
    ],
)
def test_world_construction_proxy_covers_iam_regions(
    location, caller_name, product, profile
):
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    result = registry.resolve(
        {
            "name": product + " construction",
            "reference product": product,
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={"name": caller_name, "location": location},
    )
    assert result is not None and result.profile_id == profile
    assignment = next(
        a for a in registry.assignments if a["id"] == result.assignment_id
    )
    assert assignment["kind"] == "proxy"


def test_direct_region_retains_precedence_over_world_proxy():
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    result = registry.resolve(
        {
            "name": "blast oxygen furnace converter production",
            "reference product": "blast oxygen furnace converter",
            "unit": "unit",
        },
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": "steel production, converter, unalloyed",
            "location": "RER",
        },
    )
    assert (
        result.profile_id
        == "gem-gist-europe-basic-oxygen-furnace-construction-vintage-2026"
    )
