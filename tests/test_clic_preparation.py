"""Reviewed preparation recipes use owned background data and public inputs."""

import copy
import math
from types import SimpleNamespace

import pytest
import numpy as np

from premise.inventory import clic_preparation as module
from premise.inventory.clic_archive import (
    ClicInventoryError,
    ClicRecipe,
    activity_identity,
)
from premise.inventory.clic_requirements import registry


def activity(name, product=None, location="GLO", unit="kilogram", **kwargs):
    row = dict(
        name=name, location=location, unit=unit, database="background", code=name
    )
    row["reference product"] = product or name
    row["exchanges"] = [{**row, "type": "production", "amount": 1}]
    row.update(kwargs)
    return row


def node(file, name, product=None, location="GLO", unit="kilogram"):
    return {"id": 0, "file": file, "identity": [name, product or name, location, unit]}


def prepare(selected, background=(), public=(), graph=None, **kwargs):
    graph = graph or {"nodes": [selected], "roles": {}}
    return module.prepare_recipes(
        public,
        background,
        graph,
        [selected["id"]],
        target={"version": "3.12"},
        **kwargs,
    )


def test_recipe_selection_preserves_historical_lockfiles_and_cutoff_scope():
    assert module.use_preparation_recipes(ClicRecipe(versions={}))
    assert not module.use_preparation_recipes(
        ClicRecipe(versions={}), system_model="consequential"
    )
    assert not module.use_preparation_recipes(ClicRecipe(versions={}), family="uvek")
    old = SimpleNamespace(
        resolution_fingerprint="locked", derived_inventories={}, workbooks={}
    )
    assert not module.use_preparation_recipes(old)
    old.derived_inventories = {module.DERIVATION: "pin"}
    assert module.use_preparation_recipes(old)
    old.workbooks = {"lci-PGM.xlsx": "hash"}
    assert not module.use_preparation_recipes(old)


@pytest.mark.parametrize(
    "file,product,location,share",
    [
        ("lci-PGM.xlsx", "ruthenium", "ZA", 0.03),
        ("lci-PGM.xlsx", "iridium", "RU", 0.13),
        ("lci-rhenium.xlsx", "rhenium", "CL", 0.0007),
    ],
)
def test_metal_allocation_uses_loaded_factor_and_preserves_signed_uncertainty(
    file, product, location, share
):
    pgm = product != "rhenium"
    name = (
        "platinum group metal, extraction and refinery operations"
        if pgm
        else "copper mine operation and beneficiation, sulfide ore"
    )
    donor = activity(
        name, "platinum" if pgm else "copper concentrate, sulfide ore", location
    )
    donor["exchanges"][0]["source allocation factor"] = 0.25
    donor["exchanges"].append(
        {
            "type": "technosphere",
            "amount": -4,
            "uncertainty type": 2,
            "loc": math.log(4),
            "negative": True,
            "scale": 0.1,
            "input": ("background", "scrap"),
        }
    )
    before = copy.deepcopy(donor)
    selected = node(file, name, product, location)
    records, summary = prepare(selected, [donor])
    edge = records[0]["exchanges"][1]
    assert edge["amount"] == pytest.approx(-4 * share / 0.25)
    assert edge["loc"] == pytest.approx(math.log(abs(edge["amount"])))
    assert edge["negative"] is True
    assert donor == before
    repeated, replay = prepare(
        selected, [donor], pins={module.DERIVATION: summary["fingerprint"]}
    )
    assert repeated == records
    donor["exchanges"][0]["source allocation factor"] = 0.5
    with pytest.raises(ClicInventoryError, match="locked reconstruction"):
        prepare(selected, [donor], pins={module.DERIVATION: summary["fingerprint"]})
    donor["exchanges"][0].pop("source allocation factor")
    with pytest.raises(ClicInventoryError, match="allocation factor"):
        prepare(selected, [donor])


def test_boiler_requires_unique_native_supplier_and_published_efficiency():
    selected = node(
        "lci-electric-boiler-industrial.xlsx",
        "heat production, electric boiler, industrial",
        "heat, district or industrial",
        "CH",
        "megajoule",
    )
    donor = activity(
        "market for electricity, medium voltage",
        "electricity, medium voltage",
        "CH",
        "kilowatt hour",
    )
    records, _ = prepare(selected, [donor])
    assert records[0]["exchanges"][1]["amount"] == pytest.approx(1 / (0.99 * 3.6))
    assert len(records[0]["exchanges"]) == 2
    for donors in ([], [donor, copy.deepcopy(donor)]):
        with pytest.raises(ClicInventoryError, match="loaded background supplier"):
            prepare(selected, donors)


@pytest.mark.parametrize("number", [float, np.float32])
def test_capture_budget_uses_current_donor_emissions_and_offsets_once(number):
    selected = node(
        "lci-hydrogen-oil.xlsx",
        "hydrogen production, gaseous, petroleum refinery operation, with CCS",
        "hydrogen, gaseous, 30 bar",
        "RER",
    )
    donor = activity(
        "hydrogen production, gaseous, petroleum refinery operation",
        "hydrogen, gaseous, low pressure",
        "Europe without Switzerland",
    )
    donor["exchanges"].extend(
        [
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "unit": "kilogram",
                "amount": number(v),
                "categories": ("air",),
                "uncertainty type": 2,
                "loc": math.log(v),
                "negative": False,
                "input": ("bio", str(v)),
            }
            for v in (1.23456789, 2.34567891)
        ]
    )
    donor["exchanges"][0]["amount"] = number(1)
    donor["source revision"] = np.int64(1)
    capture_name = module.capture_name("refinery")
    capture = activity(
        capture_name,
        location="RER",
        **{"clic provenance": {"slug": "capture", "version_id": 1, "sha256": "hash"}},
    )
    graph = {
        "nodes": [
            selected,
            {"identity": list(activity_identity(capture)), "clic_slug": "capture"},
        ],
        "roles": {},
    }
    records, _ = prepare(selected, [donor], [capture], graph=graph)
    edges = records[-1]["exchanges"]
    captured = 0.9 * sum(
        float(e["amount"]) for e in donor["exchanges"] if e["type"] == "biosphere"
    )
    assert edges[2]["amount"] == pytest.approx(captured, rel=1e-14)
    assert sum(e["amount"] for e in edges if e["type"] == "biosphere") == pytest.approx(
        -captured, rel=1e-14
    )
    assert all(e["negative"] for e in edges if e["type"] == "biosphere")


def test_rail_converts_reference_service_once():
    selected = node(
        "lci-fuel_cell.xlsx",
        "fuel cell system assembly production, rail transport, 860 kwe",
        "fuel cell system assembly, rail transport, 860 kWe",
        unit="unit",
    )
    slug = module.configuration()["clic_candidates"]["rail_fuel_cell"]
    public = activity(
        "generic assembly",
        unit="unit",
        **{"clic provenance": {"slug": slug, "version_id": 1, "sha256": "hash"}},
    )
    records, summary = prepare(selected, public=[public])
    assert records[-1]["exchanges"][1]["amount"] == 1290
    assert summary["decisions"][0]["reference"]["owner_reference_hours"] == 30000


def test_lifecycle_alias_adopts_public_record_without_duplicate_foreground():
    selected = node(
        "lci-electric-car-mechanical-powertrain.xlsx",
        "market for mechanical powertrain, electric passenger car",
        "mechanical powertrain, electric passenger car",
    )
    slug = module.configuration()["clic_candidates"]["electric_mechanical"]
    public = activity(
        "published lifecycle",
        **{"clic provenance": {"slug": slug, "version_id": 1, "sha256": "hash"}},
    )
    graph = {
        "nodes": [selected],
        "roles": {"transport_passenger_cars.yaml": {"Electric": [0]}},
    }
    records, summary = prepare(selected, public=[public], graph=graph)
    assert len(records) == 1 and summary["activity_count"] == 0
    assert records[0]["clic provenance"]["prepared_aliases"] == [selected["identity"]]
    assert records[0]["clic provenance"]["derived_role_bindings"] == [
        "transport_passenger_cars.yaml:Electric"
    ]
    assert "prepared_aliases" not in public["clic provenance"]
    selected["identity"][0] = "treatment of used combustion equipment"
    with pytest.raises(ClicInventoryError, match="Standalone legacy treatment"):
        prepare(selected)


def test_preparation_does_not_intercept_other_published_vehicle_components():
    assert (
        module.recipe_for_node(
            node(
                "lci-electric-car-mechanical-powertrain.xlsx",
                "charger production, road vehicle, onboard",
            )
        )
        is None
    )


def test_reviewed_public_prerequisites_are_unambiguous():
    graph = registry()["hybrid_sources"]
    for selected in graph["nodes"]:
        rule = module.recipe_for_node(selected)
        if rule is not None:
            module.required_recipe_roots(rule, graph)


def test_motorcycle_gearbox_preserves_scrap_and_component_boundary():
    selected = node(
        "lci-two_wheelers.xlsx",
        "mechanical powertrain production, motorcycle, electric",
        "mechanical powertrain, motorcycle, electric",
    )
    donors = []
    for material, scrap, working in [
        ("steel, low-alloyed", "iron scrap, unsorted", "steel"),
        ("aluminium, cast alloy", "aluminium scrap, new", "aluminium"),
    ]:
        donors.extend(
            [
                activity(f"market for {material}", material),
                activity(
                    f"metal working, average for {working} product manufacturing",
                    location="RER",
                ),
                activity(f"{scrap}, Recycled Content cut-off", scrap),
            ]
        )
    donors.append(
        activity(
            "market group for electricity, medium voltage",
            "electricity, medium voltage",
            unit="kilowatt hour",
        )
    )
    records, _ = prepare(selected, donors)
    edges = records[0]["exchanges"][1:]
    metal = [e for e in edges if e["name"].startswith("market for")]
    scrap = [e for e in edges if "cut-off" in e["name"]]
    assert sum(e["amount"] for e in metal) == pytest.approx(1.25)
    assert sum(e["amount"] for e in scrap) == pytest.approx(-0.25)
    assert len(edges) == 7 and edges[-1]["amount"] == 1


def test_afforestation_uses_net_loaded_carbon_uptake():
    selected = node(
        "lci-afforestation.xlsx",
        "afforestation, eucalyptus",
        "carbon dioxide, captured",
        "RoW",
    )
    donor = activity(
        "hardwood forestry, eucalyptus ssp., planted forest management",
        "cleft timber, green, measured as dry mass",
        "RoW",
    )
    donor["exchanges"].extend(
        [
            {
                "type": "biosphere",
                "name": "Carbon dioxide, in air",
                "unit": "kilogram",
                "amount": 2,
            },
            {
                "type": "biosphere",
                "name": "Carbon dioxide, non-fossil",
                "unit": "kilogram",
                "amount": 0.5,
                "categories": ("air",),
            },
        ]
    )
    records, _ = prepare(selected, [donor])
    assert records[0]["exchanges"][1]["amount"] == pytest.approx(1 / 1.5)
    donor["exchanges"][-1]["amount"] = 3
    with pytest.raises(ClicInventoryError, match="positive net CO2"):
        prepare(selected, [donor])


def test_explicit_preparation_roles_bind_renamed_public_record():
    from premise.inventory.activity_maps import InventorySet

    public = activity(
        "new public name",
        **{
            "clic provenance": {
                "slug": "new-slug",
                "role": "root",
                "role_bindings": ["test:role"],
                "derived_role_bindings": ["test:role"],
            }
        },
    )
    unrelated = activity(
        "old inventory name",
        **{"clic provenance": {"slug": "old-slug", "role_bindings": []}},
    )
    definitions = {
        "reviewed": {
            "fltr": "old inventory name",
            "clic_slugs": ["old-slug"],
            "clic_role_key": "test:role",
        }
    }
    assert InventorySet([public, unrelated]).generate_sets_from_filters(definitions)[
        "reviewed"
    ] == [public]


def test_production_allocation_survives_default_extraction():
    from premise.inventory.clean_datasets import _extract_exchange_for_premise

    proxy = SimpleNamespace(
        data={
            "amount": 1,
            "properties": {
                "allocation factor": {"amount": 0.42},
                "unneeded": {"amount": 99},
            },
        },
        type="production",
        input_database="db",
        input_code="a",
        output_database="db",
        output_code="a",
    )
    extracted = _extract_exchange_for_premise(proxy)
    assert extracted["source allocation factor"] == 0.42
    assert "properties" not in extracted


@pytest.mark.parametrize("version", ["3.7", "3.8", "3.9", "3.10", "3.11", "3.12"])
def test_maximal_cutoff_plan_reads_only_five_retained_workbook_families(version):
    from premise.database.new_database import NewDatabase
    from premise.inventory.clic_hybrid import plan_hybrid_inventories

    sources = NewDatabase._default_inventory_sources(
        SimpleNamespace(version=version, system_model="cutoff")
    )
    plan = plan_hybrid_inventories(
        list(registry()["sector_mappings"]) + ["battery", "metals", "mining"],
        [],
        sources,
        version=version,
        system_model="cutoff",
        prepare_background=True,
    )
    assert {path.name for path, _ in plan.sources} == {
        "lci-co-firing-power-plants.xlsx",
        "lci-battery-capacity.xlsx",
        "lci-geothermal.xlsx",
        "lci-final-energy.xlsx",
        "lci-steel.xlsx",
    }
    assert len(plan.selected_nodes) == 150
    assert len(plan.prepared_nodes) == (25 if version in {"3.11", "3.12"} else 23)
