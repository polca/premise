"""Country PV derivation uses pinned CLIC inputs rather than Excel recipes."""

import copy
import json

import pytest

from premise.electricity import photovoltaic
from premise.inventory import clic_hybrid
from premise.inventory.clic_archive import (
    ClicInventoryError,
    ClicRecipe,
    activity_identity,
)
from premise.inventory.clic_harmonize import HarmonizedClicInventory
from premise.inventory.clic_photovoltaic import (
    CORE_FILE,
    COUNTRY_FILE,
    DERIVATION,
    derive_country_inventories,
    use_derived_photovoltaics,
)


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    parameters = {
        "source": "Synthetic PV reference",
        "generation_source": "Synthetic generation",
        "atlas_source": "Synthetic yields",
        "lifetime_years": 30,
        "report_yields": {"CH": {"roof": 1000, "ground": 1000, "facade": 660}},
        "atlas_ground_yields": {"AA": 1200},
        "yield_factors": {"roof": 0.94, "ground": 1, "facade": 0.66},
        "locations": ["CH", "AA"],
    }
    parameter_path = tmp_path / "pv_2026_parameters.json"
    parameter_path.write_text(json.dumps(parameters))
    (tmp_path / "pv_generation_2023.csv").write_text(
        "location,generation_gwh\nCH,100\n"
    )
    monkeypatch.setattr(photovoltaic, "PV_PARAMETERS_PATH", parameter_path)
    core = []
    for capacity in (10, 250):
        name = f"photovoltaic installation construction, {capacity} kwp, slanted-roof, single-crystalline silicon, panel"
        core.append(
            {
                "name": name,
                "reference product": name,
                "location": "GLO",
                "unit": "unit",
                "code": str(capacity),
                "exchanges": [
                    {
                        "name": "photovoltaic panel production",
                        "unit": "square meter",
                        "amount": capacity * 5,
                        "type": "technosphere",
                    }
                ],
            }
        )
    report = {
        "name": "electricity production mix, photovoltaic",
        "reference product": "electricity, photovoltaic",
        "location": "CH",
        "unit": "kilowatt hour",
        "code": "report",
        "exchanges": [
            {
                **dict(
                    zip(
                        ("name", "reference product", "location", "unit"),
                        activity_identity(a),
                    )
                ),
                "type": "technosphere",
                "amount": 0.5 / (capacity * 1000 * 30),
            }
            for a, capacity in zip(core, (10, 250))
        ]
        + [
            {
                "name": "market for tap water",
                "reference product": "tap water",
                "location": "CH",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 0.1,
            },
            {
                "name": "treatment of wastewater",
                "reference product": "wastewater",
                "location": "CH",
                "unit": "cubic meter",
                "type": "technosphere",
                "amount": -0.00009,
            },
            {
                "name": "Water",
                "unit": "cubic meter",
                "type": "biosphere",
                "categories": ("air",),
                "amount": 0.00001,
            },
        ],
    }
    core.append(report)
    for activity in core:
        activity["exchanges"].insert(
            0,
            {
                **dict(
                    zip(
                        ("name", "reference product", "location", "unit"),
                        activity_identity(activity),
                    )
                ),
                "type": "production",
                "amount": 1,
            },
        )
    nodes = []
    for index, activity in enumerate(core):
        slug = f"reviewed-2026-input-{index}"
        activity["clic provenance"] = {
            "slug": slug,
            "version_id": index + 1,
            "sha256": str(index) * 64,
        }
        nodes.append(
            {
                "id": index,
                "file": CORE_FILE,
                "identity": list(activity_identity(activity)),
                "clic_slug": slug,
                "inputs": [],
                "science_hash": str(index),
            }
        )
    for segment, product in (
        ("commercial", "electricity, medium voltage"),
        ("residential", "electricity, low voltage"),
    ):
        nodes.append(
            {
                "id": len(nodes),
                "file": COUNTRY_FILE,
                "identity": [
                    f"electricity production, photovoltaic, {segment}",
                    product,
                    "AA",
                    "kilowatt hour",
                ],
                "clic_slug": "obsolete-2020" if segment == "residential" else None,
                "inputs": [],
                "science_hash": segment,
            }
        )
    graph = {
        "nodes": nodes,
        "files": {CORE_FILE: {"sha256": "a" * 64}, COUNTRY_FILE: {"sha256": "b" * 64}},
        "roles": {
            "electricity.yaml": {
                "Solar PV Centralized": [3],
                "Solar PV Residential": [4],
            }
        },
    }
    prepared = HarmonizedClicInventory(
        core,
        {
            "target": {
                "technosphere": {
                    "family": "ecoinvent",
                    "version": "3.12",
                    "system_model": "cutoff",
                },
                "biosphere": {"family": "ecoinvent", "version": "3.12"},
            }
        },
        {},
    )
    return prepared, graph


def test_derived_recipes_preserve_inputs_water_balance_and_recipe_pins(inputs):
    prepared, graph = inputs
    original = copy.deepcopy(prepared)
    result = derive_country_inventories(prepared, graph, [3, 4])
    assert prepared == original
    assert len(result.activities) == 5
    for record in result.activities[-2:]:
        edges = record["exchanges"]
        plant = next(
            e for e in edges if e["type"] == "technosphere" and e["unit"] == "unit"
        )
        expected_code = "10" if "residential" in record["name"] else "250"
        assert plant["input"] == ("__clic__", expected_code)
        water = next(e["amount"] for e in edges if e["name"] == "market for tap water")
        wastewater = next(
            e["amount"] for e in edges if e["name"] == "treatment of wastewater"
        )
        evaporation = next(e["amount"] for e in edges if e["name"] == "Water")
        assert water == pytest.approx(1000 * (-wastewater + evaporation))
        assert record["clic provenance"]["derived_role_bindings"]
    recipe = ClicRecipe.read(result.lockfile)
    replay = derive_country_inventories(
        prepared, graph, [3, 4], pins=recipe.derived_inventories
    )
    assert result == replay
    changed = copy.deepcopy(prepared)
    changed.activities[0]["clic provenance"]["sha256"] = "f" * 64
    with pytest.raises(ClicInventoryError, match="locked recipe"):
        derive_country_inventories(
            changed, graph, [3, 4], pins=recipe.derived_inventories
        )
    with pytest.raises(ClicInventoryError, match="locked recipe"):
        derive_country_inventories(
            prepared, graph, [3], pins=recipe.derived_inventories
        )


def test_missing_or_renamed_2026_input_never_uses_a_workbook(inputs):
    prepared, graph = inputs
    prepared.activities.pop()
    with pytest.raises(ClicInventoryError, match="missing or changed identity"):
        derive_country_inventories(prepared, graph, [3, 4])


def test_changed_production_reference_is_not_silently_rescaled(inputs):
    prepared, graph = inputs
    prepared.activities[0]["exchanges"][0]["amount"] = 2
    with pytest.raises(ClicInventoryError, match="one-unit production reference"):
        derive_country_inventories(prepared, graph, [3, 4])


def test_legacy_hydrogen_label_wraps_one_kwh_without_capacity_scaling(inputs):
    from premise.inventory.clic_photovoltaic import COMPATIBILITY_NAME, REFERENCE_NAME

    prepared, graph = inputs
    reference = copy.deepcopy(prepared.activities[2])
    reference["name"] = REFERENCE_NAME
    reference["location"] = "GLO"
    reference["code"] = "reference-1000"
    reference["clic provenance"] = {
        "slug": "reference-2026",
        "sha256": "f" * 64,
        "version_id": 12,
    }
    prepared.activities.append(reference)
    graph["nodes"].append(
        {
            "id": 5,
            "file": CORE_FILE,
            "identity": list(activity_identity(reference)),
            "clic_slug": "reference-2026",
        }
    )
    identity = [COMPATIBILITY_NAME, COMPATIBILITY_NAME, "CH", "kilowatt hour"]
    graph["nodes"].append(
        {
            "id": 6,
            "file": COUNTRY_FILE,
            "identity": identity,
            "clic_slug": "old-reference-2020",
        }
    )
    result = derive_country_inventories(prepared, graph, [6])
    record = result.activities[-1]
    assert activity_identity(record) == tuple(identity)
    assert len(record["exchanges"]) == 2
    assert [edge["amount"] for edge in record["exchanges"]] == [1, 1]
    assert record["exchanges"][1]["input"] == ("__clic__", "reference-1000")
    assert [
        row["slug"] for row in result.manifest["derived_inventories"][0]["inputs"]
    ] == ["reference-2026"]


def test_plan_selects_2026_sources_without_country_workbook(
    inputs, monkeypatch, tmp_path
):
    _, graph = inputs
    specification = {
        "hybrid_sources": graph,
        "sector_mappings": {"electricity": ["electricity.yaml"]},
    }
    monkeypatch.setattr(clic_hybrid, "registry", lambda: specification)
    monkeypatch.setattr(
        clic_hybrid, "required_roots", lambda *a, **k: ("obsolete-2020",)
    )
    import premise.inventory.activity_maps as maps
    import premise.inventory.profiles as profiles

    monkeypatch.setattr(
        maps,
        "get_mapping",
        lambda *a, **k: {"Solar PV Centralized": {}, "Solar PV Residential": {}},
    )
    monkeypatch.setattr(profiles, "profiles", lambda *a, **k: [])
    sources = [(tmp_path / CORE_FILE, "3.12"), (tmp_path / COUNTRY_FILE, "3.12")]
    plan = clic_hybrid.plan_hybrid_inventories(["electricity"], [], sources)
    assert plan.sources == ()
    assert plan.selected_nodes == frozenset()
    assert plan.derived_nodes == frozenset({3, 4})
    assert plan.clic_roots == tuple(f"reviewed-2026-input-{i}" for i in range(3))
    legacy = clic_hybrid.plan_hybrid_inventories(
        ["electricity"], [], sources, derive_photovoltaics=False
    )
    assert legacy.derived_nodes == frozenset()
    assert legacy.selected_nodes == frozenset({3})
    assert legacy.sources == (sources[1],)


def test_derivation_recipe_rejects_invalid_fingerprints():
    with pytest.raises(ClicInventoryError):
        ClicRecipe.read({"derived_inventories": {DERIVATION: "not-a-hash"}})


def test_historical_complete_recipe_keeps_its_original_pv_graph():
    assert use_derived_photovoltaics(ClicRecipe.read(None))
    assert not use_derived_photovoltaics(
        ClicRecipe.read({"resolution_fingerprint": "old-graph"})
    )
    assert not use_derived_photovoltaics(
        ClicRecipe.read({"workbooks": {COUNTRY_FILE: "a" * 64}})
    )
    assert use_derived_photovoltaics(
        ClicRecipe.read(
            {
                "resolution_fingerprint": "new-graph",
                "derived_inventories": {DERIVATION: "b" * 64},
            }
        )
    )


def test_derived_pv_roles_supersede_older_residential_binding(
    inputs, monkeypatch, tmp_path
):
    from premise.inventory import clic_requirements
    from premise.inventory.activity_maps import InventorySet

    prepared, graph = inputs
    result = derive_country_inventories(prepared, graph, [3, 4])
    derived = result.activities[-2:]
    older = copy.deepcopy(derived[-1])
    older["code"] = "older"
    older["clic provenance"] = {"slug": "obsolete-2020"}
    monkeypatch.setattr(
        clic_requirements, "registry", lambda: {"hybrid_sources": graph}
    )
    bindings = clic_requirements.bind_activity_roles(
        [*derived, older], [(tmp_path / COUNTRY_FILE, "3.12")]
    )
    assert older["clic provenance"]["role_bindings"] == []
    assert bindings["electricity.yaml"]["Solar PV Residential"] == (
        derived[-1]["code"],
    )
    filters = {
        "Solar PV Residential": {
            "fltr": ["electricity production, photovoltaic, residential"],
            "clic_slugs": ["obsolete-2020"],
            "clic_role_key": "electricity.yaml:Solar PV Residential",
        }
    }
    mapping = InventorySet([*derived, older]).generate_sets_from_filters(filters)
    assert mapping["Solar PV Residential"] == [derived[-1]]
