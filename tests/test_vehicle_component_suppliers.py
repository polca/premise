"""Shared vehicle components resolve to one canonical bundled supplier."""

import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR


def _load(stem):
    return ExcelImporter(str(INVENTORY_DIR / f"{stem}.xlsx")).data


def test_refreshed_truck_components_use_canonical_names():
    trucks = _load("lci-trucks")
    names = {dataset["name"] for dataset in trucks}
    canonical = {
        "assembly operation, for lorry",
        "exhaust system, for lorry",
        "frame, blanks and saddle, for lorry",
        "fuel tank assembly, compressed natural gas, 200 bar",
        "gearbox, for lorry",
        "internal combustion engine, for lorry",
        "lead acid battery, for lorry",
        "other components, for hybrid electric lorry",
        "power electronics, for lorry",
        "retarder, for lorry",
        "suspension, for lorry",
        "tires and wheels, for lorry",
        "transmission, for lorry",
        "fuel tank, for diesel vehicle",
        "EV charger, level 3, plugin, 200 kW",
        "storage battery",
        "charger",
        "charging stand",
        "fuel tank, compressed hydrogen gas, 700bar, with HDPE liner",
        "other components, for electric lorry",
    }
    suffix = ", carculator regional truck 2020"

    assert canonical <= names
    assert not {f"{name}{suffix}" for name in canonical} & names


def test_shared_component_suppliers_are_unique_and_consumers_use_them():
    inventories = {
        stem: _load(stem)
        for stem in ("lci-trucks", "lci-buses", "lci-final-energy", "lci-ships")
    }
    suppliers = {
        "EV charger, level 3, plugin, 200 kW": "lci-trucks",
        "Electrical equipment": "lci-buses",
        "Transformers": "lci-buses",
    }

    for name, expected_workbook in suppliers.items():
        producers = [
            stem
            for stem, datasets in inventories.items()
            if any(dataset["name"] == name for dataset in datasets)
        ]
        assert producers == [expected_workbook]

    old_tank = "fuel tank assembly, compressed hydrogen gas, 700bar"
    for datasets in inventories.values():
        assert not any(
            exchange.get("name") == old_tank
            for dataset in datasets
            for exchange in dataset.get("exchanges", [])
        )

    buses = inventories["lci-buses"]
    (catenary,) = [dataset for dataset in buses if dataset["name"] == "Catenary system"]
    catenary_suppliers = {
        exchange["name"]
        for exchange in catenary["exchanges"]
        if exchange["type"] == "technosphere"
    }
    assert {"Electrical equipment", "Transformers"} <= catenary_suppliers


def test_all_bundled_hydrogen_tank_consumers_resolve():
    """Check cross-workbook closure, including rail, without an ecoinvent licence."""
    from collections import Counter

    datasets = [
        dataset
        for stem in (
            "lci-trucks",
            "lci-buses",
            "lci-pass_cars",
            "lci-rail-freight",
            "lci-final-energy",
            "lci-ships",
        )
        for dataset in _load(stem)
    ]

    def identity(record):
        return (
            record["name"].lower(),
            record["reference product"].lower(),
            record["location"],
            record["unit"],
        )

    suppliers = Counter(identity(dataset) for dataset in datasets)
    checked = []
    for dataset in datasets:
        for exchange in dataset["exchanges"]:
            if exchange["type"] != "technosphere":
                continue
            if exchange.get("reference product", "").lower() != "hydrogen tank":
                continue
            assert suppliers[identity(exchange)] == 1, (
                dataset["name"],
                identity(exchange),
                suppliers[identity(exchange)],
            )
            checked.append(dataset["name"])
    assert "production of locomotive, fuel cell, hydrogen" in checked
    assert "hydrogen, used in a fuel cell in a passenger train" in checked


@pytest.fixture(scope="module")
def shared_component_audit():
    root = Path(__file__).parents[1]
    audit = root / "dev/data/vehicle_inventory_audit"
    plan = json.loads((audit / "component-reuse-plan.json").read_text())
    manifest = json.loads((audit / "shared-component-reuse.json").read_text())
    specification = importlib.util.spec_from_file_location(
        "component_cleanup_audit", root / "scripts/deduplicate_vehicle_components.py"
    )
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    blocks = [
        block
        for path in sorted(INVENTORY_DIR.glob("*.xlsx"))
        for block in module.read_blocks(path)
    ]
    return plan, manifest, module, blocks


def test_model_specific_components_use_unique_existing_suppliers(
    shared_component_audit,
):
    plan, manifest, module, blocks = shared_component_audit
    retired = {alias["source"]["name"].casefold() for alias in plan["aliases"]}
    canonical = {module.identity(alias["canonical"]) for alias in plan["aliases"]}
    suppliers = Counter(module.identity(block.metadata) for block in blocks)
    assert len(retired) == 28 and len(canonical) == 15
    assert all(suppliers[key] == 1 for key in canonical)
    consumers = set()
    for block in blocks:
        assert block.metadata["name"].casefold() not in retired
        for _, exchange in block.exchanges:
            assert exchange.get("name", "").casefold() not in retired
            if (
                exchange.get("type") == "technosphere"
                and module.identity(exchange) in canonical
            ):
                assert suppliers[module.identity(exchange)] == 1
                consumers.add(block.filename)
    assert {
        "lci-pass_cars.xlsx",
        "lci-trucks.xlsx",
        "lci-final-energy.xlsx",
        "lci-rail-freight.xlsx",
    } <= consumers
    assert manifest["removed_activity_count"] == 28
    assert manifest["relinked_retained_exchange_count"] == 174


def test_canonical_recipe_quantities_and_uncertainty_are_preserved(
    shared_component_audit,
):
    _, manifest, module, blocks = shared_component_audit
    by_identity = {module.identity(block.metadata): block for block in blocks}
    for canonical in manifest["canonical_recipes"]:
        block = by_identity[module.identity(canonical["identity"])]
        assert block.filename == canonical["file"]
        assert module.recipe_hash(block, {}) == canonical["recipe_sha256"]
    assert len(manifest["retired_components"]) == 28
    for retired in manifest["retired_components"]:
        metadata = retired["original_metadata"]
        assert metadata["source"]
        assert metadata["vehicle inventory provenance"]
        assert retired["original_recipe_sha256"]


def test_component_cleanup_is_idempotent(shared_component_audit):
    _, _, module, _ = shared_component_audit
    root = Path(__file__).parents[1]
    assert module.apply_cleanup(root) == {
        "status": "already_applied",
        "removed_activities": 0,
        "relinked_exchanges": 0,
    }


def test_component_cleanup_preserves_cached_formulas(shared_component_audit):
    _, manifest, module, _ = shared_component_audit
    for filename, expected in manifest["preserved_formula_caches"].items():
        assert module.formula_cache_signature(INVENTORY_DIR / filename) == expected
    assert manifest["preserved_formula_caches"]["lci-final-energy.xlsx"]["count"] == 459
