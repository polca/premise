"""Pin the independent CLIC proxy and its default-import boundary."""

import ast
import json
import runpy
from copy import deepcopy
from pathlib import Path

import numpy as np
import pytest
from bw2io import ExcelImporter
from test_pem_iridium_provider import iridium, model, source

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory, get_biosphere_code

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-iridium-economic-proxy.xlsx"
PROXY = "iridium production, economic allocation, proxy"
REFINERY = "platinum group metal, extraction and refinery operations"


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (
            ROOT
            / "tests/data/inventory_review/iridium-economic-proxy-adoption-20261027.json"
        ).read_text()
    )


@pytest.fixture(scope="module")
def imported(adoption):
    # Only supplier identities are needed to resolve product fields. No native
    # inventories or database writes are required for this preparation check.
    background = [
        {
            "name": e["name"],
            "reference product": e["reference product"],
            "location": e["location"],
            "unit": e["unit"],
            "exchanges": [],
        }
        for e in adoption["snapshot"]["exchanges"]
        if e["type"] == "technosphere"
    ]
    importer = DefaultInventory(
        background, "3.12", "3.12", INVENTORY_DIR / WORKBOOK, "cutoff", True
    )
    importer.prepare_inventory()
    assert importer.list_unlinked == []
    (dataset,) = importer.import_db.data
    return dataset


def exchange_key(exchange):
    categories = exchange.get("categories", ())
    if isinstance(categories, str):
        categories = categories.split("::")
    return (
        exchange["type"],
        exchange["name"],
        exchange.get("product", exchange.get("reference product")),
        exchange.get("location"),
        exchange["unit"],
        tuple(categories),
    )


def assert_snapshot_exchanges(dataset, snapshot):
    expected = {exchange_key(e): e["amount"] for e in snapshot["exchanges"]}
    # The historical CLIC snapshot remains unchanged. Premise adds the
    # approved product-content resource attribution at the supplier gate.
    assert len(expected) == len(snapshot["exchanges"]) == 12
    expected[
        (
            "biosphere",
            "Iridium",
            None,
            None,
            "kilogram",
            ("natural resource", "in ground"),
        )
    ] = 1
    actual = {exchange_key(e): e["amount"] for e in dataset["exchanges"]}
    assert len(actual) == len(dataset["exchanges"]) == len(expected)
    assert actual.keys() == expected.keys()
    for key, amount in expected.items():
        assert actual[key] == amount, key
        assert np.isfinite(actual[key]) and actual[key] > 0


def test_portable_fixture_is_the_approved_independent_snapshot(adoption):
    snapshot = adoption["snapshot"]
    json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.12",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.12"}
    assert adoption["license"] == "CC-BY-4.0"
    assert "independently constructed foreground data" in snapshot["comment"]


def test_workbook_has_exact_approved_schema_and_quantities(adoption):
    (dataset,) = ExcelImporter(INVENTORY_DIR / WORKBOOK).data
    snapshot = adoption["snapshot"]
    for field in ("name", "reference product", "location", "unit", "production amount"):
        assert dataset[field] == snapshot[field]
    assert (dataset["name"], dataset["reference product"], dataset["location"]) == (
        PROXY,
        "iridium",
        "ZA",
    )
    assert (
        snapshot["comment"].replace(
            "mineral-resource extraction, recovery losses, land",
            "recovery losses beyond product-content resource attribution, land",
        )
        in dataset["comment"]
    )
    assert "APPROVED PREMISE RESOURCE ATTRIBUTION, 2026-11-01" in dataset["comment"]
    assert snapshot["source"] in dataset["source"]
    assert dataset["license"] == adoption["license"] == "CC-BY-4.0"
    assert (
        dataset["background version"]
        == adoption["source_background"]["version"]
        == "3.12"
    )
    assert (
        dataset["biosphere version"]
        == adoption["source_biosphere"]["version"]
        == "3.12"
    )
    assert_snapshot_exchanges(dataset, snapshot)
    for exchange in dataset["exchanges"]:
        assert not any(key.startswith("replacement ") for key in exchange)
        assert not exchange.get("formula")


def test_premise_preparation_preserves_proxy_boundary_and_amounts(imported, adoption):
    assert imported["name"] == PROXY
    assert imported["unit"] == "kilogram"
    assert_snapshot_exchanges(imported, adoption["snapshot"])
    (production,) = [e for e in imported["exchanges"] if e["type"] == "production"]
    assert production["product"] == "iridium"
    assert production["amount"] == 1
    (sulfur,) = [e for e in imported["exchanges"] if e["name"] == "Sulfur dioxide"]
    assert sulfur["categories"] == ("air", "non-urban air or from high stacks")
    assert sulfur["input"][1] == "78c3efe4-421c-4d30-82e4-b97ac5124993"
    assert dict(imported["classifications"]) == {
        "ISIC rev.4 ecoinvent": "2420: Manufacture of basic precious and other non-ferrous metals",
        "CPC": "41330: Platinum, unwrought or in semi-manufactured forms, or in powder form",
    }


def test_iridium_resource_is_linked_and_keeps_one_kg_per_kg_output(imported):
    (resource,) = [e for e in imported["exchanges"] if e["name"] == "Iridium"]
    assert resource["type"] == "biosphere"
    assert resource["unit"] == "kilogram"
    assert resource["categories"] == ("natural resource", "in ground")
    assert resource["amount"] == imported["production amount"] == 1
    assert (
        resource["input"][1]
        == get_biosphere_code("3.12")[
            ("Iridium", "natural resource", "in ground", "kilogram")
        ]
    )
    assert "recovery-loss factor" in resource["comment"]
    assert "economic allocation" in resource["comment"]


@pytest.mark.parametrize(
    "version", ["3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]
)
def test_iridium_resource_links_in_supported_biospheres(version):
    importer = DefaultInventory(
        [], "3.12", version, INVENTORY_DIR / WORKBOOK, "cutoff", True
    )
    importer.add_biosphere_links()
    (dataset,) = importer.import_db.data
    name = "Iridium, in ground" if version == "3.8" else "Iridium"
    (resource,) = [e for e in dataset["exchanges"] if e["name"] == name]
    assert (
        resource["input"][1]
        == get_biosphere_code(version)[
            (name, "natural resource", "in ground", "kilogram")
        ]
    )
    assert resource["amount"] == 1


def test_runtime_selects_the_prepared_proxy_without_mutating_it(imported):
    stack, _ = source.__wrapped__()
    before = deepcopy(imported)
    runtime = model([imported])
    runtime.update_metal_use(stack, "PEMEL")
    (exchange,) = iridium(stack)
    assert exchange["name"] == imported["name"] == PROXY
    assert exchange["location"] == imported["location"] == "ZA"
    assert exchange["amount"] == pytest.approx(0.4 / (2 * 1.9))
    (decision,) = runtime.material_decisions
    assert decision["status"] == "updated"
    assert decision["provider selection"] == "existing input fallback"
    assert runtime.material_update_diagnostics == []
    assert imported == before


def test_pgm_workbook_retains_other_providers():
    data = ExcelImporter(INVENTORY_DIR / "lci-PGM.xlsx").data
    assert len(data) == 3
    assert not any(
        d["name"] == REFINERY
        and d["reference product"] == "iridium"
        and d["location"] == "ZA"
        for d in data
    )
    assert any(
        d["name"] == REFINERY
        and d["reference product"] == "iridium"
        and d["location"] == "RU"
        for d in data
    )


@pytest.mark.parametrize(
    "version", ["3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]
)
@pytest.mark.parametrize("system_model", ["cutoff", "consequential"])
def test_default_import_registers_separate_source_versions(version, system_model):
    generator = runpy.run_path(
        str(ROOT / "docs/_tools/generate_documentation_reference.py")
    )
    tree = ast.parse((ROOT / "premise/database/new_database.py").read_text())
    selection = generator["inventory_selection"](tree, version, system_model)
    assert selection.count((WORKBOOK, "3.12")) == 1
    assert selection.count(("lci-PGM.xlsx", "3.8")) == 1
    assert selection.count(("lci-hydrogen-electrolysis.xlsx", "3.9")) == 1
    assert selection.index((WORKBOOK, "3.12")) < selection.index(
        ("lci-hydrogen-electrolysis.xlsx", "3.9")
    )


@pytest.mark.parametrize(
    "constant",
    [
        "FILEPATH_PGM",
        "FILEPATH_IRIDIUM_ECONOMIC_PROXY",
        "FILEPATH_HYDROGEN_INVENTORIES",
    ],
)
def test_workbook_changes_invalidate_only_inventory_cache(
    constant, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = "3.12"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    inventory_cache = obj._database_cache_path("test", inventories=True)
    source_cache = obj._database_cache_path("test")
    workbook = Path(getattr(module, constant))
    original_bytes = workbook.read_bytes()
    copy = tmp_path / workbook.name
    copy.write_bytes(original_bytes)
    monkeypatch.setattr(module, constant, copy)
    assert obj._database_cache_path("test", inventories=True) == inventory_cache
    copy.write_bytes(original_bytes + b"changed iridium supply boundary")
    assert obj._database_cache_path("test", inventories=True) != inventory_cache
    assert obj._database_cache_path("test") == source_cache
    copy.write_bytes(original_bytes)
    assert obj._database_cache_path("test", inventories=True) == inventory_cache
