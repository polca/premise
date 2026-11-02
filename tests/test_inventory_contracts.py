"""Check current inventory quantities against explicit scientific references.

No historical workbook is substituted for the packaged inventory under test.
"""

from collections import defaultdict
from functools import lru_cache
import json
from pathlib import Path

from bw2io import ExcelImporter
from bw2io.importers.excel import valid_first_cell
import pytest

from premise.core.filesystem_constants import INVENTORY_DIR

REFERENCE = json.loads(
    (Path(__file__).parent / "data/inventory_contracts.json").read_text()
)
_METADATA = {
    "comment",
    "Comment",
    "source",
    "database",
    "code",
    "input",
    "output",
    "worksheet name",
}


@lru_cache(maxsize=None)
def inventory(filename):
    importer = ExcelImporter.__new__(ExcelImporter)
    rows = [
        (n, r)
        for n, r in importer.extractor.extract(INVENTORY_DIR / filename)
        if valid_first_cell(n, r)
    ]
    importer.db_name, importer.metadata = importer.get_database(rows)
    return importer.process_activities(rows)


def activity(filename, name):
    (record,) = [r for r in inventory(filename) if r["name"] == name]
    return record


def exchange_key(edge):
    categories = edge.get("categories") or ()
    if isinstance(categories, str):
        categories = categories.split("::")
    return (
        edge.get("type"),
        edge.get("name"),
        edge.get("reference product", edge.get("product")),
        edge.get("location"),
        edge.get("unit"),
        tuple(categories),
    )


def payload(edge):
    excluded = _METADATA | {
        "name",
        "product",
        "reference product",
        "location",
        "unit",
        "type",
        "categories",
    }
    return {k: v for k, v in edge.items() if k not in excluded}


@pytest.mark.parametrize(
    "contract",
    REFERENCE["contracts"],
    ids=lambda c: c["workbook"] + ":" + c["activity"][0],
)
def test_current_inventory_scientific_contract(contract):
    record = activity(contract["workbook"], contract["activity"][0])
    assert [
        record.get(k) for k in ("name", "reference product", "location", "unit")
    ] == contract["activity"]
    groups = defaultdict(list)
    for edge in record["exchanges"]:
        groups[exchange_key(edge)].append(payload(edge))
    if contract.get("closed"):
        assert set(groups) == {
            (*e["identity"][:5], tuple(e["identity"][5])) for e in contract["exchanges"]
        }, contract["cases"]
    for expected in contract["exchanges"]:
        key = (*expected["identity"][:5], tuple(expected["identity"][5]))
        assert sorted(groups[key], key=repr) == sorted(expected["values"], key=repr), (
            contract["cases"],
            key,
        )
    for removed in contract["absent"]:
        assert (*removed[:5], tuple(removed[5])) not in groups, (
            contract["cases"],
            removed,
        )


def test_lithium_oxygen_capacity_route_includes_auxiliary_assembly_once():
    filename = "lci-batteries-LiO2.xlsx"
    name = "battery auxiliaries supply and end-of-life treatment, lithium-oxygen"
    auxiliary = activity(filename, name)
    edges = {
        e["name"]: e for e in auxiliary["exchanges"] if e["type"] == "technosphere"
    }
    cooler = edges["cooling system production, for Li-O2 battery"]
    assert (cooler["amount"], cooler["location"], cooler["unit"]) == (
        29.07,
        "RER",
        "kilogram",
    )
    assert edges["market group for electricity, medium voltage"]["amount"] == 0.41 / 3.6
    assert edges["treatment of LiO2 battery, for recycling"]["amount"] == -88.07
    assert edges["battery central control and protection unit production"][
        "amount"
    ] * 4.4928 == pytest.approx(4)
    assert edges["oxygen tank production, for Li-O2 battery"]["amount"] == 55
    purchases = [
        e
        for r in inventory("lci-battery-capacity.xlsx")
        for e in r["exchanges"]
        if e.get("type") == "technosphere" and e.get("name") == name
    ]
    assert len(purchases) == 1 and purchases[0]["amount"] == 1 / 63.5


def test_lithium_oxygen_capacity_routes_retain_finished_pack_freight():
    filename = "lci-batteries-LiO2.xlsx"

    def freight(name):
        return {
            exchange_key(e): e["amount"]
            for e in activity(filename, name)["exchanges"]
            if e["type"] == "technosphere" and e["unit"] == "ton kilometer"
        }

    full = freight("market for battery, Li-oxygen, Li-O2")
    cell = freight(
        "market for battery cell, lithium-oxygen, including end-of-life treatment"
    )
    fixed = freight(
        "battery pack component production and end-of-life treatment, lithium-oxygen, without cells and controls"
    )
    assert len(full) == 4 and full.keys() == cell.keys() == fixed.keys()
    assert cell == full
    for key, amount in full.items():
        assert fixed[key] == pytest.approx(amount * 151.25 / 63.5, rel=1e-14)
