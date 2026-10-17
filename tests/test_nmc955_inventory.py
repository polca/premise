"""Check the complete-pack boundary of the imported NMC955 market."""

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR


def test_nmc955_market_preserves_pack_and_cell_mass():
    inventory = ExcelImporter(INVENTORY_DIR / "lci-batteries-NMC955-LTO.xlsx").data
    (market,) = [
        ds for ds in inventory if ds["name"] == "market for battery, Li-ion, NMC955"
    ]
    (pack,) = [
        ds for ds in inventory if ds["name"] == "battery production, Li-ion, NMC955"
    ]
    (market_output,) = [
        exc for exc in market["exchanges"] if exc["type"] == "production"
    ]
    (pack_output,) = [exc for exc in pack["exchanges"] if exc["type"] == "production"]
    (pack_input,) = [
        exc
        for exc in market["exchanges"]
        if exc["type"] == "technosphere" and exc["name"] == pack["name"]
    ]
    (cell_input,) = [
        exc
        for exc in pack["exchanges"]
        if exc["name"] == "battery cell production, Li-ion, NMC955"
    ]
    assert {
        exc["unit"] for exc in (market_output, pack_output, pack_input, cell_input)
    } == {"kilogram"}
    # One kg sold requires one kg of complete packs. The cell fraction belongs
    # inside pack manufacture and must not be applied again by the market.
    packs_per_kg_sold = pack_input["amount"] / market_output["amount"]
    assert packs_per_kg_sold == pytest.approx(1)
    cells_per_kg_pack = cell_input["amount"] / pack_output["amount"]
    assert cells_per_kg_pack == pytest.approx(0.72591)
    assert packs_per_kg_sold * cells_per_kg_pack == pytest.approx(0.72591)
