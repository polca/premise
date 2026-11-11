"""Keep allocated native CHP heat and captured CO2 on one reviewed basis."""

import json

import pytest

from premise.core.filesystem_constants import DATA_DIR
from premise.inventory.prepared_resources import _identity


@pytest.mark.parametrize(
    "fuel,heat_penalty", [("ng", 1.08), ("coal", 1.09), ("wood", 1.1)]
)
def test_native_ccs_heat_preserves_capture_fraction_and_pairs_the_stack_correction(
    fuel, heat_penalty
):
    path = DATA_DIR / f"heat/uvek2026_{fuel}_ccs_heat_inventory.json"
    definition = json.loads(path.read_text())
    activity = next(
        a
        for a in definition["activities"]
        if a.get("premise heat", {}).get("kind") == "native_stack_CCS_screening"
    )
    heat, capture = [e for e in activity["exchanges"] if e["type"] == "technosphere"]
    (credit,) = [e for e in activity["exchanges"] if e["type"] == "biosphere"]
    assert "co-generation" in activity["name"]
    assert heat["unit"] == "megajoule" and heat["amount"] == heat_penalty
    fuel_basis = next(
        requirement["amount"]
        for check in definition["supplier_checks"]
        for requirement in check["exchanges"]
        if requirement["selector"].get("type") == "technosphere"
    )
    stack_factor = next(
        requirement["amount"]
        for check in definition["supplier_checks"]
        for requirement in check["exchanges"]
        if requirement["selector"].get("type") == "biosphere"
    )
    assert capture["amount"] / (
        heat["amount"] * fuel_basis * stack_factor
    ) == pytest.approx(0.9)
    assert capture["unit"] == credit["unit"] == "kilogram"
    assert credit["amount"] == -capture["amount"]
    capture_roots = {
        "ng": "carbon-dioxide-captured-from-natural-gas-post-200km-pipeline-storage-1000m",
        "coal": "carbon-dioxide-captured-from-hard-coal-fired-power-plant-post-pipeline-200km-storage-1000m",
        "wood": "carbon-dioxide-captured-at-wood-burning-power-plant-20-mw-post-pipeline-200km-storage-1000m",
    }
    assert definition["clic_roots"] == [capture_roots[fuel]]
    expected_carbon = (
        "Carbon dioxide, non-fossil" if fuel == "wood" else "Carbon dioxide, fossil"
    )
    assert credit["name"] == expected_carbon
    guarded_carbon = next(
        r["selector"]
        for c in definition["supplier_checks"]
        for r in c["exchanges"]
        if r["selector"].get("type") == "biosphere"
    )
    assert credit["categories"] == guarded_carbon["categories"]
    assert credit["name"] == guarded_carbon["name"]


@pytest.mark.parametrize(
    "fuel,source_name",
    [
        ("ng", "Heat, natural gas, at CHP power plant"),
        ("coal", "Heat, hard coal, at cogen, allocation exergy"),
        ("wood", "Heat, at cogen 1MWth, wood chips, allocation exergy"),
    ],
)
def test_ccs_fixed_reference_keeps_regional_relinking_on_the_reviewed_stack(
    fuel, source_name
):
    definition = json.loads(
        (DATA_DIR / f"heat/uvek2026_{fuel}_ccs_heat_inventory.json").read_text()
    )
    bridge = next(
        a
        for a in definition["activities"]
        if a.get("premise heat", {}).get("kind") == "fixed_reference_provider"
    )
    root = next(
        a
        for a in definition["activities"]
        if a.get("premise heat", {}).get("kind") == "native_stack_CCS_screening"
    )
    (source,) = [e for e in bridge["exchanges"] if e["type"] == "technosphere"]
    (purchase,) = [
        e
        for e in root["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "megajoule"
    ]
    assert _identity(purchase) == _identity(bridge)
    assert purchase["input"] == [bridge["database"], bridge["code"]]
    assert source["amount"] == 1 and source["location"] == (
        "CH" if fuel == "wood" else "DE"
    )
    assert source["name"] == source_name
    assert bridge["name"] != source["name"]
    assert not any(e["type"] == "biosphere" for e in bridge["exchanges"])
