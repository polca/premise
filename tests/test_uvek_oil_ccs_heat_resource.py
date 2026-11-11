"""Oil CHP reconstruction captures combustion CO2, retaining limestone emissions."""

import json

import pytest

from premise.core.filesystem_constants import DATA_DIR
from premise.inventory.prepared_resources import _identity


def test_oil_ccs_capture_excludes_limestone_and_uses_allocated_power_plant_input():
    resource = json.loads(
        (DATA_DIR / "heat/uvek2026_oil_ccs_heat_inventory.json").read_text()
    )
    bridge, root = resource["activities"]
    (native,) = [e for e in bridge["exchanges"] if e["type"] == "technosphere"]
    assert native["name"] == "Heavy fuel oil, burned in power plant"
    assert native["unit"] == "megajoule" and native["location"] == "DE"
    assert 0 < native["amount"] < 1  # Allocated heat, not inverse efficiency.
    heat, capture = [e for e in root["exchanges"] if e["type"] == "technosphere"]
    assert _identity(heat) == _identity(bridge)
    assert heat["amount"] == 1.09 and heat["unit"] == "megajoule"
    carbon = {
        tuple(row["selector"]["categories"]): row["amount"]
        for check in resource["supplier_checks"]
        for row in check["exchanges"]
        if row["selector"].get("type") == "biosphere"
    }
    combustion = (
        native["amount"] * heat["amount"] * carbon[("air", "urban air close to ground")]
    )
    limestone = native["amount"] * heat["amount"] * carbon[("air",)]
    assert capture["amount"] == pytest.approx(0.9 * combustion)
    assert root["premise heat"]["uncaptured_limestone_CO2_kg"] == pytest.approx(
        limestone
    )
    (credit,) = [e for e in root["exchanges"] if e["type"] == "biosphere"]
    assert credit["name"] == "Carbon dioxide, fossil"
    assert credit["categories"] == ["air", "urban air close to ground"]
    assert credit["amount"] == -capture["amount"]
    assert capture["amount"] < 0.9 * (combustion + limestone)
    assert not any(e["type"] == "biosphere" for e in bridge["exchanges"])
    assert resource["clic_roots"] == [
        "carbon-dioxide-captured-from-hard-coal-fired-power-plant-post-pipeline-200km-storage-1000m"
    ]
