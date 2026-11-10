"""Documented final-energy bases do not deallocate or rescale native inventories."""

import copy
from pathlib import Path

import pytest
import xarray as xr
import yaml

from premise.energy.heat import Heat


def updater(monkeypatch, aliases):
    heat = object.__new__(Heat)
    heat.family = "uvek"
    heat.regions = ["EUR"]
    heat.year = 2050
    heat.diagnostics = {}
    heat.heat_metadata = {
        "technology": {
            "energy_basis": "final_energy",
            "conversion": "combustion",
            "uvek_aliases": aliases,
        }
    }
    supplier = {
        "name": "allocated heat supplier",
        "location": "EUR",
        "share": 1.0,
        "exchanges": [{"amount": -0.01, "type": "technosphere"}],
    }
    monkeypatch.setattr(
        heat, "_select_suppliers", lambda technology, region: ([supplier], "IAM region")
    )
    raw = xr.DataArray(
        [[[2.0, 4.0]]],
        dims=("variables", "region", "year"),
        coords={"variables": ["technology"], "region": ["EUR"], "year": [2020, 2050]},
    )
    return heat, supplier, raw


@pytest.mark.parametrize("factor", [0.33, 0.9, 0.9297454944044002])
def test_declared_heat_basis_changes_only_the_iam_volume(monkeypatch, factor):
    heat, supplier, raw = updater(
        monkeypatch,
        {
            "delivered_heat_factor": factor,
            "delivered_heat_basis": "independently reviewed source basis",
        },
    )
    original = copy.deepcopy(supplier)
    monkeypatch.setattr(
        heat,
        "_supplier_conversion_factor",
        lambda *args: pytest.fail("Do not infer efficiency from allocated inputs"),
    )
    delivered = heat.convert_to_delivered_heat(raw, "industrial_end_use")
    assert delivered.values.flatten().tolist() == pytest.approx(
        [2 * factor, 4 * factor]
    )
    assert supplier == original
    assert raw.values.flatten().tolist() == [2.0, 4.0]
    assert (
        heat.diagnostics["industrial_end_use"]["conversion factors"][0]["factor basis"]
        == "independently reviewed source basis"
    )


@pytest.mark.parametrize(
    "factor", [True, -1, 0, 1.3, float("nan"), float("inf"), "0.33"]
)
def test_invalid_documented_factors_are_refused(monkeypatch, factor):
    heat, _, raw = updater(
        monkeypatch, {"delivered_heat_factor": factor, "delivered_heat_basis": "review"}
    )
    with pytest.raises(ValueError, match="Invalid documented"):
        heat.convert_to_delivered_heat(raw, "industrial_end_use")


def test_factor_without_a_basis_is_refused(monkeypatch):
    heat, _, raw = updater(monkeypatch, {"delivered_heat_factor": 0.33})
    with pytest.raises(ValueError, match="Invalid documented"):
        heat.convert_to_delivered_heat(raw, "industrial_end_use")


def test_declared_factor_does_not_allow_missing_suppliers(monkeypatch):
    heat, _, raw = updater(
        monkeypatch, {"delivered_heat_factor": 0.33, "delivered_heat_basis": "review"}
    )
    monkeypatch.setattr(
        heat, "_select_suppliers", lambda technology, region: ([], "missing")
    )
    with pytest.raises(ValueError, match="no inventory supplier"):
        heat.convert_to_delivered_heat(raw, "industrial_end_use")


def test_ecoinvent_keeps_inventory_derived_conversion(monkeypatch):
    heat, _, raw = updater(
        monkeypatch, {"delivered_heat_factor": 0.33, "delivered_heat_basis": "review"}
    )
    heat.family = "ecoinvent"
    monkeypatch.setattr(heat, "_supplier_conversion_factor", lambda *args: 0.8)
    delivered = heat.convert_to_delivered_heat(raw, "industrial_end_use")
    assert delivered.values.flatten().tolist() == pytest.approx([1.6, 3.2])


def test_only_previously_reviewed_native_roles_declare_factors():
    import json

    root = Path(__file__).parents[1] / "premise"
    heat = yaml.safe_load((root / "iam_variables_mapping/heat.yaml").read_text())
    supplemental = json.loads(
        (root / "data/heat/uvek2026_supplement_review.json").read_text()
    )
    native = json.loads((root / "data/heat/uvek2026_review.json").read_text())
    expected = {}
    for record in supplemental["records"]:
        if record["conversion"] == "combustion":
            expected.update(
                {role: record["nominal_factor"] for role in record["roles"]}
            )
    expected.update(
        {
            record["role"]: record["documented_physical_conversion"]["factor"]
            for record in native["roles"]
            if record.get("documented_physical_conversion", {}).get("kind")
            == "allocated_fuel_cell"
        }
    )
    actual = {
        role: row["uvek_aliases"]["delivered_heat_factor"]
        for role, row in heat.items()
        if "delivered_heat_factor" in row.get("uvek_aliases", {})
    }
    assert actual == expected
