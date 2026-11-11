"""Native blend reference energy and final linked vehicle carbon origin."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.core.transformation import BaseTransformation
from premise.fuels.base import Fuels
from premise.fuels import markets
from premise.fuels.carbon import update_vehicle_fuel_co2
from premise.fuels.properties import PROPERTIES_KEY


def activity(name, location="CH", unit="kilogram", **metadata):
    return {
        "name": name,
        "reference product": name,
        "location": location,
        "unit": unit,
        "exchanges": [
            {
                "type": "production",
                "name": name,
                "product": name,
                "location": location,
                "unit": unit,
                "amount": 1,
            }
        ],
        **metadata,
    }


def purchase(a, amount, **metadata):
    return {
        "type": "technosphere",
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "amount": amount,
        **metadata,
    }


def setup(monkeypatch):
    petrol = activity("illustrative petrol")
    ethanol = activity("illustrative ethanol")
    supply = activity("Fuel supply for gasoline vehicles")
    supply["exchanges"].extend([purchase(petrol, 0.8), purchase(ethanol, 0.2)])
    market = activity("market for petrol", "NEU")
    market["reference product"] = "petrol"
    market["exchanges"][0]["product"] = "petrol"
    market[PROPERTIES_KEY] = {
        "reference unit": "kilogram",
        "reference energy MJ per unit": 42.6,
        "energy MJ per unit": 42.6,
        "fossil CO2 kg per unit": 1.0,
        "non-fossil CO2 kg per unit": 2.0,
        "unresolved CO2 kg per unit": 0.0,
        "components": [],
    }
    car = activity("Transport, passenger car, illustrative petrol", unit="kilometer")
    car["exchanges"].append(
        purchase(
            supply,
            0.1,
            **{"uncertainty type": 2, "loc": -2.302585092994046, "scale": 0.1},
        )
    )
    for name, amount in [
        ("Carbon dioxide, fossil", 0.2),
        ("Carbon dioxide, non-fossil", 0.1),
    ]:
        car["exchanges"].append(
            {
                "name": name,
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air", "unspecified"),
                "amount": amount,
            }
        )
    fuels = object.__new__(Fuels)
    BaseTransformation.__init__(
        fuels,
        database=[petrol, ethanol, supply, market, car],
        iam_data=SimpleNamespace(regions=["NEU", "World"]),
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        version="2026",
        family="uvek",
        biosphere_version="3.10",
        system_model="cutoff",
    )
    fuels.write_log = lambda *args, **kwargs: None
    entry = {
        "source": {
            "name": supply["name"],
            "reference product": supply["reference product"],
            "unit": "kilogram",
        },
        "target": {
            "name": "market for petrol",
            "reference product": "petrol",
            "unit": "kilogram",
        },
        "replace_original_supply": True,
        "energy_inputs": {
            petrol["name"]: {"unit": "kilogram", "MJ per unit": 42.6},
            ethanol["name"]: {"unit": "kilogram", "MJ per unit": 26.5},
        },
        "limitations": ["Illustrative calibration."],
    }
    monkeypatch.setattr(
        markets, "get_mapping", lambda *args, **kwargs: {"gasoline": [entry]}
    )
    monkeypatch.setattr(
        "premise.inventory.inventory_imports.get_biosphere_code",
        lambda version: {
            (name, "air", "unspecified", "kilogram"): name
            for name in ["Carbon dioxide, fossil", "Carbon dioxide, non-fossil"]
        },
    )
    target = {
        "name": "market for petrol",
        "reference_product": "petrol",
        "unit": "kilogram",
    }
    return fuels, supply, car, entry, target


def test_native_blend_keeps_operating_energy_and_replaces_old_exhaust_origin(
    monkeypatch,
):
    fuels, supply, car, _, target = setup(monkeypatch)
    before = deepcopy(car["exchanges"][1])
    fuels._handover_fuel_market_aliases(target)
    energy = 0.8 * 42.6 + 0.2 * 26.5
    assert supply[PROPERTIES_KEY]["energy MJ per unit"] == pytest.approx(energy)
    assert supply["exchanges"][1]["amount"] == pytest.approx(energy / 42.6)
    update_vehicle_fuel_co2(fuels.database, "3.10")
    assert car["exchanges"][1] == before
    amounts = {
        e["name"]: e["amount"] for e in car["exchanges"] if e["type"] == "biosphere"
    }
    assert amounts["Carbon dioxide, fossil"] == pytest.approx(0.1 * energy / 42.6)
    assert amounts["Carbon dioxide, non-fossil"] == pytest.approx(0.2 * energy / 42.6)
    # Repeating an update must retain the original reference, not compound it.
    fuels._handover_fuel_market_aliases(target)
    update_vehicle_fuel_co2(fuels.database, "3.10")
    repeated = {
        e["name"]: e["amount"] for e in car["exchanges"] if e["type"] == "biosphere"
    }
    assert repeated == pytest.approx(amounts)


def test_unknown_blend_ingredient_is_not_silently_omitted(monkeypatch):
    fuels, supply, _, entry, target = setup(monkeypatch)
    entry["energy_inputs"].pop("illustrative ethanol")
    before = deepcopy(supply)
    with pytest.raises(ValueError, match="Unreviewed vehicle-blend energy ingredient"):
        fuels._handover_fuel_market_aliases(target)
    assert supply == before
