"""Independent coal roles must not share mutable generators or rescale portfolios."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.core.transformation import BaseTransformation
from premise.electricity import Electricity
from premise.inventory.activity_maps import InventorySet


def activity(name, location, unit="kilowatt hour", exchanges=()):
    return {
        "name": name,
        "reference product": name,
        "location": location,
        "unit": unit,
        "code": name + location,
        "exchanges": [
            {
                "type": "production",
                "name": name,
                "product": name,
                "location": location,
                "unit": unit,
                "amount": 1,
                "production volume": 12,
            },
            *exchanges,
        ],
    }


def purchase(a, amount):
    return {
        "type": "technosphere",
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "amount": amount,
        "uncertainty type": 3,
        "loc": amount,
        "scale": abs(amount) / 10,
    }


def setup(family="uvek"):
    fuel = activity("Hard coal, burned in power plant", "RER", "megajoule")
    plant = activity(
        "Electricity, hard coal, at power plant",
        "DE",
        exchanges=[
            purchase(fuel, 12),
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "unit": "kilogram",
                "categories": ("air", "unspecified"),
                "amount": 0.8,
            },
            {
                "type": "technosphere",
                "name": "illustrative signed credit",
                "product": "credit",
                "location": "RER",
                "unit": "kilogram",
                "amount": -0.2,
            },
        ],
    )
    portfolio = activity(plant["name"], "RER", exchanges=[purchase(plant, 1)])
    iam = SimpleNamespace(regions=["EUR", "World"])
    owner = object.__new__(Electricity)
    BaseTransformation.__init__(
        owner,
        [fuel, plant, portfolio],
        iam,
        "remind",
        "SSP2-NPi",
        2050,
        "2026" if family == "uvek" else "3.12",
        "cutoff",
        family=family,
        biosphere_version="3.10",
    )
    owner.mapping = InventorySet(owner.database, model="remind", family=family)
    owner.powerplant_map = owner.mapping.generate_powerplant_map()
    owner.production_per_tech = {
        (plant["name"], a["location"]): 12 for a in [plant, portfolio]
    }
    owner.write_log = lambda **kwargs: None
    return owner, plant, portfolio


def test_complete_role_copies_keep_signed_inventory_and_close_country_links():
    owner, plant, portfolio = setup()
    before = deepcopy(owner.database)
    owner.create_missing_power_plant_datasets()
    copies = owner.powerplant_map["Coal CHP"]
    assert len(copies) == 2
    assert owner.database[:3] == before
    for original, copied in zip(
        [plant, portfolio], sorted(copies, key=lambda a: a["location"])
    ):
        assert copied["code"] != original["code"]
        assert copied["reference product"] == original["reference product"]
        assert copied["unit"] == original["unit"]
        for old, new in zip(original["exchanges"], copied["exchanges"]):
            assert new["amount"] == old["amount"]
            for field in ["uncertainty type", "loc", "scale"]:
                assert old.get(field) == new.get(field)
        assert owner.production_per_tech[copied["name"], copied["location"]] == 12
    country_copy = next(a for a in copies if a["location"] == "DE")
    mix_copy = next(a for a in copies if a["location"] == "RER")
    assert mix_copy["exchanges"][1]["name"] == country_copy["name"]
    assert portfolio["exchanges"][1]["name"] == plant["name"]
    once = deepcopy(owner.database)
    owner.create_missing_power_plant_datasets()
    assert owner.database == once


@pytest.mark.parametrize("order", [["Coal PC", "Coal CHP"], ["Coal CHP", "Coal PC"]])
def test_role_efficiencies_are_independent_and_portfolios_keep_one_kwh(order):
    owner, plant, portfolio = setup()
    owner.create_missing_power_plant_datasets()
    owner.powerplant_fuels_map = {}
    owner.fuels_specs = {}
    owner.fuel_map_reverse = {}
    owner.use_absolute_efficiency = False
    owner.powerplant_min_efficiency = {}
    owner.powerplant_max_efficiency = {"Coal PC": 0.4, "Coal CHP": 0.45}
    owner.is_in_index = lambda dataset: True
    owner.geo = SimpleNamespace(ecoinvent_to_iam_location=lambda location: "EUR")
    owner.find_iam_efficiency_change = lambda **kwargs: 2
    owner.get_iam_mapping = lambda **kwargs: {
        role: {"fuel filters": ["Hard coal, burned in power plant"]} for role in order
    }
    efficiencies = SimpleNamespace(
        variables=SimpleNamespace(values=order),
        coords={"region": SimpleNamespace(values=["EUR"])},
    )
    owner.iam_data.electricity_technology_efficiencies = efficiencies
    owner.iam_data.electricity_mix = SimpleNamespace(
        variables=SimpleNamespace(values=order)
    )
    owner.update_electricity_efficiency()
    clone = next(a for a in owner.powerplant_map["Coal CHP"] if a["location"] == "DE")
    assert plant["log parameters"]["new efficiency"] == 0.4
    assert clone["log parameters"]["new efficiency"] == 0.45
    assert plant["exchanges"][1]["amount"] == pytest.approx(9)
    assert clone["exchanges"][1]["amount"] == pytest.approx(8)
    for mix in [
        portfolio,
        next(a for a in owner.powerplant_map["Coal CHP"] if a["location"] == "RER"),
    ]:
        assert mix["exchanges"][1]["amount"] == 1
        assert "new efficiency" not in mix.get("log parameters", {})


def test_legacy_family_does_not_opt_into_native_role_copies():
    owner, _, _ = setup(family="ecoinvent")
    before = deepcopy(owner.database)
    owner.create_missing_power_plant_datasets()
    assert owner.database == before


def test_historical_coal_adjustment_uses_cogeneration_branch_and_skips_portfolios(
    monkeypatch,
):
    import premise.electricity.electricity as module

    owner, plant, portfolio = setup()
    owner.create_missing_power_plant_datasets()
    owner.iam_data.coal_power_plants = object()
    owner.powerplant_fuels_map = {
        role: ["Hard coal, burned in power plant"] for role in ["Coal PC", "Coal CHP"]
    }
    owner.fuels_specs = {}
    owner.fuel_map_reverse = {}
    calls = []

    def value(**kwargs):
        calls.append((kwargs["country"], kwargs["chp"]))
        return 0.45 if kwargs["chp"] else 0.4

    monkeypatch.setattr(
        module,
        "_CoalPowerPlantData",
        lambda data: SimpleNamespace(
            contains_country=lambda country: True,
            value=value,
            emission_factor=lambda **kwargs: 0,
        ),
    )
    owner.adjust_coal_power_plant_emissions()
    assert sorted(calls) == [("DE", False), ("DE", True)]
    clone = next(a for a in owner.powerplant_map["Coal CHP"] if a["location"] == "DE")
    assert plant["exchanges"][1]["amount"] == pytest.approx(9)
    assert clone["exchanges"][1]["amount"] == pytest.approx(8)
    for mix in [
        portfolio,
        next(a for a in owner.powerplant_map["Coal CHP"] if a["location"] == "RER"),
    ]:
        assert mix["exchanges"][1]["amount"] == 1
        assert "Oberschelp et al. efficiency" not in mix.get("log parameters", {})


def test_missing_combustion_alias_is_not_silently_accepted_as_a_portfolio(monkeypatch):
    import premise.electricity.electricity as module

    owner, _, _ = setup()
    owner.create_missing_power_plant_datasets()
    owner.iam_data.coal_power_plants = object()
    owner.powerplant_fuels_map = {
        role: ["unmatched combustion fuel"] for role in ["Coal PC", "Coal CHP"]
    }
    monkeypatch.setattr(module, "_CoalPowerPlantData", lambda data: SimpleNamespace())
    with pytest.raises(ValueError, match="Missing declared combustion fuel"):
        owner.adjust_coal_power_plant_emissions()
