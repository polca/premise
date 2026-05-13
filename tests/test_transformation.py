import pytest

from premise.activity_maps import InventorySet
from premise.transformation import find_fuel_efficiency


def test_find_fuel_efficiency_uses_default_fuels_when_filter_is_none(capsys):
    dataset = {
        "name": "electricity production, biomass",
        "location": "GLO",
        "exchanges": [
            {
                "name": "market for wood chips, green, measured as dry mass",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            },
        ],
    }

    efficiency = find_fuel_efficiency(
        dataset=dataset,
        energy_out=3.6,
        fuel_specs={"wood chips": {"lhv": {"value": 18.0}}},
        fuel_map_reverse={
            "market for wood chips, green, measured as dry mass": "wood chips",
        },
        fuel_filters=None,
    )

    assert efficiency == pytest.approx(0.2)
    assert capsys.readouterr().out == ""


def test_find_fuel_efficiency_rejects_empty_filter():
    dataset = {
        "name": "electricity production, biomass",
        "location": "GLO",
        "exchanges": [],
    }

    with pytest.raises(ValueError, match="No fuel filters configured"):
        find_fuel_efficiency(
            dataset=dataset,
            energy_out=3.6,
            fuel_specs={},
            fuel_map_reverse={},
            fuel_filters=[],
        )


def test_find_fuel_efficiency_rejects_missing_fuel_input():
    dataset = {
        "name": "electricity production, biomass",
        "location": "GLO",
        "exchanges": [
            {
                "name": "market for steel, low-alloyed",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            },
        ],
    }

    with pytest.raises(ValueError, match="No fuel input found"):
        find_fuel_efficiency(
            dataset=dataset,
            energy_out=3.6,
            fuel_specs={"wood chips": {"lhv": {"value": 18.0}}},
            fuel_map_reverse={
                "market for wood chips, green, measured as dry mass": "wood chips",
            },
            fuel_filters=["market for wood chips, green, measured as dry mass"],
        )


def test_biomass_fuel_map_includes_green_wood_chips():
    fuel_dataset = {
        "name": "market for wood chips, green, measured as dry mass",
        "reference product": "wood chips, green, measured as dry mass",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [],
    }

    fuel_map = InventorySet(
        database=[fuel_dataset],
        version="3.12",
        model="image",
    ).generate_powerplant_fuels_map()

    assert fuel_dataset in fuel_map["Biomass CHP (existing)"]
    assert fuel_dataset in fuel_map["Biomass IGCC CCS"]
