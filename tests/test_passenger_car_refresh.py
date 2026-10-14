"""Integration boundaries needed by the regenerated 2020 car inventories."""

from types import SimpleNamespace

import pytest

from premise.fuels.base import Fuels
from premise.validation import TransportValidation


@pytest.mark.parametrize(
    "powertrain, expected_issues", [("fuel cell electric", 0), ("battery electric", 1)]
)
def test_refuelling_electricity_is_not_battery_traction(powertrain, expected_issues):
    validator = object.__new__(TransportValidation)
    validator.regions = ["EUR"]
    validator.database = [
        {
            "name": f"transport, passenger car, {powertrain}, Medium",
            "location": "EUR",
            "exchanges": [
                {
                    "name": "market group for electricity, low voltage",
                    "type": "technosphere",
                    "unit": "kilowatt hour",
                    "amount": 0.02,
                }
            ],
        }
    ]
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append(args)
    validator.check_vehicle_efficiency("transport, passenger car", elec_minimum=0.1)
    assert len(issues) == expected_issues


@pytest.mark.parametrize(
    "unit, name, quantity, total_co2",
    [
        ("kilogram", "market for natural gas, medium pressure, vehicle grade", 1, 2.79),
        ("cubic meter", "market for natural gas, low pressure", 1, 2.12),
    ],
)
def test_regional_gas_blend_preserves_carbon_for_kg_and_m3(
    unit, name, quantity, total_co2
):
    fuel = {
        "name": name,
        "type": "technosphere",
        "location": "GLO",
        "unit": unit,
        "amount": quantity,
    }
    fossil = {
        "name": "Carbon dioxide, fossil",
        "type": "biosphere",
        "categories": ("air",),
        "unit": "kilogram",
        "amount": total_co2,
    }
    consumer = {"name": "gas consumer", "location": "EUR", "exchanges": [fuel, fossil]}
    fuels = object.__new__(Fuels)
    fuels.database = [consumer]
    fuels.fuel_map = {"natural gas": [], "methane, from biomass": []}
    fuels.iam_data = SimpleNamespace(production_volumes=None)
    fuels.regions = ["EUR"]
    fuels.ecoinvent_to_iam_loc = {}
    fuels.is_in_index = lambda exc, location: location == "EUR"
    fuels.biosphere_flows = {
        ("Carbon dioxide, non-fossil", "air", "unspecified", "kilogram"): "bio"
    }
    fuels.get_technology_and_regional_production_shares = lambda **kwargs: (
        None,
        {("natural gas", "EUR"): 0.8, ("methane, from biomass", "EUR"): 0.2},
        {"EUR": 1},
    )
    fuels.update_carbon_dioxide_emissions()
    fuels.update_carbon_dioxide_emissions()
    assert fuel["location"] == "EUR"
    assert fossil["amount"] == pytest.approx(total_co2 * 0.8)
    assert sum(
        e["amount"] for e in consumer["exchanges"] if e["type"] == "biosphere"
    ) == pytest.approx(total_co2)
    assert consumer["exchanges"][-1]["amount"] == pytest.approx(total_co2 * 0.2)
