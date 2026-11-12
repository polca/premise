"""Native delivery composition must propagate without changing its inventory."""

from copy import deepcopy
import math

import pytest

from premise.fuels.carbon import update_vehicle_fuel_co2, vehicle_fuel_carbon
from premise.fuels.properties import (
    PROPERTIES_KEY,
    activity_key,
    annotate_cng_properties,
    supplied_energy,
)
from premise.inventory.activity_maps import FUELS_TECHS, get_mapping


def activity(name, unit="megajoule", exchanges=()):
    return {
        "name": name,
        "reference product": name,
        "location": "CH",
        "unit": unit,
        "exchanges": [
            {
                "name": name,
                "product": name,
                "location": "CH",
                "unit": unit,
                "type": "production",
                "amount": 1,
            },
            *exchanges,
        ],
    }


def purchase(a, amount, **kwargs):
    return {
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "type": "technosphere",
        "amount": amount,
        **kwargs,
    }


def setup():
    high = activity("Natural gas, high pressure, at consumer")
    high[PROPERTIES_KEY] = {
        "reference unit": "megajoule",
        "reference energy MJ per unit": 1,
        "energy MJ per unit": 1,
        "fossil CO2 kg per unit": 0.04,
        "non-fossil CO2 kg per unit": 0.02,
        "unresolved CO2 kg per unit": 0,
    }
    pipe = activity(
        "Pipeline, natural gas, low pressure distribution network", "kilometer"
    )
    auxiliary = activity("Natural gas, burned in gas turbine")
    low = activity(
        "Natural gas, low pressure, at consumer",
        exchanges=[
            purchase(
                high,
                1.02,
                **{"uncertainty type": 2, "loc": math.log(1.02), "scale": 0.1},
            ),
            purchase(pipe, 0.001),
            purchase(auxiliary, 0.002),
            purchase(pipe, -0.00001),
        ],
    )
    burner = activity(
        "illustrative low-pressure gas burner",
        exchanges=[
            purchase(low, 1),
            {
                "name": "Carbon dioxide, fossil",
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air", "unspecified"),
                "amount": 0.0564,
            },
            {
                "name": "Methane, fossil",
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air", "unspecified"),
                "amount": 0.0001,
            },
        ],
    )
    spec = deepcopy(
        get_mapping(FUELS_TECHS, "uvek_gas_property_carriers")["natural gas"][0]
    )
    return [high, pipe, auxiliary, low, burner], low, burner, spec


def test_delivery_energy_basis_and_downstream_origin_preserve_network_recipe():
    database, low, burner, spec = setup()
    original = deepcopy(low["exchanges"])
    unaffected = deepcopy(burner["exchanges"][:2] + burner["exchanges"][3:])
    for _ in range(2):
        with pytest.warns(UserWarning, match="lubricant"):
            annotate_cng_properties(database, carrier_specs=[spec])
        props = low[PROPERTIES_KEY]
        assert props["energy MJ per unit"] == 1
        assert props["upstream fuel energy MJ per unit"] == pytest.approx(1.02)
        assert supplied_energy(low) == pytest.approx(1.02)
        assert props["fossil CO2 kg per unit"] == pytest.approx(0.04)
        assert props["non-fossil CO2 kg per unit"] == pytest.approx(0.02)
        assert low["exchanges"] == original
        update_vehicle_fuel_co2(
            database,
            "3.10",
            recorded_exhaust_filters=[
                {"exact": True, "fltr": {"name": burner["name"], "unit": "megajoule"}}
            ],
        )
        assert (
            burner["exchanges"][:2]
            + [e for e in burner["exchanges"][2:] if e["name"] == "Methane, fossil"]
            == unaffected
        )
        totals = {
            n: sum(e["amount"] for e in burner["exchanges"] if e.get("name") == n)
            for n in ["Carbon dioxide, fossil", "Carbon dioxide, non-fossil"]
        }
        assert totals == pytest.approx(
            {"Carbon dioxide, fossil": 0.0376, "Carbon dioxide, non-fossil": 0.0188}
        )


def test_unknown_origin_remains_unknown_and_prior_annotations_are_refreshed():
    database, low, burner, spec = setup()
    with pytest.warns(UserWarning, match="lubricant"):
        annotate_cng_properties(database, carrier_specs=[spec])
    upstream = database[0][PROPERTIES_KEY]
    upstream["fossil CO2 kg per unit"] = 0.03
    upstream["unresolved CO2 kg per unit"] = 0.01
    with pytest.warns(UserWarning, match="lubricant"):
        annotate_cng_properties(database, carrier_specs=[spec])
    assert low[PROPERTIES_KEY]["unresolved CO2 kg per unit"] == pytest.approx(0.01)
    assert low[PROPERTIES_KEY]["fossil CO2 kg per unit"] == pytest.approx(0.03)
    assert (
        vehicle_fuel_carbon(burner, {activity_key(a): a for a in database})["status"]
        == "unresolved"
    )


@pytest.mark.parametrize(
    "failure", ["unit", "missing", "negative", "unknown input", "cycle"]
)
def test_unreviewed_or_invalid_carrier_supply_cannot_acquire_an_invented_origin(
    failure,
):
    database, low, _, spec = setup()
    if failure == "unit":
        database[0][PROPERTIES_KEY]["reference unit"] = "cubic meter"
    elif failure == "missing":
        database[0].pop(PROPERTIES_KEY)
    elif failure == "negative":
        low["exchanges"][1]["amount"] = -1
    elif failure == "unknown input":
        low["exchanges"].append(purchase(activity("unreviewed gas"), 0.2))
    else:
        low["exchanges"][1] = purchase(low, 1)
        spec["gas input"] = {k: low[k] for k in ["name", "unit"]}
        spec["gas input"]["product"] = low["reference product"]
    before = deepcopy(low["exchanges"])
    with pytest.raises(ValueError):
        annotate_cng_properties(database, carrier_specs=[spec])
    assert low["exchanges"] == before


def test_carrier_limitations_are_warned_once_per_declaration_not_per_country():
    database, low, _, spec = setup()
    other = deepcopy(low)
    other["location"] = "RER"
    other["exchanges"][0]["location"] = "RER"
    database.append(other)
    with pytest.warns(UserWarning, match="lubricant") as captured:
        annotate_cng_properties(database, carrier_specs=[spec])
    assert len(captured) == 1
    assert low[PROPERTIES_KEY]["carrier limitations"] == spec["limitations"]
    assert other[PROPERTIES_KEY]["carrier limitations"] == spec["limitations"]


@pytest.mark.parametrize("native_supply", [False, True])
@pytest.mark.parametrize("canonical_electricity_product", [False, True])
def test_native_station_and_kg_forwarder_share_final_gas_origin_without_inventory_changes(
    native_supply,
    canonical_electricity_product,
):
    spec = deepcopy(
        get_mapping(FUELS_TECHS, "uvek_gas_property_carriers")["natural gas"][1]
    )
    upstream = activity("market for natural gas, high pressure", "cubic meter")
    upstream["reference product"] = "natural gas, high pressure"
    if native_supply:
        upstream = activity("Natural gas, high pressure, at consumer")
    upstream[PROPERTIES_KEY] = {
        "reference unit": upstream["unit"],
        "energy MJ per unit": 1 if native_supply else 36,
        "fossil CO2 kg per unit": 0.04 if native_supply else 1.44,
        "non-fossil CO2 kg per unit": 0.02 if native_supply else 0.72,
        "unresolved CO2 kg per unit": 0,
    }
    electricity = activity("Electricity, medium voltage, at grid", "kilowatt hour")
    if canonical_electricity_product:
        electricity["reference product"] = "electricity, medium voltage"
    station = activity(
        spec["selector"]["fltr"]["name"],
        "kilogram",
        [
            purchase(upstream, 1.4),
            purchase(
                electricity,
                0.2,
                **{"uncertainty type": 3, "loc": 0.2, "scale": 0.02},
            ),
        ],
    )
    station["location"] = "NEU"
    station["exchanges"][0]["location"] = "NEU"
    forwarder = activity(station["name"], "kilogram", [purchase(station, 1)])
    original = deepcopy([station["exchanges"], forwarder["exchanges"]])
    with pytest.warns(UserWarning, match="nominal"):
        annotate_cng_properties([upstream, station, forwarder], carrier_specs=[spec])
    assert [station["exchanges"], forwarder["exchanges"]] == original
    for supplier in [station, forwarder]:
        props = supplier[PROPERTIES_KEY]
        assert props["energy MJ per unit"] == 46.5
        assert props["fossil CO2 kg per unit"] == pytest.approx(46.5 * 0.04)
        assert props["non-fossil CO2 kg per unit"] == pytest.approx(46.5 * 0.02)
