"""CNG without IAM blending needs a complete native fossil-origin proof."""

from copy import deepcopy

import pytest

from premise.fuels.carbon import update_vehicle_fuel_co2, vehicle_fuel_carbon
from premise.fuels.properties import (
    PROPERTIES_KEY,
    activity_key,
    annotate_cng_properties,
)

GAS = "natural gas, high pressure"
CNG = "natural gas, medium pressure, vehicle grade"


def activity(name, product=GAS, unit="cubic meter", inputs=()):
    return {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "unit": unit,
        "exchanges": [
            {
                "name": name,
                "product": product,
                "location": "GLO",
                "unit": unit,
                "type": "production",
                "amount": 1.0,
            },
            *inputs,
        ],
    }


def purchase(ds, amount=1):
    return {
        "name": ds["name"],
        "product": ds["reference product"],
        "location": ds["location"],
        "unit": ds["unit"],
        "type": "technosphere",
        "amount": amount,
    }


@pytest.fixture
def supply():
    extraction = activity(
        "petroleum and gas production, onshore",
        inputs=[
            {
                "name": "Gas, natural",
                "categories": ("natural resource", "in ground"),
                "unit": "standard cubic meter",
                "type": "biosphere",
                "amount": 1.1,
            }
        ],
    )
    gas = activity(
        "market for natural gas, high pressure", inputs=[purchase(extraction)]
    )
    compression = activity(
        "natural gas production, medium pressure, vehicle grade",
        CNG,
        "kilogram",
        [purchase(gas, 1.3605442176870748)],
    )
    market = activity(
        "market for natural gas, medium pressure, vehicle grade",
        CNG,
        "kilogram",
        [purchase(compression)],
    )
    return [extraction, gas, compression, market]


def test_native_cng_metadata_and_vehicle_co2_follow_explicit_extraction(supply):
    before = deepcopy(supply)
    annotate_cng_properties(supply)
    properties = supply[-1][PROPERTIES_KEY]
    assert properties["energy MJ per unit"] == 46.5
    assert properties["fossil CO2 kg per unit"] == pytest.approx(2.79)
    assert properties["non-fossil CO2 kg per unit"] == 0
    assert properties["unresolved CO2 kg per unit"] == 0
    evidence = supply[-2][PROPERTIES_KEY]["components"][0]["native gas supply evidence"]
    assert evidence["fossil extraction activity keys"] == [activity_key(supply[0])]
    assert evidence["activity count"] == 2
    assert (
        supply[:2] == before[:2]
    )  # No new properties or changes on native gas providers.
    assert [d["exchanges"] for d in supply] == [d["exchanges"] for d in before]

    car = activity(
        "transport, passenger car, compressed gas, Medium, EURO-6",
        "transport, passenger car, EURO-6",
        "kilometer",
        [
            purchase(supply[-1], 0.1),
            {
                "name": "Carbon dioxide, fossil",
                "unit": "kilogram",
                "type": "biosphere",
                "categories": ("air", "unspecified"),
                "amount": 0.3,
            },
            {
                "name": "Methane, fossil",
                "unit": "kilogram",
                "type": "biosphere",
                "categories": ("air", "unspecified"),
                "amount": 0.0001,
            },
        ],
    )
    supply.append(car)
    update_vehicle_fuel_co2(supply, "3.12")
    assert next(
        e["amount"] for e in car["exchanges"] if e["name"] == "Carbon dioxide, fossil"
    ) == pytest.approx(0.279)
    assert (
        next(e["amount"] for e in car["exchanges"] if e["name"] == "Methane, fossil")
        == 0.0001
    )
    balance = vehicle_fuel_carbon(car, {activity_key(d): d for d in supply})
    assert balance["status"] == "resolved"
    once = deepcopy(supply)
    annotate_cng_properties(supply)
    update_vehicle_fuel_co2(supply, "3.12")
    assert supply == once


@pytest.mark.parametrize("connected", [False, True])
def test_trade_loop_requires_a_fossil_anchor_on_every_branch(supply, connected):
    loop = activity("loop")
    loop["exchanges"].append(purchase(loop))
    if connected:
        loop["exchanges"].append(purchase(supply[0]))
    supply[1]["exchanges"].append(purchase(loop))
    supply.append(loop)
    annotate_cng_properties(supply)
    assert (PROPERTIES_KEY in supply[3]) is connected


@pytest.mark.parametrize(
    "case",
    ["missing", "ambiguous", "biogenic", "unresolved", "synthetic", "wrong-unit"],
)
def test_unknown_or_mixed_branch_is_not_treated_as_fossil(supply, case):
    other = activity("other gas")
    supply[1]["exchanges"].append(purchase(other))
    if case == "ambiguous":
        supply.extend([other, deepcopy(other)])
    elif case in {"biogenic", "unresolved"}:
        other[PROPERTIES_KEY] = {
            "reference unit": "cubic meter",
            "energy MJ per unit": 36,
            "fossil CO2 kg per unit": 0,
            "non-fossil CO2 kg per unit": 2.16 if case == "biogenic" else 0,
            "unresolved CO2 kg per unit": 2.16 if case == "unresolved" else 0,
        }
        supply.append(other)
    elif case == "synthetic":
        supply.append(other)  # Gas output without a verified carbon source.
    elif case == "wrong-unit":
        supply[1]["exchanges"][-1]["unit"] = "kilogram"
        supply.append(other)
    annotate_cng_properties(supply)
    assert PROPERTIES_KEY not in supply[2]
    assert PROPERTIES_KEY not in supply[3]


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-resource",
        "wrong-compartment",
        "wrong-producer",
        "negative-input",
        "nan-input",
    ],
)
def test_names_alone_or_invalid_inputs_do_not_establish_origin(supply, mutation):
    if mutation == "missing-resource":
        supply[0]["exchanges"].pop()
    elif mutation == "wrong-compartment":
        supply[0]["exchanges"][-1]["categories"] = ("air",)
    elif mutation == "wrong-producer":
        supply[0]["name"] = "synthetic natural gas"
        supply[1]["exchanges"][-1]["name"] = "synthetic natural gas"
    else:
        supply[1]["exchanges"][-1]["amount"] = (
            -1 if mutation == "negative-input" else float("nan")
        )
    annotate_cng_properties(supply)
    assert PROPERTIES_KEY not in supply[3]


def test_changed_supply_invalidates_previously_generated_cng_properties(supply):
    annotate_cng_properties(supply)
    assert PROPERTIES_KEY in supply[3]
    supply[0][
        "exchanges"
    ].pop()  # Its original extraction evidence is no longer present.
    annotate_cng_properties(supply)
    assert PROPERTIES_KEY not in supply[2]
    assert PROPERTIES_KEY not in supply[3]


@pytest.mark.parametrize("unit", ["kilogram", "cubic meter"])
def test_liquid_gas_needs_its_own_energy_conversion(supply, unit):
    gas = supply[1]
    gas["reference product"] = "natural gas, liquefied"
    gas["unit"] = unit
    gas["exchanges"][0].update(product="natural gas, liquefied", unit=unit)
    supply[2]["exchanges"][-1] = purchase(gas, 1.36)
    annotate_cng_properties(supply)
    assert PROPERTIES_KEY not in supply[3]
