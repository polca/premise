"""Conservation and selection regressions for Swiss hydropower water corrections."""

from copy import deepcopy
import math

import pytest

from premise.electricity import Electricity


def plant(
    name="electricity production, hydro, pumped storage",
    withdrawal=4.3,
    location="CH",
    categories=("water",),
):
    def flow(name, categories, amount):
        return {
            "name": name,
            "categories": categories,
            "amount": amount,
            "type": "biosphere",
            "unit": "cubic meter",
            "uncertainty type": 2,
            "loc": math.log(amount),
            "scale": 0.3,
        }

    return {
        "name": name,
        "reference product": "electricity, high voltage",
        "location": location,
        "unit": "kilowatt hour",
        "exchanges": [
            flow(
                "Water, turbine use, unspecified natural origin",
                ("natural resource", "in water"),
                withdrawal,
            ),
            flow("Water", ("air",), 0.029221678),
            flow("Water", categories, withdrawal - 0.029221678),
            {
                "name": "electricity",
                "type": "technosphere",
                "amount": 1.43,
                "unit": "kilowatt hour",
            },
        ],
    }


def correct(database):
    electricity = Electricity.__new__(Electricity)
    electricity.database = database
    electricity.correct_hydropower_water_emissions()


@pytest.mark.parametrize("withdrawal", [0.81, 4.3, 7.8])
@pytest.mark.parametrize(
    "suffix", ["", "_arbitrary external pathway", ", renewable energy products"]
)
def test_pumped_storage_conserves_its_own_withdrawal(withdrawal, suffix):
    ds = plant(withdrawal=withdrawal)
    ds["name"] += suffix
    before = deepcopy(ds)
    correct([ds])
    incoming, evaporated, returned, electricity = ds["exchanges"]
    assert incoming == before["exchanges"][0]
    assert electricity == before["exchanges"][3]
    assert evaporated["amount"] == pytest.approx(0.00175)
    assert returned["amount"] == pytest.approx(withdrawal - 0.00175)
    assert incoming["amount"] == pytest.approx(
        evaporated["amount"] + returned["amount"]
    )
    for flow in [evaporated, returned]:
        assert math.exp(flow["loc"]) == pytest.approx(flow["amount"])
        assert flow["scale"] == 0.3
    after = deepcopy(ds)
    correct([ds])
    assert ds == after


@pytest.mark.parametrize("reverse", [False, True])
def test_both_families_are_corrected_in_either_database_order(reverse):
    reservoir = plant("electricity production, hydro, reservoir, alpine region", 0.81)
    pump = plant(categories=("water", "unspecified"))
    database = [reservoir, pump]
    correct(database[::-1] if reverse else database)
    assert reservoir["exchanges"][2]["amount"] == pytest.approx(0.80825)
    assert pump["exchanges"][2]["amount"] == pytest.approx(4.29825)


def test_other_geographies_technologies_and_units_are_unchanged():
    database = [
        plant(location="FR"),
        plant("electricity production, hydro, run-of-river"),
        plant("electricity production, hydro, pumped storage plant construction"),
    ]
    wrong_unit = plant()
    wrong_unit["unit"] = "megajoule"
    database.append(wrong_unit)
    before = deepcopy(database)
    correct(database)
    assert database == before


@pytest.mark.parametrize("amount", [float("nan"), -1, 0.001])
def test_invalid_withdrawal_is_rejected_before_mutation(amount):
    ds = plant()
    ds["exchanges"][0]["amount"] = amount
    before = deepcopy(ds["exchanges"][1:])
    with pytest.raises(ValueError, match="Invalid hydropower water balance"):
        correct([ds])
    assert ds["exchanges"][1:] == before


@pytest.mark.parametrize("index", [0, 1, 2])
def test_missing_water_balance_component_fails_loudly(index):
    ds = plant()
    ds["exchanges"].pop(index)
    before = deepcopy(ds)
    with pytest.raises(ValueError, match="Cannot balance hydropower"):
        correct([ds])
    assert ds == before
