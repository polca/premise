from types import SimpleNamespace

import pytest

from premise.cement import Cement
from premise.utils import get_fuel_properties


def get_cement_transform(efficiency_change):
    cement = object.__new__(Cement)
    cement.fuels_specs = get_fuel_properties()
    cement.iam_data = SimpleNamespace(cement_technology_efficiencies=None)
    cement.find_iam_efficiency_change = lambda **kwargs: efficiency_change
    return cement


def clinker_dataset(coal_energies, other_visible_energy=1.0):
    coal_lhv = get_fuel_properties()["hard coal"]["lhv"]["value"]
    exchanges = [
        {
            "name": "diesel, burned in building machine",
            "amount": other_visible_energy,
            "type": "technosphere",
            "unit": "megajoule",
        },
        {
            "name": "Carbon dioxide, fossil",
            "amount": 1.0,
            "type": "biosphere",
            "unit": "kilogram",
        },
        {
            "name": "Carbon dioxide, non-fossil",
            "amount": 0.1,
            "type": "biosphere",
            "unit": "kilogram",
        },
    ]

    for index, energy in enumerate(coal_energies):
        exchanges.append(
            {
                "name": "market for hard coal",
                "product": "hard coal",
                "location": f"R{index}",
                "amount": energy / coal_lhv,
                "type": "technosphere",
                "unit": "kilogram",
            }
        )

    return {
        "name": "clinker production",
        "reference product": "clinker",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": exchanges,
    }


def hard_coal_energy(dataset):
    coal_lhv = get_fuel_properties()["hard coal"]["lhv"]["value"]
    return sum(
        exc["amount"] * coal_lhv
        for exc in dataset["exchanges"]
        if exc["type"] == "technosphere" and "hard coal" in exc["name"]
    )


def hard_coal_energies(dataset):
    coal_lhv = get_fuel_properties()["hard coal"]["lhv"]["value"]
    return [
        exc["amount"] * coal_lhv
        for exc in dataset["exchanges"]
        if exc["type"] == "technosphere" and "hard coal" in exc["name"]
    ]


def fossil_co2(dataset):
    return next(
        exc["amount"]
        for exc in dataset["exchanges"]
        if exc["type"] == "biosphere" and exc["name"] == "Carbon dioxide, fossil"
    )


def exchange(dataset, name):
    return next(exc for exc in dataset["exchanges"] if exc["name"] == name)


def test_clinker_energy_adjustment_scales_split_coal_once():
    cement = get_cement_transform(efficiency_change=10)
    dataset = clinker_dataset(coal_energies=[0.6, 0.4], other_visible_energy=1.0)

    cement.adjust_process_efficiency(dataset, "cement, dry feed rotary kiln")

    assert dataset["log parameters"]["new energy input per ton clinker"] == 3100
    assert dataset["log parameters"][
        "hidden secondary fuel energy per kg clinker"
    ] == pytest.approx(1.4)
    assert dataset["log parameters"][
        "new accounted fuel energy per kg clinker"
    ] == pytest.approx(3.1)
    assert hard_coal_energy(dataset) == pytest.approx(0.7)
    assert hard_coal_energies(dataset) == pytest.approx([0.42, 0.28])
    assert fossil_co2(dataset) == pytest.approx(1.0 - 0.3 * 0.098)


def test_clinker_efficient_kiln_uses_3000_kj_floor():
    cement = get_cement_transform(efficiency_change=10)
    dataset = clinker_dataset(coal_energies=[0.6, 0.4], other_visible_energy=1.0)

    cement.adjust_process_efficiency(dataset, "cement, dry feed rotary kiln, efficient")

    assert dataset["log parameters"]["new energy input per ton clinker"] == 3000
    assert dataset["log parameters"][
        "new accounted fuel energy per kg clinker"
    ] == pytest.approx(3.0)
    assert hard_coal_energy(dataset) == pytest.approx(0.6)


def test_clinker_energy_adjustment_does_not_make_coal_negative():
    cement = get_cement_transform(efficiency_change=10)
    dataset = clinker_dataset(coal_energies=[0.12, 0.08], other_visible_energy=1.0)

    cement.adjust_process_efficiency(dataset, "cement, dry feed rotary kiln, efficient")

    assert hard_coal_energy(dataset) == pytest.approx(0.0)
    assert all(energy >= 0 for energy in hard_coal_energies(dataset))
    assert dataset["log parameters"][
        "new accounted fuel energy per kg clinker"
    ] == pytest.approx(3.2)
    assert dataset["log parameters"][
        "unmet thermal energy change per kg clinker"
    ] == pytest.approx(-0.2)


def test_clinker_energy_adjustment_documents_dataset_and_exchanges():
    cement = get_cement_transform(efficiency_change=10)
    dataset = clinker_dataset(coal_energies=[0.6, 0.4], other_visible_energy=1.0)

    cement.adjust_process_efficiency(dataset, "cement, dry feed rotary kiln")

    assert "original visible fuel inputs" in dataset["comment"]
    assert "hidden secondary-fuel contribution" in dataset["comment"]
    assert "new accounted kiln fuel demand" in dataset["comment"]
    assert "hard coal changes from" in dataset["comment"]

    coal_comments = [
        exc["comment"]
        for exc in dataset["exchanges"]
        if exc["type"] == "technosphere" and "hard coal" in exc["name"]
    ]
    assert coal_comments
    assert all("amount changes from" in comment for comment in coal_comments)
    assert all("scaled proportionally" in comment for comment in coal_comments)

    diesel_comment = exchange(dataset, "diesel, burned in building machine")["comment"]
    assert "visible kiln fuel energy" in diesel_comment
    assert "not changed by the cement efficiency adjustment" in diesel_comment

    fossil_comment = exchange(dataset, "Carbon dioxide, fossil")["comment"]
    assert "amount before the fuel efficiency adjustment" in fossil_comment
    assert "calcination CO2 is not scaled" in fossil_comment

    biogenic_comment = exchange(dataset, "Carbon dioxide, non-fossil")["comment"]
    assert "Non-fossil CO2 from secondary fuels is not changed" in biogenic_comment
