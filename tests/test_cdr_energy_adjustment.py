from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from premise.carbon_dioxide_removal import CarbonDioxideRemoval


def get_cdr_transform(electricity_efficiency=2.0, heat_efficiency=0.5):
    technology = "direct air capture (solvent) with storage"
    cdr = object.__new__(CarbonDioxideRemoval)
    cdr.year = 2030
    cdr.iam_data = SimpleNamespace(
        cdr_technology_efficiencies=xr.DataArray(
            np.array([[[[electricity_efficiency]], [[heat_efficiency]]]]),
            dims=("variables", "carrier", "region", "year"),
            coords={
                "variables": [technology],
                "carrier": ["electricity", "heat"],
                "region": ["EUR"],
                "year": [2030],
            },
        )
    )
    return cdr, technology


def test_cdr_efficiency_adjustment_scales_electricity_and_heat_separately():
    cdr, technology = get_cdr_transform()
    dataset = {
        "name": "carbon dioxide, captured and stored, with DAC",
        "reference product": "carbon dioxide, captured and stored",
        "location": "EUR",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market group for electricity, medium voltage",
                "product": "electricity, medium voltage",
                "amount": 10.0,
                "type": "technosphere",
                "unit": "kilowatt hour",
            },
            {
                "name": "market for heat, district or industrial, natural gas",
                "product": "heat, district or industrial, natural gas",
                "amount": 8.0,
                "type": "technosphere",
                "unit": "megajoule",
            },
            {
                "name": "market for diesel, burned in agricultural machinery",
                "product": "diesel, burned in agricultural machinery",
                "amount": 2.0,
                "type": "technosphere",
                "unit": "megajoule",
            },
            {
                "name": "sorbent production",
                "product": "sorbent",
                "amount": 4.0,
                "type": "technosphere",
                "unit": "kilogram",
            },
            {
                "name": "Water",
                "amount": 7.0,
                "type": "biosphere",
                "unit": "cubic meter",
            },
            {
                "name": "Carbon dioxide, fossil",
                "amount": 3.0,
                "type": "biosphere",
                "unit": "kilogram",
            },
        ],
    }

    cdr.adjust_cdr_efficiency(dataset, technology)

    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["market group for electricity, medium voltage"] == pytest.approx(5.0)
    assert amounts["market for heat, district or industrial, natural gas"] == pytest.approx(
        12.0
    )
    assert amounts["market for diesel, burned in agricultural machinery"] == pytest.approx(
        3.0
    )
    assert amounts["sorbent production"] == pytest.approx(4.0)
    assert amounts["Water"] == pytest.approx(7.0)
    assert amounts["Carbon dioxide, fossil"] == pytest.approx(3.0)
    assert dataset["log parameters"][
        "electricity efficiency scaling factor"
    ] == pytest.approx(0.5)
    assert dataset["log parameters"]["heat efficiency scaling factor"] == pytest.approx(
        1.5
    )
