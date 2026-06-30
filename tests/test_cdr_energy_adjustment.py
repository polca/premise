from types import SimpleNamespace

import numpy as np
from openpyxl import load_workbook
import pytest
import xarray as xr

from premise.carbon_dioxide_removal import CarbonDioxideRemoval
from premise.filesystem_constants import INVENTORY_DIR


CDR_INVENTORY = INVENTORY_DIR / "lci-carbon-capture.xlsx"


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


def get_inventory_activity(name):
    workbook = load_workbook(CDR_INVENTORY, data_only=True, read_only=True)
    worksheet = workbook["DAC"]
    try:
        rows = list(worksheet.iter_rows(values_only=True))
        for index, row in enumerate(rows):
            if row and row[0] == "Activity" and row[1] == name:
                activity_rows = []
                for activity_row in rows[index + 1 :]:
                    if activity_row and activity_row[0] == "Activity":
                        break
                    activity_rows.append(activity_row)

                comment = next(
                    row[1] for row in activity_rows if row and row[0] == "comment"
                )
                header_index = next(
                    idx
                    for idx, row in enumerate(activity_rows)
                    if row and row[0] == "name"
                )
                header = activity_rows[header_index]
                exchanges = [
                    {
                        key: value
                        for key, value in zip(header, row)
                        if key is not None and value is not None
                    }
                    for row in activity_rows[header_index + 1 :]
                    if row and row[0]
                ]
                return comment, exchanges
    finally:
        workbook.close()

    raise AssertionError(f"Activity not found: {name}")


def test_wood_ccs_inventory_uses_volkart_wood_energy_penalty():
    comment, exchanges = get_inventory_activity(
        "carbon dioxide, captured and stored, at wood burning power plant, "
        "pipeline 200km, storage 1000m"
    )

    assert "host power plant" in comment
    assert "Volkart et al. (2013)" in comment
    assert "3.565 MJ additional wood-derived host energy" in comment
    assert "charcoal input" in comment

    assert not any(exc["name"] == "market for charcoal" for exc in exchanges)
    assert not any(
        exc["name"] == "market group for electricity, low voltage"
        for exc in exchanges
    )

    heat = next(
        exc
        for exc in exchanges
        if exc["name"] == "heat and power co-generation, wood chips, 6667 kW"
    )
    assert heat["amount"] == pytest.approx(6.0 / (0.187 * 0.9 / 0.1))
    assert heat["unit"] == "megajoule"
    assert (
        heat["reference product"]
        == "heat, district or industrial, other than natural gas"
    )

    mea = next(exc for exc in exchanges if exc["name"] == "market for monoethanolamine")
    assert mea["amount"] == pytest.approx(2.84e-4)

    spent_solvent = next(
        exc
        for exc in exchanges
        if exc["name"] == "treatment of spent solvent mixture, hazardous waste incineration"
    )
    assert spent_solvent["amount"] == pytest.approx(-2.27e-4)

    activated_carbon = next(
        exc for exc in exchanges if exc["name"] == "market for activated carbon, granular"
    )
    assert activated_carbon["amount"] == pytest.approx(8.26e-5)


def test_hydrogen_ccs_inventory_uses_antonini_mdea_and_electricity_penalty():
    comment, exchanges = get_inventory_activity(
        "carbon dioxide, captured and stored, from a hydrogen production plant "
        "using steam methane reforming of biomethane"
    )

    assert "host SMR plant" in comment
    assert "methyldiethanolamine" in comment
    assert "0.11598 kWh per kg CO2 stored" in comment
    assert "former 4.0556 MJ/kg CO2 biomethane heat placeholder is removed" in comment
    assert "Final CO2 compression electricity is omitted" in comment

    assert not any(exc["name"] == "market for monoethanolamine" for exc in exchanges)
    assert not any(
        exc["name"] == "heat production, biomethane, at boiler condensing modulating <100kW"
        for exc in exchanges
    )
    assert not any(
        exc["name"] == "market group for electricity, low voltage"
        and "final compression" in exc.get("comment", "").lower()
        for exc in exchanges
    )

    mdea = next(
        exc for exc in exchanges if exc["name"] == "market for methyldiethanolamine"
    )
    assert mdea["amount"] == pytest.approx(3.4e-5)
    assert mdea["reference product"] == "methyldiethanolamine"

    electricity = next(
        exc
        for exc in exchanges
        if exc["name"] == "market group for electricity, low voltage"
    )
    assert electricity["amount"] == pytest.approx(
        (0.005477565380988522 - (-0.000466178173455711))
        / 0.05124742496104852
    )
    assert electricity["unit"] == "kilowatt hour"
    assert electricity["reference product"] == "electricity, low voltage"


def test_fermentation_ccs_inventory_has_no_solvent_or_extra_capture_energy():
    comment, exchanges = get_inventory_activity(
        "carbon dioxide, captured and stored, from a biomass fermentation plant"
    )

    assert "already high-purity fermentation CO2 stream" in comment
    assert "no solvent, sorbent or regeneration heat" in comment
    assert "omitted here to avoid double-counting compression" in comment

    assert not any(
        "electricity" in exc.get("name", "").lower() for exc in exchanges
    )
    assert not any("heat" in exc.get("name", "").lower() for exc in exchanges)
    assert not any("ethanolamine" in exc.get("name", "").lower() for exc in exchanges)
    assert any(
        exc["name"] == "carbon dioxide compression, transport and storage"
        for exc in exchanges
    )
