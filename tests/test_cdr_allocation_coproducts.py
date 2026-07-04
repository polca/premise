import pytest

from premise.electricity import Electricity
from premise.fuels.base import Fuels
from premise.transformation import BaseTransformation


def co2_uptake(amount=1.0):
    return {
        "name": "Carbon dioxide, in air",
        "categories": ("natural resource", "in air"),
        "amount": amount,
        "loc": amount,
        "type": "biosphere",
        "unit": "kilogram",
    }


def production_exchange(name, product, unit):
    return {
        "name": name,
        "product": product,
        "amount": 1.0,
        "type": "production",
        "unit": unit,
    }


def co2_storage_input(
    amount=1.0,
    name="carbon dioxide compression, transport and storage",
):
    return {
        "name": name,
        "product": "carbon dioxide, stored",
        "amount": amount,
        "loc": amount,
        "type": "technosphere",
        "unit": "kilogram",
    }


def non_fossil_co2(amount=1.0):
    return {
        "name": "Carbon dioxide, non-fossil",
        "categories": ("air", "unspecified"),
        "amount": amount,
        "loc": amount,
        "type": "biosphere",
        "unit": "kilogram",
    }


def test_zero_atmospheric_co2_uptake_sets_exchange_to_zero():
    transform = object.__new__(BaseTransformation)
    dataset = {
        "name": "activity with embedded CDR credit",
        "reference product": "product",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [
            production_exchange(
                "activity with embedded CDR credit", "product", "kilogram"
            ),
            co2_uptake(2.5),
            {
                "name": "Carbon dioxide, non-fossil",
                "amount": 1.0,
                "type": "biosphere",
                "unit": "kilogram",
            },
        ],
    }

    removed = transform.zero_atmospheric_co2_uptake(
        dataset, reason="Removed for CDR allocation."
    )

    assert removed == pytest.approx(2.5)
    uptake = next(
        exc for exc in dataset["exchanges"] if exc["name"] == "Carbon dioxide, in air"
    )
    assert uptake["amount"] == 0
    assert uptake["loc"] == 0
    assert "Removed for CDR allocation." in uptake["comment"]
    assert dataset["log parameters"][
        "atmospheric CO2 uptake removed for CDR allocation"
    ] == pytest.approx(2.5)
    assert next(
        exc
        for exc in dataset["exchanges"]
        if exc["name"] == "Carbon dioxide, non-fossil"
    )["amount"] == pytest.approx(1.0)


def test_zero_negative_non_fossil_co2_emissions_sets_only_negative_exchange_to_zero():
    transform = object.__new__(BaseTransformation)
    dataset = {
        "name": "activity with negative non-fossil CO2",
        "reference product": "product",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [
            production_exchange(
                "activity with negative non-fossil CO2", "product", "kilogram"
            ),
            non_fossil_co2(-0.7),
            non_fossil_co2(0.2),
        ],
    }

    removed = transform.zero_negative_non_fossil_co2_emissions(
        dataset, reason="Removed for CDR allocation."
    )

    assert removed == pytest.approx(0.7)
    assert dataset["exchanges"][1]["amount"] == 0
    assert dataset["exchanges"][1]["loc"] == 0
    assert dataset["exchanges"][2]["amount"] == pytest.approx(0.2)
    assert "Removed for CDR allocation." in dataset["exchanges"][1]["comment"]
    assert dataset["log parameters"][
        "negative non-fossil CO2 emission removed for CDR allocation"
    ] == pytest.approx(0.7)


def test_zero_carbon_dioxide_storage_inputs_sets_storage_exchange_to_zero():
    transform = object.__new__(BaseTransformation)
    dataset = {
        "name": "activity with embedded storage input",
        "reference product": "product",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [
            production_exchange(
                "activity with embedded storage input", "product", "kilogram"
            ),
            co2_storage_input(1.7),
            co2_storage_input(
                0.4,
                name=(
                    "carbon dioxide storage at wood burning power plant 20 MW "
                    "post, pipeline 200km, storage 1000m"
                ),
            ),
            {
                "name": "market for transport, freight, lorry",
                "product": "transport, freight, lorry",
                "amount": 2.0,
                "type": "technosphere",
                "unit": "ton kilometer",
            },
        ],
    }

    removed = transform.zero_carbon_dioxide_storage_inputs(
        dataset, reason="Removed for CDR allocation."
    )

    assert removed == pytest.approx(2.1)
    storage_inputs = [
        exc
        for exc in dataset["exchanges"]
        if exc.get("product") == "carbon dioxide, stored"
    ]
    assert all(exc["amount"] == 0 for exc in storage_inputs)
    assert all(exc["loc"] == 0 for exc in storage_inputs)
    assert all(
        "Removed for CDR allocation." in exc["comment"] for exc in storage_inputs
    )
    assert dataset["log parameters"][
        "carbon dioxide storage input removed for CDR allocation"
    ] == pytest.approx(2.1)
    assert next(
        exc
        for exc in dataset["exchanges"]
        if exc["name"] == "market for transport, freight, lorry"
    )["amount"] == pytest.approx(2.0)


def test_electricity_removes_cdr_credit_only_from_biomass_ccs_power():
    electricity = object.__new__(Electricity)
    biomass_ccs = {
        "name": (
            "electricity production, at wood burning power plant, post, pipeline "
            "200km, storage 1000m"
        ),
        "reference product": "electricity",
        "location": "WEU",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                "electricity production, at wood burning power plant, post, pipeline "
                "200km, storage 1000m",
                "electricity",
                "kilowatt hour",
            ),
            co2_uptake(1.2),
            co2_storage_input(
                1.0,
                name=(
                    "carbon dioxide storage at wood burning power plant 20 MW "
                    "post, pipeline 200km, storage 1000m"
                ),
            ),
        ],
    }
    coal_ccs = {
        "name": (
            "electricity production, at hard coal-fired power plant, post, pipeline "
            "200km, storage 1000m"
        ),
        "reference product": "electricity",
        "location": "WEU",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                "electricity production, at hard coal-fired power plant, post, "
                "pipeline 200km, storage 1000m",
                "electricity",
                "kilowatt hour",
            ),
            co2_uptake(9.9),
            co2_storage_input(
                9.9,
                name=(
                    "carbon dioxide storage from hard coal, post, pipeline 400km, "
                    "storage 3000m"
                ),
            ),
        ],
    }
    electricity.powerplant_map = {
        "Biomass ST CCS": [biomass_ccs],
        "Coal PC CCS": [coal_ccs],
    }

    removed = electricity.remove_cdr_credit_from_biomass_ccs_power_plants()

    assert removed == pytest.approx(2.2)
    assert biomass_ccs["exchanges"][1]["amount"] == 0
    assert biomass_ccs["exchanges"][2]["amount"] == 0
    assert coal_ccs["exchanges"][1]["amount"] == pytest.approx(9.9)
    assert coal_ccs["exchanges"][2]["amount"] == pytest.approx(9.9)


def test_electricity_removes_negative_non_fossil_co2_from_ccs_power():
    electricity = object.__new__(Electricity)
    biomass_ccs = {
        "name": (
            "electricity production, at wood burning power plant, post, pipeline "
            "200km, storage 1000m"
        ),
        "reference product": "electricity",
        "location": "WEU",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                "electricity production, at wood burning power plant, post, pipeline "
                "200km, storage 1000m",
                "electricity",
                "kilowatt hour",
            ),
            non_fossil_co2(-0.4),
        ],
    }
    coal_ccs = {
        "name": (
            "electricity production, at hard coal-fired power plant, post, pipeline "
            "200km, storage 1000m"
        ),
        "reference product": "electricity",
        "location": "WEU",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                "electricity production, at hard coal-fired power plant, post, "
                "pipeline 200km, storage 1000m",
                "electricity",
                "kilowatt hour",
            ),
            non_fossil_co2(-0.7),
            non_fossil_co2(0.3),
        ],
    }
    non_ccs = {
        "name": "electricity production, at wood burning power plant",
        "reference product": "electricity",
        "location": "WEU",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                "electricity production, at wood burning power plant",
                "electricity",
                "kilowatt hour",
            ),
            non_fossil_co2(-0.9),
        ],
    }
    electricity.powerplant_map = {
        "Biomass ST CCS": [biomass_ccs],
        "Coal PC CCS": [coal_ccs],
        "Biomass ST": [non_ccs],
    }

    removed = electricity.zero_negative_non_fossil_co2_from_ccs_power_plants()

    assert removed == pytest.approx(1.1)
    assert biomass_ccs["exchanges"][1]["amount"] == 0
    assert coal_ccs["exchanges"][1]["amount"] == 0
    assert coal_ccs["exchanges"][2]["amount"] == pytest.approx(0.3)
    assert non_ccs["exchanges"][1]["amount"] == pytest.approx(-0.9)


def test_fuels_remove_cdr_credit_only_from_ccs_fuel_variables():
    fuels = object.__new__(Fuels)
    ccs_fuel = {
        "name": "ethanol production, via fermentation, with carbon capture and storage",
        "reference product": "ethanol",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [
            production_exchange(
                "ethanol production, via fermentation, with carbon capture and storage",
                "ethanol",
                "kilogram",
            ),
            co2_uptake(0.8),
            co2_storage_input(0.6),
        ],
    }
    unmapped_ccs_fuel = {
        "name": (
            "ethanol production, via fermentation, from switchgrass, with carbon "
            "capture and storage, system expansion"
        ),
        "reference product": "ethanol, from switchgrass",
        "location": "US",
        "unit": "kilogram",
        "exchanges": [
            production_exchange(
                "ethanol production, via fermentation, from switchgrass, with carbon "
                "capture and storage, system expansion",
                "ethanol, from switchgrass",
                "kilogram",
            ),
            co2_storage_input(
                4.0,
                name=(
                    "carbon dioxide storage at wood burning power plant 20 MW "
                    "post, pipeline 200km, storage 1000m"
                ),
            ),
        ],
    }
    non_ccs_fuel = {
        "name": "ethanol production, via fermentation",
        "reference product": "ethanol",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [
            production_exchange(
                "ethanol production, via fermentation", "ethanol", "kilogram"
            ),
            co2_uptake(0.8),
            co2_storage_input(0.6),
        ],
    }
    non_fuel_ccs = {
        "name": "cement production, with carbon capture and storage",
        "reference product": "cement",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [
            production_exchange(
                "cement production, with carbon capture and storage",
                "cement",
                "kilogram",
            ),
            co2_storage_input(3.0),
        ],
    }
    fuels.fuel_map = {
        "bioethanol, from wood, with CCS": [ccs_fuel],
        "bioethanol, from wood": [non_ccs_fuel],
    }
    fuels.database = [ccs_fuel, unmapped_ccs_fuel, non_ccs_fuel, non_fuel_ccs]

    removed = fuels.remove_cdr_credit_from_ccs_fuel_activities()

    assert removed == pytest.approx(5.4)
    assert ccs_fuel["exchanges"][1]["amount"] == 0
    assert ccs_fuel["exchanges"][2]["amount"] == 0
    assert unmapped_ccs_fuel["exchanges"][1]["amount"] == 0
    assert non_ccs_fuel["exchanges"][1]["amount"] == pytest.approx(0.8)
    assert non_ccs_fuel["exchanges"][2]["amount"] == pytest.approx(0.6)
    assert non_fuel_ccs["exchanges"][1]["amount"] == pytest.approx(3.0)
