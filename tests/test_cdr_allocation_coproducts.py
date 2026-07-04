import pytest

from premise.electricity import (
    CHP_CCS_CAPTURE_RATE,
    CHP_CCS_POWER_PLANT_SPECS,
    Electricity,
)
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


def fossil_co2(amount=1.0):
    return {
        "name": "Carbon dioxide, fossil",
        "categories": ("air", "unspecified"),
        "amount": amount,
        "loc": amount,
        "type": "biosphere",
        "unit": "kilogram",
    }


def fuel_input(amount=1.0):
    return {
        "name": "market for fuel",
        "product": "fuel",
        "amount": amount,
        "loc": amount,
        "type": "technosphere",
        "unit": "megajoule",
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


def test_make_gas_chp_ccs_power_plant_reduces_direct_fossil_co2():
    spec = CHP_CCS_POWER_PLANT_SPECS["Gas CHP CCS"]
    source = {
        "name": (
            "heat and power co-generation, natural gas, conventional power plant, "
            "100MW electrical"
        ),
        "reference product": "electricity, high voltage",
        "location": "DE",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                (
                    "heat and power co-generation, natural gas, conventional "
                    "power plant, 100MW electrical"
                ),
                "electricity, high voltage",
                "kilowatt hour",
            ),
            fuel_input(2.0),
            fossil_co2(0.4),
        ],
    }

    dataset = Electricity._make_chp_ccs_power_plant_dataset(
        source_dataset=source,
        region="WEU",
        technology="Gas CHP CCS",
        spec=spec,
    )

    fuel = next(exc for exc in dataset["exchanges"] if exc.get("product") == "fuel")
    fossil = next(
        exc
        for exc in dataset["exchanges"]
        if exc["type"] == "biosphere" and exc["name"] == "Carbon dioxide, fossil"
    )
    captured = next(
        exc
        for exc in dataset["exchanges"]
        if exc["type"] == "technosphere" and exc["name"] == spec["capture_name"]
    )

    penalized_co2 = 0.4 * spec["energy_penalty"]
    assert dataset["name"] == spec["name"]
    assert dataset["location"] == "WEU"
    assert fuel["amount"] == pytest.approx(2.0 * spec["energy_penalty"])
    assert fossil["amount"] == pytest.approx(
        penalized_co2 * (1 - CHP_CCS_CAPTURE_RATE)
    )
    assert captured["amount"] == pytest.approx(penalized_co2 * CHP_CCS_CAPTURE_RATE)
    assert not any(
        exc["type"] == "biosphere"
        and exc["name"] == "Carbon dioxide, fossil"
        and exc["amount"] < 0
        for exc in dataset["exchanges"]
    )


def test_make_biomass_chp_ccs_power_plant_reduces_direct_non_fossil_co2():
    spec = CHP_CCS_POWER_PLANT_SPECS["Biomass CHP CCS"]
    source = {
        "name": "heat and power co-generation, wood chips, 6667 kW",
        "reference product": "electricity, high voltage",
        "location": "CH",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                "heat and power co-generation, wood chips, 6667 kW",
                "electricity, high voltage",
                "kilowatt hour",
            ),
            fuel_input(1.5),
            non_fossil_co2(0.5),
        ],
    }

    dataset = Electricity._make_chp_ccs_power_plant_dataset(
        source_dataset=source,
        region="WEU",
        technology="Biomass CHP CCS",
        spec=spec,
    )

    non_fossil = next(
        exc
        for exc in dataset["exchanges"]
        if exc["type"] == "biosphere"
        and exc["name"] == "Carbon dioxide, non-fossil"
    )
    captured = next(
        exc
        for exc in dataset["exchanges"]
        if exc["type"] == "technosphere" and exc["name"] == spec["capture_name"]
    )

    penalized_co2 = 0.5 * spec["energy_penalty"]
    assert non_fossil["amount"] == pytest.approx(
        penalized_co2 * (1 - CHP_CCS_CAPTURE_RATE)
    )
    assert captured["amount"] == pytest.approx(penalized_co2 * CHP_CCS_CAPTURE_RATE)
    assert not any(
        exc["type"] == "biosphere"
        and exc["name"] == "Carbon dioxide, non-fossil"
        and exc["amount"] < 0
        for exc in dataset["exchanges"]
    )


def test_create_chp_ccs_power_plant_datasets_replaces_imported_template():
    spec = CHP_CCS_POWER_PLANT_SPECS["Gas CHP CCS"]
    source = {
        "name": (
            "heat and power co-generation, natural gas, conventional power plant, "
            "100MW electrical"
        ),
        "reference product": "electricity, high voltage",
        "location": "DE",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                (
                    "heat and power co-generation, natural gas, conventional "
                    "power plant, 100MW electrical"
                ),
                "electricity, high voltage",
                "kilowatt hour",
            ),
            fuel_input(2.0),
            fossil_co2(0.4),
        ],
    }
    imported_template = {
        "name": spec["name"],
        "reference product": "electricity, high voltage",
        "location": "DE",
        "unit": "kilowatt hour",
        "exchanges": [
            production_exchange(
                spec["name"], "electricity, high voltage", "kilowatt hour"
            ),
            fossil_co2(-0.36),
        ],
    }

    class FakeMapping:
        @staticmethod
        def generate_powerplant_fuels_map():
            return {"Gas CHP CCS": [source]}

    electricity = object.__new__(Electricity)
    electricity.database = [source, imported_template]
    electricity.regions = ["WEU"]
    electricity.ecoinvent_to_iam_loc = {"DE": "WEU"}
    electricity.mapping = FakeMapping()
    electricity.remove_from_index = lambda dataset: None
    electricity.relink_technosphere_exchanges = lambda dataset: dataset
    electricity.add_geo_definition_metadata = lambda dataset: dataset
    electricity.add_to_index = lambda dataset: None
    electricity.write_log = lambda dataset, status="created": None

    electricity.create_chp_ccs_power_plant_datasets()

    generated = [
        dataset for dataset in electricity.database if dataset["name"] == spec["name"]
    ]
    assert len(generated) == 1
    assert generated[0] is not imported_template
    assert generated[0]["location"] == "WEU"
    assert not any(
        exc["type"] == "biosphere"
        and exc["name"] == "Carbon dioxide, fossil"
        and exc["amount"] < 0
        for exc in generated[0]["exchanges"]
    )


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
