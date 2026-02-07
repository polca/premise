from functools import lru_cache
import numpy as np
import yaml

from .config import CROPS_PROPERTIES


@lru_cache()
def get_compression_effort(
    inlet_pressure: int, outlet_pressure: int, flow_rate: int
) -> float:
    """
    Calculate the required electricity consumption from the compressor given
    an inlet and outlet pressure and a flow rate for hydrogen.

    :param inlet_pressure: the input pressure in bar
    :param outlet_pressure: the output pressure in bar
    :param flow_rate: the flow rate of hydrogen in kg/day
    :return: the required electricity consumption in kWh

    """
    # Constants
    COMPRESSIBILITY_FACTOR = 1.03198
    NUM_COMPRESSOR_STAGES = 2
    INLET_TEMPERATURE = 310.95  # K
    RATIO_SPECIFIC_HEATS = 1.4
    MOLECULAR_MASS_H2 = 2.15  # g/mol
    COMPRESSOR_EFFICIENCY = 0.75

    # Intermediate calculations
    mass_flow_rate = flow_rate / (24 * 3600)  # convert to kg/s
    specific_gas_constant = 8.314  # J/(mol*K)
    part_1 = (
        mass_flow_rate
        * (COMPRESSIBILITY_FACTOR * INLET_TEMPERATURE * specific_gas_constant)
        / (MOLECULAR_MASS_H2 * COMPRESSOR_EFFICIENCY)
        * (NUM_COMPRESSOR_STAGES * RATIO_SPECIFIC_HEATS / (RATIO_SPECIFIC_HEATS - 1))
    )
    part_2 = (
        (outlet_pressure / inlet_pressure)
        ** ((RATIO_SPECIFIC_HEATS - 1) / (NUM_COMPRESSOR_STAGES * RATIO_SPECIFIC_HEATS))
    ) - 1
    shaft_power = part_1 * part_2

    # Convert to kWh
    electricity_consumption = shaft_power * 24 / 1000

    return electricity_consumption


@lru_cache()
def get_pre_cooling_energy(
    ambient_temperature: float, capacity_utilization: float
) -> float:
    """
    Calculate the required electricity consumption to pre-cool the hydrogen
    before tank filling.

    :param ambient_temperature: the ambient temperature in degrees Celsius
    :param capacity_utilization: the capacity utilization of the pre-cooling system
    :return: the required electricity consumption in kWh

    """
    # Constants
    COEFFICIENT_1 = 0.3 / 1.6
    COEFFICIENT_2 = -0.018
    COEFFICIENT_3 = 25
    COEFFICIENT_4 = -21

    # Convert temperature to Kelvin
    temperature_K = ambient_temperature + 273.15

    # Calculate pre-cooling energy
    energy_pre_cooling = (
        COEFFICIENT_1 * np.exp(COEFFICIENT_2 * ambient_temperature)
        + (COEFFICIENT_3 * np.log(temperature_K) + COEFFICIENT_4) / capacity_utilization
    )

    return energy_pre_cooling


@lru_cache()
def add_boil_off_losses(vehicle, distance, loss_val):
    if vehicle == "truck":
        # average truck speed
        speed = 50
    else:
        # average ship speed
        speed = 36
    days = distance / speed / 24
    # boil-off losses, function of days in transit
    return np.power(1 + loss_val, days)


@lru_cache()
def add_pipeline_losses(distance, loss_val):
    # pipeline losses, function of distance
    return 1 + (loss_val * distance)


def add_other_losses(loss_val):
    return 1 + loss_val


def calculate_fuel_properties(amount, lhv, co2_factor, biogenic_share):
    """
    Calculate the fossil and non-fossil CO2 emissions and LHV for the given fuel
    properties and amount.
    """
    fossil_co2 = amount * lhv * co2_factor * (1 - biogenic_share)
    non_fossil_co2 = amount * lhv * co2_factor * biogenic_share
    weighted_lhv = amount * lhv
    return fossil_co2, non_fossil_co2, weighted_lhv


def update_dataset(dataset, supplier_key, amount):
    """
    Add a new exchange to the dataset for the given fuel and supplier, and update
    the LHV and CO2 fields.
    """
    exchange = {
        "uncertainty type": 0,
        "amount": amount,
        "product": supplier_key[2],
        "name": supplier_key[0],
        "unit": supplier_key[-1],
        "location": supplier_key[1],
        "type": "technosphere",
    }
    dataset["exchanges"].append(exchange)

    return dataset


def fetch_mapping(filepath: str) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


def get_crops_properties() -> dict:
    """
    Return a dictionary with crop names as keys and IAM labels as values
    relating to land use change CO2 per crop type
    :return: dict
    """
    with open(CROPS_PROPERTIES, "r", encoding="utf-8") as stream:
        crop_props = yaml.safe_load(stream)

    return crop_props


def update_co2_emissions(
    dataset: dict, amount_non_fossil_co2: float, biosphere_flows: dict
) -> dict:
    """Update fossil and non-fossil CO2 emissions of the dataset."""
    # Test for the presence of a fossil CO2 flow
    if not any(
        exc for exc in dataset["exchanges"] if exc["name"] == "Carbon dioxide, fossil"
    ):
        return dataset

    # subtract the biogenic CO2 amount to the initial fossil CO2 emission amount
    for exc in ws.biosphere(dataset, ws.equals("name", "Carbon dioxide, fossil")):
        dataset.setdefault("log parameters", {}).update(
            {"initial amount of fossil CO2": exc["amount"]}
        )
        exc["amount"] -= amount_non_fossil_co2
        if exc["amount"] < 0:
            exc["amount"] = 0
        dataset.setdefault("log parameters", {}).update(
            {"new amount of fossil CO2": exc["amount"]}
        )

    # add the non-fossil CO2 emission flow
    non_fossil_co2 = {
        "uncertainty type": 0,
        "amount": amount_non_fossil_co2,
        "type": "biosphere",
        "name": "Carbon dioxide, non-fossil",
        "unit": "kilogram",
        "categories": ("air",),
        "input": (
            "biosphere3",
            biosphere_flows[
                ("Carbon dioxide, non-fossil", "air", "unspecified", "kilogram")
            ],
        ),
    }

    dataset.setdefault("log parameters", {}).update(
        {"new amount of biogenic CO2": amount_non_fossil_co2}
    )

    dataset["exchanges"].append(non_fossil_co2)

    return dataset


def adjust_electrolysis_electricity_requirement(
    year: int, projected_efficiency: dict
) -> [float, float, float]:
    """

    Calculate the adjusted electricity requirement for hydrogen electrolysis
    based on the given year.

    :param year: the year for which to calculate the adjusted electricity requirement
    :param hydrogen_type: the type of hydrogen production
    :param projected_efficiency: the projected efficiency of the electrolysis process
    :return: the adjusted mena, min and max electricity requirement in kWh/kg H2

    """

    if year < 2020:
        mean = projected_efficiency[2020]["mean"]
        min = projected_efficiency[2020]["minimum"]
        max = projected_efficiency[2020]["maximum"]

    elif year > 2050:
        mean = projected_efficiency[2050]["mean"]
        min = projected_efficiency[2050]["minimum"]
        max = projected_efficiency[2050]["maximum"]

    else:
        mean = np.interp(
            year,
            [2020, 2050],
            [projected_efficiency[2020]["mean"], projected_efficiency[2050]["mean"]],
        )
        min = np.interp(
            year,
            [2020, 2050],
            [
                projected_efficiency[2020]["minimum"],
                projected_efficiency[2050]["minimum"],
            ],
        )
        max = np.interp(
            year,
            [2020, 2050],
            [
                projected_efficiency[2020]["maximum"],
                projected_efficiency[2050]["maximum"],
            ],
        )

    return mean, min, max


def add_hydrogen_regional_storage(
    dataset: dict, region: str, config: dict, supplier: dict
) -> dict:
    """

    Add a geological storage activity to the dataset for a given region.

    :param dataset: The dataset to modify.
    :param region: The region for which to add the activity.
    :param config: The configuration file for the analysis.
    :param subset: The subset of the database to search in.
    :return: The modified dataset.

    """

    dataset["exchanges"].append(
        {
            "uncertainty type": 0,
            "amount": 1,
            "type": "technosphere",
            "product": supplier[2],
            "name": supplier[0],
            "unit": supplier[3],
            "location": region,
            "comment": "Geological storage (salt cavern).",
        }
    )

    string = (
        " Geological storage is added. It includes 0.344 kWh for "
        "the injection and pumping of 1 kg of H2."
    )
    if "comment" in dataset:
        dataset["comment"] += string
    else:
        dataset["comment"] = string

    return dataset


def add_hydrogen_inhibitor(dataset: dict, region: str, supplier: dict) -> dict:
    """
    Adds hydrogen embrittlement inhibitor to the dataset for a given region.

    :param dataset: The dataset to modify.
    :param region: The region for which to add the activity.
    :return: The modified dataset.
    """

    dataset["exchanges"].append(
        {
            "uncertainty type": 0,
            "amount": 1,
            "type": "technosphere",
            "product": supplier[2],
            "name": supplier[0],
            "unit": supplier[3],
            "location": region,
            "comment": "Injection of an inhibiting gas (oxygen) "
            "to prevent embrittlement of metal. ",
        }
    )

    string = (
        "2.46 kWh/kg H2 is needed to purify the hydrogen from the inhibiting gas. "
        "The recovery rate for hydrogen after separation from the inhibitor gas is 93%. "
    )
    if "comment" in dataset:
        dataset["comment"] += string
    else:
        dataset["comment"] = string

    return dataset


def add_compression_electricity(
    state: str,
    vehicle: str,
    distance: float,
    dataset: dict,
    suppliers: list,
    year: int,
) -> dict:
    """
    Add the electricity needed for the compression of hydrogen.

    :param state: The state of the hydrogen (gaseous or liquid).
    :param vehicle: The vehicle used for transport (truck or pipeline).
    :param distance: The distance travelled by the vehicle.
    :param region: The region for which to add the activity.
    :param dataset: The dataset to modify.
    :return: The modified dataset.

    """

    # if gaseous
    # if transport by truck, compression from 25 bar to 500 bar for the transport
    # and from 500 bar to 900 bar for dispensing in 700 bar storage tanks

    # if transport by pipeline, initial compression from 25 bar to 100 bar
    # and 0.6 kWh re-compression every 250 km
    # and finally from 100 bar to 900 bar for dispensing in 700 bar storage tanks

    # if liquid
    # liquefaction electricity need
    # currently, 12 kWh/kg H2
    # midterm, 8 kWh/ kg H2
    # by 2050, 6 kWh/kg H2

    if state == "gaseous":
        if vehicle == "truck":
            electricity_comp = get_compression_effort(25, 500, 1000)
            electricity_comp += get_compression_effort(500, 900, 1000)
        else:
            electricity_comp = get_compression_effort(25, 100, 1000) + (
                0.6 * distance / 250
            )
            electricity_comp += get_compression_effort(100, 900, 1000)

        string = (
            f" {electricity_comp} kWh is added to compress from 25 bar 100 bar (if pipeline)"
            f"or 500 bar (if truck), and then to 900 bar to dispense in storage tanks at 700 bar. "
            " Additionally, if transported by pipeline, there is re-compression (0.6 kWh) every 250 km."
        )

    else:
        electricity_comp = np.clip(
            np.interp(
                year,
                [2020, 2035, 2050],
                [12, 8, 6],
            ),
            12,
            6,
        )

        string = f" {electricity_comp} kWh is added to liquefy the hydrogen. "

    new_exc = []
    for supplier, share in suppliers.items():
        new_exc.append(
            {
                "uncertainty type": 0,
                "amount": electricity_comp * share,
                "type": "technosphere",
                "product": supplier[2],
                "name": supplier[0],
                "unit": supplier[-1],
                "location": supplier[1],
            }
        )

    dataset["exchanges"].extend(new_exc)

    if "comment" in dataset:
        dataset["comment"] += string
    else:
        dataset["comment"] = string

    dataset.setdefault("log parameters", {}).update(
        {"electricity for hydrogen compression": electricity_comp}
    )

    return dataset


def add_h2_fuelling_station(region: str, supplier: dict) -> dict:
    """
    Add the hydrogen fuelling station.

    :param region: The region for which to add the activity.
    :return: The modified dataset.

    """

    return {
        "uncertainty type": 0,
        "amount": 1 / (600 * 365 * 40),  # 1 over lifetime: 40 years, 600 kg H2/day
        "type": "technosphere",
        "product": supplier[2],
        "name": supplier[0],
        "unit": supplier[3],
        "location": region,
    }
