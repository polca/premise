"""Fuel mapping, crop properties and electrolysis energy requirements."""

import numpy as np
import yaml

from .config import CROPS_PROPERTIES


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


def adjust_electrolysis_electricity_requirement(
    year: int, projected_efficiency: dict
) -> [float, float, float]:
    """

    Calculate the adjusted electricity requirement for hydrogen electrolysis
    based on the given year.

    :param year: the year for which to calculate the adjusted electricity requirement
    :param projected_efficiency: the projected efficiency of the electrolysis process
    :return: the adjusted mean, min and max electricity requirement in kWh/kg H2

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
