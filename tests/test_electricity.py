# content of test_electricity.py
import math
import os
from pathlib import Path

import numpy as np
import pytest

from premise.data_collection import IAMDataCollection
from premise.electricity import Electricity
from premise.filesystem_constants import DATA_DIR

LHV_FUELS = DATA_DIR / "fuels_lower_heating_value.txt"


def get_db():
    dummy_db = [
        {
            "name": "fake activity",
            "reference product": "fake product",
            "location": "IAI Area, Africa",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "fake activity",
                    "product": "fake product",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "input": ("dummy_db", "6543541"),
                },
                {
                    "name": "1,4-Butanediol",
                    "categories": ("air", "urban air close to ground"),
                    "amount": 1,
                    "type": "biosphere",
                    "unit": "kilogram",
                    "input": ("dummy_bio", "123"),
                },
            ],
        }
    ]
    version = 3.5
    return dummy_db, version


def test_correct_hydropower_water_emissions_rescales_uncertainty():
    original_amount = 0.029221678
    water_to_air = {
        "name": "Water",
        "categories": ("air",),
        "amount": original_amount,
        "type": "biosphere",
        "unit": "cubic meter",
        "uncertainty type": 2,
        "loc": math.log(original_amount),
        "scale": 0.4,
    }
    unmatched_exchange = {
        "name": "Water",
        "categories": ("soil",),
        "amount": 2.0,
        "type": "biosphere",
        "unit": "cubic meter",
        "uncertainty type": 2,
        "loc": math.log(2.0),
        "scale": 0.2,
    }
    non_swiss_water = {
        "name": "Water",
        "categories": ("air",),
        "amount": 0.5,
        "type": "biosphere",
        "unit": "cubic meter",
        "uncertainty type": 2,
        "loc": math.log(0.5),
        "scale": 0.3,
    }
    electricity = Electricity.__new__(Electricity)
    electricity.database = [
        {
            "name": "electricity production, hydro, reservoir, alpine region",
            "location": "CH",
            "unit": "kilowatt hour",
            "exchanges": [water_to_air, unmatched_exchange],
        },
        {
            "name": "electricity production, hydro, reservoir, alpine region",
            "location": "FR",
            "unit": "kilowatt hour",
            "exchanges": [non_swiss_water],
        },
    ]

    electricity.correct_hydropower_water_emissions()

    assert water_to_air["amount"] == pytest.approx(0.00175)
    assert math.exp(water_to_air["loc"]) == pytest.approx(water_to_air["amount"])
    assert water_to_air["scale"] == pytest.approx(0.4)
    assert water_to_air["uncertainty type"] == 2
    assert unmatched_exchange["amount"] == pytest.approx(2.0)
    assert unmatched_exchange["loc"] == pytest.approx(math.log(2.0))
    assert non_swiss_water["amount"] == pytest.approx(0.5)
    assert non_swiss_water["loc"] == pytest.approx(math.log(0.5))


def test_correct_hydropower_water_emissions_handles_zero_amount():
    water_to_reservoir = {
        "name": "Water",
        "categories": ("water",),
        "amount": 0.0,
        "type": "biosphere",
        "unit": "cubic meter",
        "uncertainty type": 2,
        "loc": 0.0,
        "scale": 0.4,
        "minimum": 0.0,
        "maximum": 1.0,
        "negative": False,
    }
    electricity = Electricity.__new__(Electricity)
    electricity.database = [
        {
            "name": "electricity production, hydro, reservoir, alpine region",
            "location": "CH",
            "unit": "kilowatt hour",
            "exchanges": [water_to_reservoir],
        }
    ]

    electricity.correct_hydropower_water_emissions()

    assert water_to_reservoir["amount"] == pytest.approx(0.80825)
    assert water_to_reservoir["loc"] == pytest.approx(0.80825)
    assert water_to_reservoir["uncertainty type"] == 0
    for field in ("scale", "shape", "minimum", "maximum", "negative"):
        assert field not in water_to_reservoir


# This won't work with PRs because PRs from outside contributors don't have
# access to secrets (for good reason).
if "IAM_FILES_KEY" in os.environ:
    key = os.environ["IAM_FILES_KEY"]
else:
    # This won't work on most computers :)
    if Path("/Users/romain/Dropbox/Notebooks/key.txt").is_file():
        with open("/Users/romain/Dropbox/Notebooks/key.txt") as f:
            lines = f.readlines()
        key = lines[0]
    else:
        key = None


if key:
    rdc = IAMDataCollection(
        model="remind",
        pathway="SSP2-NPi",
        year=2012,
        filepath_iam_files=DATA_DIR / "iam_output_files",
        key=str.encode(key),
    )
    db, _ = get_db()
    el = Electricity(
        database=db,
        iam_data=rdc,
        model="remind",
        pathway="SSP2-NPi",
        year=2012,
        version="3.5",
        system_model="cutoff",
    )


@pytest.mark.skipif(not key, reason="No access to decryption key")
def test_losses():
    assert len(el.network_loss) == 13


@pytest.mark.skipif(not key, reason="No access to decryption key")
def test_powerplant_map():
    s = el.powerplant_map["Biomass IGCC CCS"]
    assert isinstance(s, list)
