# content of test_electricity.py
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
