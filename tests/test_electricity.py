# content of test_electricity.py
import math
import os
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from premise.data_collection import IAMDataCollection
import premise.electricity as electricity_module
from premise.electricity import Electricity, _CoalPowerPlantData
from premise.filesystem_constants import DATA_DIR

LHV_FUELS = DATA_DIR / "fuels_lower_heating_value.txt"


def test_supplier_pruning_never_drops_an_entire_technology():
    first = {"name": "first"}
    second = {"name": "second"}

    retained = Electricity.prune_and_normalize_suppliers(
        [(first, 0.0004), (second, 0.0006)]
    )

    assert retained == [(second, 1.0)]


def test_supplier_pruning_renormalizes_retained_providers():
    retained = Electricity.prune_and_normalize_suppliers(
        [({"name": "first"}, 0.2), ({"name": "second"}, 0.8)]
    )

    assert sum(share for _, share in retained) == pytest.approx(1.0)


def test_consequential_msw_map_uses_electricity_conversion_provider():
    valid = {
        "name": (
            "electricity, from municipal waste incineration to generic market "
            "for electricity, medium voltage"
        ),
        "reference product": "electricity, medium voltage",
        "unit": "kilowatt hour",
    }
    treatment = {
        "name": "treatment of municipal solid waste, municipal incineration",
        "reference product": "municipal solid waste",
        "unit": "kilogram",
    }
    electricity = object.__new__(Electricity)
    electricity.system_model = "consequential"
    electricity.database = [treatment, valid]
    electricity.powerplant_map = {"Biomass MSW": [treatment]}

    electricity.complete_consequential_powerplant_map()

    assert electricity.powerplant_map["Biomass MSW"] == [treatment, valid]


def test_coal_power_plant_data_caches_selections_and_emission_factors():
    data = xr.DataArray(
        np.array([[[[100.0, 2.0, 0.4]]]]),
        dims=("country", "fuel", "CHP", "variable"),
        coords={
            "country": ["CH"],
            "fuel": ["Anthracite coal"],
            "CHP": [False],
            "variable": ["generation", "SO2", "efficiency"],
        },
    )

    class CountingData:
        def __init__(self, array):
            self.array = array
            self.country = array.country
            self.selections = 0

        def sel(self, **kwargs):
            self.selections += 1
            return self.array.sel(**kwargs)

    counting_data = CountingData(data)
    coal_data = _CoalPowerPlantData(counting_data)

    assert coal_data.contains_country("CH")
    assert not coal_data.contains_country("FR")
    assert coal_data.value("CH", "Anthracite coal", False, "efficiency") == 0.4
    assert coal_data.value("CH", "Anthracite coal", False, "efficiency") == 0.4
    assert coal_data.emission_factor("CH", "Anthracite coal", False, "SO2") == 2e-5
    assert coal_data.emission_factor("CH", "Anthracite coal", False, "SO2") == 2e-5
    assert counting_data.selections == 3


def test_electricity_efficiency_reuses_technology_region_change(monkeypatch):
    efficiencies = xr.DataArray(
        [[[1.1]]],
        dims=("region", "variables", "year"),
        coords={"region": ["World"], "variables": ["Coal PC"], "year": [2050]},
    )
    datasets = [
        {
            "name": f"coal power plant {index}",
            "reference product": "electricity",
            "location": "GLO",
            "unit": "kilowatt hour",
            "exchanges": [],
        }
        for index in range(2)
    ]
    electricity = Electricity.__new__(Electricity)
    electricity.iam_data = SimpleNamespace(
        electricity_technology_efficiencies=efficiencies,
        electricity_mix=efficiencies,
    )
    electricity.powerplant_map = {"Coal PC": datasets}
    electricity.powerplant_fuels_map = {"Coal PC": []}
    electricity.get_iam_mapping = lambda **kwargs: {"Coal PC": {"fuel filters": []}}
    electricity.is_in_index = lambda dataset: True
    electricity.geo = SimpleNamespace(
        ecoinvent_to_iam_location=lambda location: "World"
    )
    electricity.use_absolute_efficiency = False
    electricity.fuels_specs = {}
    electricity.fuel_map_reverse = {}
    electricity.powerplant_min_efficiency = {}
    electricity.powerplant_max_efficiency = {}
    electricity.update_ecoinvent_efficiency_parameter = lambda *args: None
    electricity.write_log = lambda **kwargs: None
    calls = []

    def efficiency_change(**kwargs):
        calls.append((kwargs["variable"], kwargs["location"]))
        return 1.1

    electricity.find_iam_efficiency_change = efficiency_change
    monkeypatch.setattr(
        electricity_module, "find_fuel_efficiency", lambda **kwargs: 0.5
    )
    monkeypatch.setattr(electricity_module, "rescale_exchanges", lambda *args: None)

    electricity.update_electricity_efficiency()

    assert calls == [("Coal PC", "World")]
    assert all(
        dataset["log parameters"]["new efficiency"] == pytest.approx(0.55)
        for dataset in datasets
    )


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
