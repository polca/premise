import numpy as np
import pytest

xr = pytest.importorskip("xarray")

from premise.emissions import Emissions


def _gains_data(values):
    return xr.DataArray(
        values,
        coords={
            "region": ["R1", "R2"],
            "sector": ["SEC"],
            "year": [2020, 2030],
            "pollutant": ["NOx"],
        },
        dims=("region", "sector", "year", "pollutant"),
    )


def test_prepare_data_adds_world_emissions_weighted_scaling():
    values = np.array(
        [
            [[[100.0], [50.0]]],
            [[[300.0], [60.0]]],
        ]
    )
    emissions = object.__new__(Emissions)
    emissions.year = 2030

    result = Emissions.prepare_data(emissions, _gains_data(values))

    assert result.sel(
        region="R1", sector="SEC", pollutant="NOx"
    ).item() == pytest.approx(0.5)
    assert result.sel(
        region="R2", sector="SEC", pollutant="NOx"
    ).item() == pytest.approx(0.2)
    assert result.sel(
        region="World", sector="SEC", pollutant="NOx"
    ).item() == pytest.approx(110 / 400)


def test_update_emissions_updates_world_dataset_with_world_scaling():
    values = np.array(
        [
            [[[100.0], [50.0]]],
            [[[300.0], [60.0]]],
        ]
    )
    dataset = {
        "name": "hot pollutant activity",
        "location": "World",
        "exchanges": [
            {
                "name": "Nitrogen oxides",
                "amount": 10.0,
                "type": "biosphere",
            }
        ],
    }
    emissions = object.__new__(Emissions)
    emissions.year = 2030
    emissions.database = [dataset]
    emissions.ecoinvent_to_iam_loc = {"World": "World"}
    emissions.gains_IAM = Emissions.prepare_data(emissions, _gains_data(values))
    emissions.rev_gains_map = {"hot pollutant activity": "SEC"}
    emissions.ei_pollutants = {"Nitrogen oxides": "NOx"}
    emissions.write_log = lambda dataset, status="created": None

    Emissions.update_emissions_in_database(emissions)

    assert dataset["exchanges"][0]["amount"] == pytest.approx(10.0 * 110 / 400)
    assert dataset["log parameters"]["NOx"] == pytest.approx(110 / 400)
