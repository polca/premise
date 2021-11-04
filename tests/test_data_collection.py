# content of test_activity_maps.py
import os
from pathlib import Path

import pytest

from premise.data_collection import *

fp = Path(DATA_DIR / "iam_output_files")

dc = IAMDataCollection(
    model="remind",
    pathway="SSP2-Base",
    year=2035,
    key=os.environ["IAM_FILES_KEY"],
    filepath_iam_files=fp,
)


def test_eff_array():
    """
    Check that relative efficiency changes are all between 0.5 and 2.
    :return:
    """
    assert (dc.efficiency.all() >= 0.5) & (dc.efficiency.all() <= 2)
    assert "steel - primary" in dc.efficiency.coords["variables"]


def test_ccs_rate():
    """
    Check that carbon capture rates are all between 0 and 1.
    :return:
    """
    assert (dc.carbon_capture_rate.all() >= 0) & (dc.carbon_capture_rate.all() <= 1)


def test_electricity_markets():
    """
    Check that the sum of electricity market compositions equal 1.
    :return:
    """
    nb_year = len(dc.electricity_markets.coords["year"])
    assert (
        dc.electricity_markets.sel(region="EUR").sum(dim=["variables", "year"])
        == nb_year
    )


def test_iam_variable_names():
    d = dc._IAMDataCollection__get_iam_variable_labels(IAM_ELEC_VARS, "name_aliases")
    assert d["Hydro"] == "SE|Electricity|Hydro"


def test_fuel_market():
    """
    Check that the sum of fuel shares equal to 1
    :return:
    """
    assert dc.fuel_markets.sel(region="EUR").sum(dim="variables") == 1


def test_out_of_scope_year():
    """
    Check that passing a year outside of the allowed scope returns an error
    :return:
    """
    with pytest.raises(KeyError) as wrapped_error:
        IAMDataCollection(
            model="remind",
            pathway="SSP2-Base",
            year=2000,
            key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
            filepath_iam_files=fp,
        )
    assert wrapped_error.type == KeyError
