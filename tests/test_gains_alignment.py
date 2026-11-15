"""GAINS pollutant tables retain differing sector/region coverage across xarray defaults."""

import json
from contextlib import nullcontext

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from premise.iam import data_collection as module


@pytest.fixture
def gains_tables(tmp_path, monkeypatch):
    directory = tmp_path / "GAINS_emission_factors" / "iam_data" / "fixture"
    directory.mkdir(parents=True)
    years = list(range(1990, 2055, 5))
    observations = {
        "NOx": [
            ("A", "industry", list(range(10, 23))),
            ("B", "industry", list(range(13))),
        ],
        "SO2": [
            ("A", "industry", list(range(30, 43))),
            ("A", "transport", list(range(50, 63))),
            ("C", "transport", list(range(70, 83))),
        ],
    }
    for pollutant, rows in observations.items():
        records = [
            {
                "Region": region,
                "EMF30 Sector": sector,
                **dict(zip(map(str, years), values, strict=True)),
            }
            for region, sector, values in rows
        ]
        pd.DataFrame(records).to_csv(directory / f"{pollutant}.csv", index=False)
    mapping = tmp_path / "regions.yaml"
    mapping.write_text(json.dumps({r: {"image": f"IAM_{r}"} for r in "ABC"}))
    monkeypatch.setattr(module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(module, "GAINS_GEO_MAP", mapping)
    module.get_gains_IAM_data.cache_clear()
    yield years, observations
    module.get_gains_IAM_data.cache_clear()


@pytest.mark.parametrize("new_defaults", [False, True])
@pytest.mark.filterwarnings("error::FutureWarning")
def test_gains_outer_alignment_preserves_observations_and_missing_coverage(
    gains_tables, new_defaults
):
    option = "use_new_combine_kwarg_defaults"
    supported = option in xr.get_options()
    if new_defaults and not supported:
        pytest.skip("Installed xarray does not expose the future-defaults switch.")
    context = xr.set_options(**{option: new_defaults}) if supported else nullcontext()
    with context:
        result = module.get_gains_IAM_data("image", "fixture")
    years, observations = gains_tables
    assert set(result.region.values) == {"IAM_A", "IAM_B", "IAM_C"}
    assert set(result.sector.values) == {"industry", "transport"}
    assert set(result.pollutant.values) == {"NOx", "SO2"}
    np.testing.assert_array_equal(result.year.values, years)
    observed = set()
    for pollutant, rows in observations.items():
        for region, sector, values in rows:
            key = (pollutant, f"IAM_{region}", sector)
            observed.add(key)
            np.testing.assert_array_equal(
                result.sel(
                    pollutant=pollutant, region=f"IAM_{region}", sector=sector
                ).values,
                values,
            )
    for pollutant in result.pollutant.values:
        for region in result.region.values:
            for sector in result.sector.values:
                if (pollutant, region, sector) not in observed:
                    assert np.isnan(
                        result.sel(
                            pollutant=pollutant, region=region, sector=sector
                        ).values
                    ).all()
