"""Scenario mappings must survive regionalization without changing inventories."""

from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import yaml

from premise.external_data_validation import check_inventories
from premise.pathways import PathwaysDataPackage


def example(existing=True):
    production = xr.DataArray(
        [[[2.0, 4.0]], [[np.nan, np.nan]], [[9.0, 10.0]]],
        dims=["region", "variables", "year"],
        coords={
            "region": ["Region-A", "Region-B", "Region-C"],
            "variables": ["arbitrary-demand"],
            "year": [2030, 2040],
        },
        name="value",
        attrs={"unit": {"arbitrary-demand": "kilogram"}},
    )
    efficiency = (
        production.rename({"variables": "technology"})
        .assign_coords(technology=["arbitrary-efficiency"])
        .rename({"technology": "variables"})
    )
    pathway = {
        "ecoinvent alias": {
            "name": "Example process",
            "reference product": "material",
            "exists in original database": existing,
            "regionalize": False,
        },
        "except regions": ["Region-B"],
        "efficiency": [
            {
                "variable": "arbitrary-efficiency",
                "reference year": 2030,
                "includes": {"technosphere": ["fuel"]},
            }
        ],
        "replaces": [{"name": "alternative supplier"}],
        "replacement ratio": 2.0,
    }
    config = {
        "production pathways": {"arbitrary-demand": pathway},
        "regionalize": {
            "datasets": [
                {
                    "name": "Example process",
                    "reference product": "material",
                    "exists in original database": existing,
                }
            ],
            "except regions": ["Region-C"],
        },
    }
    activity = {
        "name": "Example process",
        "reference product": "material",
        "unit": "kilogram",
        "location": "GLO",
        "code": "source",
        "production volume": 37.0,
        "exchanges": [
            {
                "name": "Example process",
                "product": "material",
                "unit": "kilogram",
                "location": "GLO",
                "type": "production",
                "amount": 1.0,
            },
            {
                "name": "fuel",
                "product": "fuel",
                "unit": "megajoule",
                "location": "GLO",
                "type": "technosphere",
                "amount": 3.0,
            },
        ],
    }
    return config, {"production volume": production, "efficiency": efficiency}, activity


@pytest.fixture(autouse=True)
def no_geography_dependencies(monkeypatch):
    monkeypatch.setattr("premise.external_data_validation.Geomap", lambda model: None)


@pytest.mark.parametrize("existing", [True, False])
def test_binding_preserved_without_merging_transformation_metadata(existing):
    config, data, activity = example(existing)
    source_data = deepcopy(data)
    imported = [] if existing else [deepcopy(activity)]
    database = [deepcopy(activity)] if existing else []
    checked_imported, checked_database, _, mapping = check_inventories(
        config, imported, data, database, 2030, "arbitrary-model"
    )
    assert mapping == {
        "arbitrary-demand": [
            {
                "name": "Example process",
                "reference product": "material",
                "unit": "kilogram",
            }
        ]
    }
    adjusted = (checked_database if existing else checked_imported)[0]
    expected = deepcopy(activity)
    expected.update(regions=["Region-A", "Region-B"], regionalize=True)
    if not existing:
        expected["custom scenario dataset"] = True
    assert adjusted == expected
    # No pathway-specific exclusions, efficiency, replacements or production
    # volume metadata leak into the existing direct-regionalization behavior.
    assert "production volume variable" not in adjusted
    assert "adjust efficiency" not in adjusted
    assert "replaces" not in adjusted
    xr.testing.assert_identical(
        data["production volume"], source_data["production volume"]
    )
    xr.testing.assert_identical(data["efficiency"], source_data["efficiency"])


@pytest.mark.parametrize("existing", [True, False])
def test_regionalization_without_pathway_does_not_invent_mapping(existing):
    config, data, activity = example(existing)
    config["production pathways"] = {}
    imported = [] if existing else [deepcopy(activity)]
    database = [deepcopy(activity)] if existing else []
    checked_imported, checked_database, _, mapping = check_inventories(
        config, imported, data, database, 2030, "arbitrary-model"
    )
    assert mapping == {}
    adjusted = (checked_database if existing else checked_imported)[0]
    assert adjusted["regions"] == ["Region-A", "Region-B"]
    assert adjusted["regionalize"] is True
    assert adjusted["exchanges"] == activity["exchanges"]


def test_regionalized_binding_reaches_export_with_original_demands(
    tmp_path, monkeypatch
):
    config, data, activity = example()
    _, _, _, mapping = check_inventories(
        config, [], data, [activity], 2030, "arbitrary-model"
    )
    exporter = object.__new__(PathwaysDataPackage)
    exporter.datapackage = SimpleNamespace(
        scenarios=[
            {
                "model": "arbitrary-model",
                "pathway": "arbitrary-path",
                "year": year,
                "mapping": {"external_0": mapping},
                "iam data": SimpleNamespace(
                    production_volumes=data["production volume"].assign_coords(
                        variables=["background"]
                    )
                ),
                "external data": {0: data},
                "external scenarios": [{"scenario": "arbitrary-scenario"}],
            }
            for year in [2030, 2040]
        ]
    )
    exporter.variables_name_change = {}
    monkeypatch.chdir(tmp_path)
    exporter._add_variables_mapping()
    exporter._add_scenario_data()
    written = yaml.safe_load(
        (tmp_path / "pathways_temp/mapping/mapping.yaml").read_text()
    )
    key = "EXT - 0 - arbitrary-demand"
    assert written == {key: {"dataset": mapping["arbitrary-demand"]}}
    exported = pd.read_csv(tmp_path / "pathways_temp/scenario_data/scenario_data.csv")
    selected = exported.loc[exported.variables.eq(key)].set_index(["region", "year"])
    assert selected.value.to_dict() == {
        ("Region-A", 2030): 2.0,
        ("Region-A", 2040): 4.0,
        ("Region-C", 2030): 9.0,
        ("Region-C", 2040): 10.0,
    }
    assert set(selected.unit) == {"kilogram"}
