"""Shared inventories must retain each external variable and its demand."""

from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import yaml

from premise.external_data_validation import check_inventories
from premise.pathways import PathwaysDataPackage


def scenario_data(variables, unit="kilogram"):
    values = np.arange(1, len(variables) * 2 + 1, dtype=float).reshape(
        1, len(variables), 2
    )
    production = xr.DataArray(
        values,
        coords={"region": ["Region-A"], "variables": variables, "year": [2030, 2040]},
        dims=["region", "variables", "year"],
        name="value",
        attrs={"unit": {variable: unit for variable in variables}},
    )
    return {"production volume": production}


def shared_configuration(variables, name, product, unit):
    return {
        "production pathways": {
            variable: {
                "ecoinvent alias": {
                    "name": name,
                    "reference product": product,
                    "exists in original database": False,
                    "new dataset": True,
                }
            }
            for variable in variables
        },
        "markets": [{"name": name, "reference product": product, "unit": unit}],
    }


@pytest.fixture(autouse=True)
def no_geography_dependencies(monkeypatch):
    # No geographic fallback is needed for the inventories in these fixtures.
    monkeypatch.setattr("premise.external_data_validation.Geomap", lambda model: None)


@pytest.mark.parametrize(
    "variables,name,product,unit,year",
    [
        (
            ["factory demand", "household demand", "retail demand"],
            "shared material market",
            "material",
            "kilogram",
            2030,
        ),
        (
            ["user-z", "user-x", "user-y"],
            "shared water market",
            "water",
            "cubic meter",
            2040,
        ),
    ],
)
def test_shared_new_inventory_retains_all_bindings(
    variables, name, product, unit, year
):
    config = shared_configuration(variables, name, product, unit)
    original = deepcopy(config)
    imported, database, checked, mapping = check_inventories(
        config, [], scenario_data(variables, unit), [], year, "test-model"
    )
    assert set(mapping) == set(variables)
    expected = {"name": name, "reference product": product, "unit": unit}
    assert all(mapping[variable] == [expected] for variable in variables)
    assert len({id(mapping[v][0]) for v in variables}) == len(variables)
    assert imported == database == []
    assert checked == original  # no duplicate markets or renamed shared supplier


def test_multiple_shared_markets_keep_distinct_suppliers():
    first = shared_configuration(["alpha", "beta"], "market A", "product A", "kilogram")
    second = shared_configuration(
        ["gamma", "delta"], "market B", "product B", "cubic meter"
    )
    first["production pathways"].update(second["production pathways"])
    first["markets"].extend(second["markets"])
    _, _, _, mapping = check_inventories(
        first,
        [],
        scenario_data(["alpha", "beta", "gamma", "delta"]),
        [],
        2030,
        "test-model",
    )
    assert set(mapping) == {"alpha", "beta", "gamma", "delta"}
    assert mapping["alpha"] == mapping["beta"]
    assert mapping["gamma"] == mapping["delta"]
    assert mapping["alpha"][0]["unit"] == "kilogram"
    assert mapping["gamma"][0]["unit"] == "cubic meter"
    assert mapping["alpha"] != mapping["gamma"]


def test_shared_new_inventory_exports_separate_scenario_demands(tmp_path, monkeypatch):
    variables = ["consumer-a", "consumer-b", "consumer-c"]
    external = scenario_data(variables)
    original_demand = external["production volume"].copy(deep=True)
    config = shared_configuration(variables, "shared market", "product", "kilogram")
    _, _, _, mapping = check_inventories(config, [], external, [], 2030, "test-model")
    xr.testing.assert_identical(external["production volume"], original_demand)
    background = scenario_data(["background supply"])["production volume"]
    exporter = object.__new__(PathwaysDataPackage)
    exporter.datapackage = SimpleNamespace(
        scenarios=[
            {
                "model": "test-model",
                "pathway": "test-path",
                "year": year,
                "mapping": {"external_0": mapping},
                "iam data": SimpleNamespace(production_volumes=background),
                "external data": {0: external},
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
    exported = pd.read_csv(tmp_path / "pathways_temp/scenario_data/scenario_data.csv")
    assert set(written) == {f"EXT - 0 - {variable}" for variable in variables}
    assert all(len(value["dataset"]) == 1 for value in written.values())
    demand = exported.loc[exported.variables.isin(written)].set_index(
        ["variables", "year"]
    )
    assert len(demand) == 6
    for variable in variables:
        for year in [2030, 2040]:
            row = demand.loc[(f"EXT - 0 - {variable}", year)]
            assert (
                row["value"]
                == original_demand.sel(variables=variable, year=year).item()
            )
            assert row["unit"] == "kilogram"
            assert row["pathway"] == "test-path - arbitrary-scenario"


def test_existing_inventory_still_gets_separate_technology_copies():
    config = shared_configuration(
        ["use-a", "use-b"], "original supplier", "product", "kilogram"
    )
    for pathway in config["production pathways"].values():
        pathway["ecoinvent alias"].update(
            {"exists in original database": True, "new dataset": False}
        )
    inventory = [
        {
            "name": "original supplier",
            "reference product": "product",
            "unit": "kilogram",
            "location": "Region-A",
            "code": "original",
            "exchanges": [
                {"type": "production", "name": "original supplier", "amount": 1}
            ],
        }
    ]
    _, checked, _, mapping = check_inventories(
        config, [], scenario_data(["use-a", "use-b"]), inventory, 2030, "test-model"
    )
    assert set(mapping) == {"use-a", "use-b"}
    assert mapping["use-a"][0]["name"] == "original supplier"
    assert mapping["use-b"][0]["name"] == "original supplier_use-b"
    assert len(checked) == 2
