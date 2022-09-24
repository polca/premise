"""
export.py contains all the functions to format, prepare and export databases.
"""

import csv
import datetime
import json
import os
import re

import numpy as np
import pandas as pd

from . import DATA_DIR, __version__
from .utils import c, s

FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "flows_biosphere_38.csv"


def generate_scenario_difference_file(database, db_name):
    scenario_cols = [
        t for t in database.columns if t[1] == c.amount and t[0] != s.ecoinvent
    ]

    unchanged_data_rows = database[scenario_cols].isnull().all(1)
    scenario_diff_file = database.loc[~unchanged_data_rows]

    scenario_cols = [t for t in database.columns if t[1] == c.amount]

    keep_cols = [
        (s.exchange, c.prod_name),
        (s.exchange, c.prod_prod),
        (s.exchange, c.prod_loc),
        (s.exchange, c.prod_key),
        (s.exchange, c.cons_name),
        (s.exchange, c.cons_prod),
        (s.exchange, c.cons_loc),
        (s.exchange, c.cons_key),
        (s.exchange, c.type),
        (s.exchange, c.unit),
    ] + scenario_cols

    scenario_diff_file = scenario_diff_file[keep_cols].droplevel(level=1, axis=1)

    scenario_diff_file.columns = [
        "from activity name",
        "from reference product",
        "from location",
        "from code",
        "to activity name",
        "to reference product",
        "to location",
        "to code",
        "flow type",
        "unit",
    ] + [t[0] for t in scenario_cols]

    scenario_diff_file["from categories"] = scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from location"
    ]

    scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from categories"
    ] = (
        scenario_diff_file.loc[
            scenario_diff_file["flow type"] == "biosphere", "from categories"
        ]
        .str.split("::")
        .apply(lambda x: tuple(x))
    )

    scenario_diff_file.loc[:, "to categories"] = ""

    scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from location"
    ] = ""

    scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from database"
    ] = "biosphere3"
    scenario_diff_file.loc[
        scenario_diff_file["flow type"] != "biosphere", "from database"
    ] = db_name
    scenario_diff_file.loc[:, "to database"] = db_name

    scenario_diff_file["from code"] = scenario_diff_file["from code"].astype("string")
    scenario_diff_file["to code"] = scenario_diff_file["to code"].astype("string")

    scenario_diff_file["from key"] = list(
        scenario_diff_file[["from database", "from code"]].to_records(index=False)
    )
    scenario_diff_file["to key"] = list(
        scenario_diff_file[["to database", "to code"]].to_records(index=False)
    )

    return scenario_diff_file


def export_scenario_factor_file(database, db_name, scenarios, filepath):
    scenario_cols = [
        t for t in database.columns if t[1] == c.amount and t[0] != s.ecoinvent
    ]

    unchanged_data_rows = database[scenario_cols].isnull().all(1)
    scenario_diff_file = database.loc[
        ~unchanged_data_rows | (database[(s.exchange, c.new)] == True)
    ]

    scenario_cols = [t for t in database.columns if t[1] == c.amount]

    keep_cols = [
        (s.exchange, c.prod_name),
        (s.exchange, c.prod_prod),
        (s.exchange, c.prod_loc),
        (s.exchange, c.prod_key),
        (s.exchange, c.cons_name),
        (s.exchange, c.cons_prod),
        (s.exchange, c.cons_loc),
        (s.exchange, c.cons_key),
        (s.exchange, c.type),
        (s.exchange, c.unit),
        (s.exchange, c.new),
    ] + scenario_cols

    scenario_diff_file = scenario_diff_file[keep_cols].droplevel(level=1, axis=1)

    scenario_diff_file.columns = [
        "from activity name",
        "from reference product",
        "from location",
        "from code",
        "to activity name",
        "to reference product",
        "to location",
        "to code",
        "flow type",
        "unit",
        "new",
    ] + [t[0] for t in scenario_cols]

    scenario_diff_file["from categories"] = scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from location"
    ]

    scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from categories"
    ] = (
        scenario_diff_file.loc[
            scenario_diff_file["flow type"] == "biosphere", "from categories"
        ]
        .str.split("::")
        .apply(lambda x: tuple(x))
    )

    scenario_diff_file.loc[:, "to categories"] = ""

    scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from location"
    ] = ""

    scenario_diff_file.loc[
        scenario_diff_file["flow type"] == "biosphere", "from database"
    ] = "biosphere3"

    scenario_diff_file.loc[
        scenario_diff_file["flow type"] != "biosphere", "from database"
    ] = db_name
    scenario_diff_file.loc[:, "to database"] = db_name

    scenario_diff_file["from code"] = scenario_diff_file["from code"].astype("string")
    scenario_diff_file["to code"] = scenario_diff_file["to code"].astype("string")

    scenario_diff_file["from key"] = list(
        scenario_diff_file[["from database", "from code"]].to_records(index=False)
    )
    scenario_diff_file["to key"] = list(
        scenario_diff_file[["to database", "to code"]].to_records(index=False)
    )

    scenario_cols = [scenario[0] for scenario in scenario_cols]

    scenario_diff_file[scenario_cols] = scenario_diff_file[scenario_cols].fillna(0)

    scenario_diff_file.loc[
        scenario_diff_file["new"] == True, scenario_cols
    ] = scenario_diff_file.loc[scenario_diff_file["new"] == True, s.ecoinvent]

    scenario_diff_file.loc[
        (scenario_diff_file["new"] == False) & (scenario_diff_file[s.ecoinvent] != 0),
        scenario_cols,
    ] = scenario_diff_file.loc[
        (scenario_diff_file["new"] == False) & (scenario_diff_file[s.ecoinvent] != 0),
        scenario_cols,
    ].div(
        scenario_diff_file.loc[
            (scenario_diff_file["new"] == False)
            & (scenario_diff_file[s.ecoinvent] != 0),
            s.ecoinvent,
        ],
        axis=0,
    )

    order_dict = {"production": 0, "technosphere": 1, "biosphere": 3}

    scenario_diff_file = scenario_diff_file[
        [
            "from activity name",
            "from reference product",
            "from location",
            "from categories",
            "from database",
            "from key",
            "to activity name",
            "to reference product",
            "to location",
            "to categories",
            "to database",
            "to key",
            "flow type",
            "unit",
        ]
        + scenario_cols
    ].sort_values(by=["flow type"], key=lambda x: x.map(order_dict))

    scenario_diff_file[scenario_cols] = scenario_diff_file[scenario_cols].fillna(0)
    scenario_diff_file[scenario_cols] = scenario_diff_file[scenario_cols].replace(
        [np.inf, -np.inf], 0
    )

    scenario_diff_file.drop(s.ecoinvent, axis=1, inplace=True)

    scenario_diff_file.to_excel(filepath, index=False)

    print(f"Scenario factor file exported to {filepath}!")


def export_scenario_difference_file(database, scenarios, filepath):
    scenario_cols = [
        f'{scenario["model"]}::{scenario["pathway"]}::{scenario["year"]}'
        for scenario in scenarios
    ]

    database = database[
        [
            "from activity name",
            "from reference product",
            "from location",
            "from categories",
            "from database",
            "from key",
            "to activity name",
            "to reference product",
            "to location",
            "to categories",
            "to database",
            "to key",
            "flow type",
        ]
        + scenario_cols
    ].sort_values(by="flow type", ascending=False)

    database[scenario_cols] = database[scenario_cols].fillna(0)

    database.to_excel(filepath, index=False)

    print(f"Scenario factor file exported to {filepath}!")
