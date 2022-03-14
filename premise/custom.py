from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from schema import And, Optional, Or, Schema, Use

from .ecoinvent_modification import (
    LIST_IMAGE_REGIONS,
    LIST_REMIND_REGIONS,
    SUPPORTED_EI_VERSIONS,
)


def check_inventories(custom_scenario, data):

    for i, scenario in enumerate(custom_scenario):

        with open(scenario["config"], "r") as stream:
            config_file = yaml.safe_load(stream)

        for k in config_file["production pathways"].values():

            name = k["ecoinvent alias"]["name"]
            ref = k["ecoinvent alias"]["reference product"]

            if len([a for a in data if (name, ref) == (a["name"], a["reference product"])]) == 0:
                raise ValueError(f"The inventories provided do not contain the activity: {name, ref}")


def check_custom_scenario_dictionary(custom_scenario):

    dict_schema = Schema(
        [
            {
                "inventories": And(
                    str,
                    Use(str),
                    lambda f: Path(f).exists() and Path(f).suffix == ".xlsx",
                ),
                "scenario data": And(
                    Use(str), lambda f: Path(f).exists() and Path(f).suffix == ".xlsx"
                ),
                "config": And(
                    Use(str), lambda f: Path(f).exists() and Path(f).suffix == ".yaml"
                ),
                Optional("ecoinvent version"): And(
                    Use(str), lambda v: v in SUPPORTED_EI_VERSIONS
                ),
            }
        ]
    )

    dict_schema.validate(custom_scenario)

    if (
        sum(s == y for s in custom_scenario for y in custom_scenario)
        / len(custom_scenario)
        > 1
    ):
        raise ValueError("Two or more entries in `custom_scenario` are similar.")


def check_config_file(custom_scenario):

    for i, scenario in enumerate(custom_scenario):

        with open(scenario["config"], "r") as stream:
            config_file = yaml.safe_load(stream)

        file_schema = Schema(
            {
                "production pathways": {
                    str: {
                        "production volume": {
                            "variable": str,
                        },
                        "ecoinvent alias": {
                            "name": str,
                            "reference product": str,
                            "exists in ecoinvent": bool,
                        },
                        Optional("efficiency"): {"variable": str},
                        Optional("except regions"): Or(
                            And(
                                str,
                                Use(str),
                                lambda s: s in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS,
                            ),
                            And(
                                list,
                                Use(list),
                                lambda s: all(
                                    i in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS
                                    for i in s
                                ),
                            ),
                        ),
                    },
                },
                Optional("markets"): {
                    "name": str,
                    "reference product": str,
                    Optional("except regions"): Or(
                        And(
                            str,
                            Use(str),
                            lambda s: s in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS,
                        ),
                        And(
                            list,
                            Use(list),
                            lambda s: all(
                                i in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS for i in s
                            ),
                        ),
                    ),
                    Optional("replaces"): {"name": str, "reference product": str},
                },
            }
        )

        file_schema.validate(config_file)


def check_scenario_data_file(custom_scenario, iam_scenarios):

    for i, scenario in enumerate(custom_scenario):

        with open(scenario["config"], "r") as stream:
            config_file = yaml.safe_load(stream)

        df = pd.read_excel(scenario["scenario data"])

        mandatory_fields = ["model", "pathway", "region", "variables", "unit"]
        if not all(v in df.columns for v in mandatory_fields):
            raise ValueError(
                f"One or several mandatory column are missing "
                f"in the scenario data file no. {i + 1}. Mandatory columns: {mandatory_fields}."
            )

        years_cols = [c for c in df.columns if isinstance(c, int)]
        if any(y for y in years_cols if y < 2005 or y > 2100):
            raise ValueError(
                f"One or several of the years provided in the scenario data file no. {i + 1} are "
                "out of boundaries (2005 - 2100)."
            )

        if len(pd.isnull(df).sum()[pd.isnull(df).sum() > 0]) > 0:
            raise ValueError(
                f"The following columns in the scenario data file no. {i + 1}"
                f"contains empty cells.\n{pd.isnull(df).sum()[pd.isnull(df).sum() > 0]}."
            )

        if any(
            m not in [s["model"] for s in iam_scenarios] for m in df["model"].unique()
        ):
            raise ValueError(
                f"One or several model name(s) in the scenario data file no. {i + 1} "
                "is/are not found in the list of scenarios to create."
            )

        if any(
            m not in df["model"].unique() for m in [s["model"] for s in iam_scenarios]
        ):
            raise ValueError(
                f"One or several model name(s) in the list of scenarios to create "
                f"is/are not found in the scenario data file no. {i + 1}. "
            )


        if any(
            m not in [s["pathway"] for s in iam_scenarios]
            for m in df["pathway"].unique()
        ):
            raise ValueError(
                f"One or several pathway name(s) in the scenario data file no. {i + 1} "
                "is/are not found in the list of scenarios to create."
            )

        if any(
            m not in df["pathway"].unique()
            for m in [s["pathway"] for s in iam_scenarios]
        ):
            raise ValueError(
                f"One or several pathway name(s) in the list of scenarios to create "
                f"is/are not found in the scenario data file no. {i + 1}."
            )

        d_regions = {"remind": LIST_REMIND_REGIONS, "image": LIST_IMAGE_REGIONS}

        for irow, r in df.iterrows():
            if r["region"] not in d_regions[r["model"]]:
                raise ValueError(
                    f"Region {r['region']} indicated "
                    f"in row {irow} is not valid for model {r['model'].upper()}."
                )

        if not all(
            v in get_recursively(config_file, "variable")
            for v in df["variables"].unique()
        ):
            raise ValueError(
                f"One or several variable names in the scenario data file no. {i + 1} "
                "cannot be found in the configuration file."
            )

        try:
            np.array_equal(df.iloc[:, 5:], df.iloc[:, 5:].astype(float))
        except ValueError as e:
            raise TypeError(
                f"All values provided in the time series must be numerical "
                f"in the scenario data file no. {i + 1}."
            ) from e

    return custom_scenario


def get_recursively(search_dict, field):
    """Takes a dict with nested lists and dicts,
    and searches all dicts for a key of the field
    provided.
    """
    fields_found = []

    for key, value in search_dict.items():

        if key == field:
            fields_found.append(value)

        elif isinstance(value, dict):
            results = get_recursively(value, field)
            for result in results:
                fields_found.append(result)

        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    more_results = get_recursively(item, field)
                    for another_result in more_results:
                        fields_found.append(another_result)

    return fields_found


def check_custom_scenario(custom_scenario: dict, iam_scenarios: list) -> dict:
    """
    Check that all required keys and values are found to add a custom scenario.
    :param custom_scenario: scenario dictionary
    :return: scenario dictionary
    """

    # Validate `custom_scenario` dictionary
    check_custom_scenario_dictionary(custom_scenario)

    # Validate yaml config file
    check_config_file(custom_scenario)

    # Validate scenario data
    check_scenario_data_file(custom_scenario, iam_scenarios)

    return custom_scenario


