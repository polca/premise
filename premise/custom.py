from pathlib import Path

import pandas as pd
import yaml
from schema import And, Optional, Or, Schema, SchemaError, Use

from .ecoinvent_modification import (
    LIST_IMAGE_REGIONS,
    LIST_REMIND_REGIONS,
    SUPPORTED_EI_VERSIONS,
)


def check_custom_scenario(scenario: dict) -> dict:
    """
    Check that all required keys and values are found to add a custom scenario.
    :param scenario: scenario dictionary
    :return: scenario dictionary
    """

    # Validate `scenario`
    dict_schema = Schema(
        {
            "inventories": And(
                str, Use(str), lambda f: Path(f).exists() and Path(f).suffix == ".xlsx"
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
    )

    dict_schema.validate(scenario)

    # Validate yaml config file
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
                                i in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS for i in s
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

    # Validate scenario data
    df = pd.read_excel(scenario["scenario data"])

    return scenario
