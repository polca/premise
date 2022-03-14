from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from schema import And, Optional, Or, Schema, Use
import wurst

from .ecoinvent_modification import (
    LIST_IMAGE_REGIONS,
    LIST_REMIND_REGIONS,
    SUPPORTED_EI_VERSIONS,
)
from .transformation import *
from .utils import eidb_label


def find_iam_efficiency_change(
        variable: Union[str, list], location: str, custom_data
) -> float:
    """
    Return the relative change in efficiency for `variable` in `location`
    relative to 2020.
    :param variable: IAM variable name
    :param location: IAM region
    :return: relative efficiency change (e.g., 1.05)
    """

    for c in custom_data.values():
        if "efficiency" in c:
            if variable in c["efficiency"].variables.values:

                scaling_factor = c["efficiency"].sel(
                    region=location, variables=variable
                ).values.item(0)

                if scaling_factor in (np.nan, np.inf):
                    scaling_factor = 1

    return scaling_factor

def check_inventories(custom_scenario, data, model, pathway, custom_data):

    for i, scenario in enumerate(custom_scenario):

        with open(scenario["config"], "r") as stream:
            config_file = yaml.safe_load(stream)

        df = pd.read_excel(scenario["scenario data"])

        for k, v in config_file["production pathways"].items():

            name = v["ecoinvent alias"]["name"]
            ref = v["ecoinvent alias"]["reference product"]

            if (
                len(
                    [
                        a
                        for a in data
                        if (name, ref) == (a["name"], a["reference product"])
                    ]
                )
                == 0
            ):
                raise ValueError(
                    f"The inventories provided do not contain the activity: {name, ref}"
                )

            for a in data:
                a["custom scenario dataset"] = True
                if (name, ref) == (a["name"], a["reference product"]):
                    regions = df.loc[(df["model"] == model) & (df["pathway"] == pathway), "region"].unique().tolist()
                    if "except regions" in v:
                        regions = [r for r in regions if r not in v["except regions"]]

                    a["adjust efficiency"] = True
                    a["new efficiency"] = {r: find_iam_efficiency_change(k, r, custom_data) for r in regions}
                    a["efficiency variable name"] = k
                    a["regions"] = regions

        return data


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
                        Optional("except regions"): And(
                                list,
                                Use(list),
                                lambda s: all(
                                    i in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS
                                    for i in s
                                ),
                            ),

                    },
                },
                Optional("markets"): {
                    "name": str,
                    "reference product": str,
                    "unit": str,
                    "includes": list,
                    Optional("except regions"): And(
                            list,
                            Use(list),
                            lambda s: all(
                                i in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS for i in s
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


class Custom(BaseTransformation):

    def __init__(
            self,
            database: List[dict],
            iam_data: IAMDataCollection,
            custom_scenario: dict,
            custom_data: dict,
            model: str,
            pathway: str,
            year: int,
            version: str,
    ):
        super().__init__(database, iam_data, model, pathway, year)
        self.custom_scenario = custom_scenario
        self.custom_data = custom_data


    def adjust_efficiency(self, dataset):

        if "adjust efficiency" in dataset:

            scaling_factor = 1 / dataset["new efficiency"][dataset["location"]]

            wurst.change_exchanges_by_constant_factor(
                dataset,
                scaling_factor)

            del dataset["new efficiency"]

        return dataset

    def regionalize_imported_inventories(self):

        acts_to_regionalize = [ds for ds in self.database if "custom scenario dataset" in ds]

        for ds in acts_to_regionalize:

            new_acts = self.fetch_proxies(
                name=ds["name"],
                ref_prod=ds["reference product"],
                relink=True,
                regions=ds.get("regions", self.regions)
            )

            # adjust efficiency
            new_acts = {k: self.adjust_efficiency(v) for k, v in new_acts.items()}

            self.database.extend(new_acts.values())

    def create_custom_markets(self):

        for i, c in enumerate(self.custom_scenario):

            with open(c["config"], "r") as stream:
                config_file = yaml.safe_load(stream)

            if "markets" in config_file:
                print("Create custom markets.")
                if "except regions" in config_file["markets"]:
                    regions = (
                        region for region in self.regions
                        if region not in config_file["markets"]["except regions"]
                    )
                else:
                    regions = self.regions

                for region in regions:

                    new_market = {
                        "name": config_file["markets"]["name"],
                        "reference product": config_file["markets"]["reference product"],
                        "unit": config_file["markets"]["unit"],
                        "location": region,
                        "database": eidb_label(self.model, self.scenario, self.year),
                        "code": str(uuid.uuid4().hex),
                        "exchanges": [
                            {
                                "name": config_file["markets"]["name"],
                                "product": config_file["markets"]["reference product"],
                                "unit": config_file["markets"]["unit"],
                                "location": region,
                                "type": "production",
                                "amount": 1
                            }
                        ]
                    }

                    new_excs = []

                    for name in config_file["markets"]["includes"]:

                        try:
                            act = ws.get_one(
                                self.database,
                                ws.equals("name", name),
                                ws.equals("location", region)
                            )

                            var = act["efficiency variable name"]

                            supply_share = (self.custom_data[i]["production volume"].sel(
                                        region=region,
                                        year=self.year,
                                        variables=var)/self.custom_data[i]["production volume"].sel(
                                        region=region,
                                        year=self.year).sum(dim="variables")).values.item(0)

                            new_excs.append(
                                {
                                    "name": act["name"],
                                    "product": act["reference product"],
                                    "unit": act["unit"],
                                    "location": act["location"],
                                    "type": "technosphere",
                                    "amount": supply_share
                                }
                            )
                        except ws.NoResults:
                            continue

                    if len(new_excs) > 0:
                        total = 0
                        for exc in new_excs:
                            total += exc["amount"]
                        for exc in new_excs:
                            exc["amount"] /= total

                        new_market["exchanges"].extend(new_excs)

                        self.database.append(new_market)

                if "World" not in regions:
                    new_excs = []
                    new_market = {
                        "name": config_file["markets"]["name"],
                        "reference product": config_file["markets"]["reference product"],
                        "unit": config_file["markets"]["unit"],
                        "location": "World",
                        "database": eidb_label(self.model, self.scenario, self.year),
                        "code": str(uuid.uuid4().hex),
                        "exchanges": [
                            {
                                "name": config_file["markets"]["name"],
                                "product": config_file["markets"]["reference product"],
                                "unit": config_file["markets"]["unit"],
                                "location": "World",
                                "type": "production",
                                "amount": 1
                            }
                        ]
                    }

                    for region in regions:

                        supply_share = (self.custom_data[i]["production volume"].sel(
                            region=region,
                            year=self.year,
                            variables=var) / self.custom_data[i]["production volume"].sel(
                            region=region,
                            year=self.year).sum(dim="variables")).values.item(0)

                        new_excs.append(
                            {
                                "name": config_file["markets"]["name"],
                                "product": config_file["markets"]["reference product"],
                                "unit": config_file["markets"]["unit"],
                                "location": region,
                                "type": "technosphere",
                                "amount": supply_share
                            }
                        )

                    new_market["exchanges"].extend(new_excs)
                    self.database.append(new_market)

                if "replaces" in config_file["markets"]:

                    self.relink_to_new_markets(
                        old_name = config_file["markets"]["replaces"]["name"],
                        old_ref = config_file["markets"]["replaces"]["reference product"],
                        new_name = config_file["markets"]["name"],
                        new_ref = config_file["markets"]["reference product"],
                        regions=regions
                    )


    def relink_to_new_markets(self, old_name, old_ref, new_name, new_ref, regions):

        print("Relink to new markets.")

        for ds in self.database:
            for exc in ds["exchanges"]:
                if (exc["name"], exc.get("product")) == (old_name, old_ref) and exc["type"] == "technosphere":

                    new_loc = self.ecoinvent_to_iam_loc[ds["location"]]
                    if new_loc not in regions:
                        new_loc = "World"

                    exc["name"] = new_name
                    exc["product"] = new_ref
                    exc["location"] = new_loc
                    if "input" in exc:
                        del exc["input"]







