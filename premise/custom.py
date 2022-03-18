from pathlib import Path

import numpy as np
import pandas as pd
import wurst
import yaml
from schema import And, Optional, Or, Schema, Use

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

                scaling_factor = (
                    c["efficiency"]
                    .sel(region=location, variables=variable)
                    .values.item(0)
                )

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

                    regions = (
                        df.loc[
                            (df["model"] == model) & (df["pathway"] == pathway),
                            "region",
                        ]
                        .unique()
                        .tolist()
                    )
                    if "except regions" in v:
                        regions = [r for r in regions if r not in v["except regions"]]

                    # add potential technosphere or biosphere filters
                    if "efficiency" in v:
                        a["adjust efficiency"] = True

                        a["regions"] = regions

                        for eff in v["efficiency"]:
                            if "includes" in eff:
                                for flow_type in ["technosphere", "biosphere"]:
                                    if flow_type in eff["includes"]:
                                        items_to_include = eff["includes"][flow_type]
                                        if f"{flow_type} filters" in a:
                                            a[f"{flow_type} filters"].append(
                                                [
                                                    items_to_include,
                                                    {
                                                        r: find_iam_efficiency_change(
                                                            eff["variable"],
                                                            r,
                                                            custom_data,
                                                        )
                                                        for r in regions
                                                    },
                                                ]
                                            )
                                        else:
                                            a[f"{flow_type} filters"] = [
                                                items_to_include,
                                                {
                                                    r: find_iam_efficiency_change(
                                                        eff["variable"],
                                                        r,
                                                        custom_data,
                                                    )
                                                    for r in regions
                                                },
                                            ]
                            else:

                                a[f"technosphere filters"] = [
                                    [
                                        [],
                                        {
                                            r: find_iam_efficiency_change(
                                                eff["variable"],
                                                r,
                                                custom_data,
                                            )
                                            for r in regions
                                        },
                                    ],
                                ]

                    print(a["name"])
                    print(a.get("technosphere filters"))

                    if "replaces" in v:
                        a["replaces"] = v["replaces"]
                    if "replacement ratio" in v:
                        a["replacement ratio"] = v["replacement ratio"]

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
                        Optional("efficiency"): [
                            {
                                "variable": str,
                                Optional("reference year"): And(
                                    Use(int), lambda n: 2005 <= n <= 2100
                                ),
                                Optional("includes"): {
                                    Optional("technosphere"): list,
                                    Optional("biosphere"): list,
                                },
                            }
                        ],
                        Optional("except regions"): And(
                            list,
                            Use(list),
                            lambda s: all(
                                i in LIST_REMIND_REGIONS + LIST_IMAGE_REGIONS for i in s
                            ),
                        ),
                        Optional("replaces"): [{"name": str, "reference product": str}],
                        Optional("replacement ratio"): float,
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
                    Optional("replaces"): [{"name": str, "reference product": str}],
                    Optional("replacement ratio"): float,
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

    def adjust_efficiency(self, dataset: dict) -> dict:
        """
        Adjust the input-to-output efficiency of a dataset and return it back.
        :param dataset: dataset to be adjusted
        :return: adjusted dataset
        """

        if "adjust efficiency" in dataset:

            for eff_type in ["technosphere", "biosphere"]:

                if f"{eff_type} filters" in dataset:
                    for x in dataset[f"{eff_type} filters"]:

                        print(dataset["name"])
                        print(x)

                        scaling_factor = 1 / x[1][dataset["location"]]
                        filters = x[0]
                        wurst.change_exchanges_by_constant_factor(
                            dataset,
                            scaling_factor,
                            technosphere_filters=[
                                ws.either(*[ws.contains("name", x) for x in filters])
                            ]
                            if eff_type == "technosphere"
                            else [],
                            biosphere_filters=[
                                ws.either(*[ws.contains("name", x) for x in filters])
                            ]
                            if eff_type == "biosphere"
                            else [],
                        )

            del dataset["new efficiency"]

        return dataset

    def regionalize_imported_inventories(self) -> None:
        """
        Produce IAM region-specific version fo the dataset.

        """

        acts_to_regionalize = [
            ds for ds in self.database if "custom scenario dataset" in ds
        ]

        for ds in acts_to_regionalize:

            new_acts = self.fetch_proxies(
                name=ds["name"],
                ref_prod=ds["reference product"],
                relink=True,
                regions=ds.get("regions", self.regions),
            )

            # adjust efficiency
            new_acts = {k: self.adjust_efficiency(v) for k, v in new_acts.items()}

            self.database.extend(new_acts.values())

            if "replaces" in ds:
                self.relink_to_new_datasets(
                    replaces=ds["replaces"],
                    new_name=ds["name"],
                    new_ref=ds["reference product"],
                    ratio=ds.get("replacement ratio", 1),
                    regions=ds.get("regions", self.regions),
                )

    def get_market_dictionary_structure(self, config_file: dict, region: str) -> dict:
        """
        Return a dictionary for market creation. To be further filled with exchanges.
        :param config_file: YAML configuration file
        :param region: region to create the dataset for.
        :return: dictionary
        """

        return {
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
                    "amount": 1,
                }
            ],
        }

    def fill_in_world_market(self, config_file: dict, regions: list, i: int) -> dict:

        world_market = self.get_market_dictionary_structure(config_file, "World")
        new_excs = []

        for region in regions:
            supply_share = np.clip(
                (
                    self.custom_data[i]["production volume"]
                    .sel(region=region, year=self.year)
                    .sum(dim="variables")
                    / self.custom_data[i]["production volume"]
                    .sel(year=self.year)
                    .sum(dim=["variables", "region"])
                ).values.item(0),
                0,
                1,
            )

            new_excs.append(
                {
                    "name": config_file["markets"]["name"],
                    "product": config_file["markets"]["reference product"],
                    "unit": config_file["markets"]["unit"],
                    "location": region,
                    "type": "technosphere",
                    "amount": supply_share,
                }
            )

        world_market["exchanges"].extend(new_excs)

        return world_market

    def create_custom_markets(self) -> None:
        """
        Create new custom markets, and create a `World` market
        if no data is provided for it.

        """

        # Loop through custom scenarios
        for i, c in enumerate(self.custom_scenario):

            # Open corresponding config file
            with open(c["config"], "r") as stream:
                config_file = yaml.safe_load(stream)

            # Check if information on market creation is provided
            if "markets" in config_file:
                print("Create custom markets.")

                # Check if there are regions we should not
                # create a market for
                if "except regions" in config_file["markets"]:
                    regions = [
                        r
                        for r in self.regions
                        if r not in config_file["markets"]["except regions"]
                    ]
                else:
                    regions = self.regions

                # Loop through regions
                for region in regions:

                    # Create market dictionary
                    new_market = self.get_market_dictionary_structure(
                        config_file, region
                    )

                    new_excs = []

                    # Loop through the technologies that should compose the market
                    for name in config_file["markets"]["includes"]:

                        # try to see if we find a provider with that region
                        try:
                            act = ws.get_one(
                                self.database,
                                ws.equals("name", name),
                                ws.equals("location", region),
                            )

                            var = act["efficiency variable name"]

                            # supply share = production volume of that technology in this region
                            # over production volume of all technologies in this region
                            supply_share = np.clip(
                                (
                                    self.custom_data[i]["production volume"].sel(
                                        region=region, year=self.year, variables=var
                                    )
                                    / self.custom_data[i]["production volume"]
                                    .sel(region=region, year=self.year)
                                    .sum(dim="variables")
                                ).values.item(0),
                                0,
                                1,
                            )

                            new_excs.append(
                                {
                                    "name": act["name"],
                                    "product": act["reference product"],
                                    "unit": act["unit"],
                                    "location": act["location"],
                                    "type": "technosphere",
                                    "amount": supply_share,
                                }
                            )
                        # if we do not find a supplier, it can be correct if it was
                        # listed in `except regions`. In any case, we jump to the next technology.
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
                    else:
                        regions.remove(region)

                # if so far, a market for `World` has not been created
                # we need to create one then
                if "World" not in regions:
                    world_market = self.fill_in_world_market(config_file, regions, i)
                    self.database.append(world_market)

                # if the new markets are meant to replace for other
                # providers in the database
                if "replaces" in config_file["markets"]:
                    self.relink_to_new_datasets(
                        replaces=config_file["markets"]["replaces"],
                        new_name=config_file["markets"]["name"],
                        new_ref=config_file["markets"]["reference product"],
                        ratio=config_file["markets"].get("replacement ratio", 1),
                        regions=regions,
                    )

    def relink_to_new_datasets(
        self, replaces: list, new_name: str, new_ref: str, ratio, regions: list
    ) -> None:
        """
        Replaces exchanges that match `old_name` and `old_ref` with exchanges that
        have `new_name` and `new_ref`. The new exchange is from an IAM region, and so, if the
        region is not part of `regions`, we use `World` instead.

        :param old_name: `name` of the exchange to replace
        :param old_ref: `product` of the exchange to replace
        :param new_name: `name`of the new provider
        :param new_ref: `product` of the new provider
        :param regions: list of IAM regigons the new provider can originate from

        """

        print("Relink to new markets.")

        providers_to_replace = [(x["name"], x["reference product"]) for x in replaces]

        for ds in self.database:
            for exc in ds["exchanges"]:
                if (exc["name"], exc.get("product")) in providers_to_replace and exc[
                    "type"
                ] == "technosphere":

                    if ds["location"] in self.regions:
                        if ds["location"] not in regions:
                            new_loc = "World"
                        else:
                            new_loc = ds["location"]
                    else:
                        new_loc = self.ecoinvent_to_iam_loc[ds["location"]]

                    exc["name"] = new_name
                    exc["product"] = new_ref
                    exc["location"] = new_loc
                    exc["amount"] *= ratio

                    if "input" in exc:
                        del exc["input"]
