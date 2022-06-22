"""
Implements external scenario data.
"""

import pandas as pd
import yaml

from .clean_datasets import get_biosphere_flow_uuid
from .transformation import *
from .utils import eidb_label


def flag_activities_to_adjust(a, df, model, pathway, v, custom_data):
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
                            ]
            else:

                a[f"technosphere filters"] = [
                    [
                        None,
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

                a[f"biosphere filters"] = [
                    [
                        None,
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

    if "replaces" in v:
        a["replaces"] = v["replaces"]

    if "replaces in" in v:
        a["replaces in"] = v["replaces in"]

    if "replacement ratio" in v:
        a["replacement ratio"] = v["replacement ratio"]

    return a


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

    scaling_factor = 1
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


def detect_ei_activities_to_adjust(datapackages, data, model, pathway, custom_data):
    """
    Flag activities native to ecoinvent that will their efficiency to be adjusted.
    """

    for i, dp in enumerate(datapackages):

        resource = dp.get_resource("config")
        config_file = yaml.safe_load(resource.raw_read())

        resource = dp.get_resource("scenario_data")
        scenario_data = resource.read()
        scenario_headers = resource.headers

        df = pd.DataFrame(scenario_data, columns=scenario_headers)

        for k, v in config_file["production pathways"].items():

            if v["ecoinvent alias"].get("exists in original database"):

                if v.get("efficiency"):

                    name = v["ecoinvent alias"]["name"]
                    ref = v["ecoinvent alias"]["reference product"]

                    for ds in ws.get_many(
                        data,
                        ws.equals("name", name),
                        ws.equals("reference product", ref),
                    ):
                        ds = flag_activities_to_adjust(
                            ds, df, model, pathway, v, custom_data
                        )
    return data


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


def adjust_efficiency(dataset: dict) -> dict:
    """
    Adjust the input-to-output efficiency of a dataset and return it back.
    :param dataset: dataset to be adjusted
    :return: adjusted dataset
    """

    if "adjust efficiency" in dataset:

        for eff_type in ["technosphere", "biosphere"]:

            if f"{eff_type} filters" in dataset:
                for x in dataset[f"{eff_type} filters"]:

                    scaling_factor = 1 / x[1][dataset["location"]]
                    filters = x[0]

                    if eff_type == "technosphere":

                        for exc in ws.technosphere(
                            dataset,
                            *[ws.contains("name", x) for x in filters]
                            if filters is not None
                            else [],
                        ):
                            wurst.rescale_exchange(exc, scaling_factor)

                    else:

                        for exc in ws.biosphere(
                            dataset,
                            *[ws.contains("name", x) for x in filters]
                            if filters is not None
                            else [],
                        ):
                            wurst.rescale_exchange(exc, scaling_factor)

    return dataset


def fetch_dataset_description_from_production_pathways(config, item):
    for p, v in config["production pathways"].items():
        if p == item:
            if "exists in original database" not in v["ecoinvent alias"]:
                v["ecoinvent alias"].update({"exists in original database": True})

            if "new dataset" not in v["ecoinvent alias"]:
                v["ecoinvent alias"].update({"new dataset": False})

            if "regionalize" not in v["ecoinvent alias"]:
                v["ecoinvent alias"].update({"regionalize": False})

            return (
                v["ecoinvent alias"]["name"],
                v["ecoinvent alias"]["reference product"],
                v["ecoinvent alias"]["exists in original database"],
                v["ecoinvent alias"]["new dataset"],
                v["ecoinvent alias"]["regionalize"],
            )


def fetch_var(config_file, list_vars):

    return [
        config_file["production pathways"][v]["production volume"]["variable"]
        for v in list_vars
    ]


class ExternalScenario(BaseTransformation):
    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        external_scenarios: list,
        external_scenarios_data: dict,
        model: str,
        pathway: str,
        year: int,
    ):
        super().__init__(database, iam_data, model, pathway, year)
        self.datapackages = external_scenarios
        self.external_scenarios_data = external_scenarios_data

        for i, datapackage in enumerate(self.datapackages):
            regions = self.external_scenarios_data[i]["regions"]
            # Open corresponding config file
            resource = datapackage.get_resource("config")
            config_file = yaml.safe_load(resource.raw_read())
            self.regionalize_imported_inventories(config_file, regions)
        self.dict_bio_flows = get_biosphere_flow_uuid()

    def regionalize_imported_inventories(self, config_file, regions) -> None:
        """
        Produce IAM region-specific version of the dataset.

        """

        acts_to_regionalize = [
            {
                "name": ds["name"],
                "reference product": ds["reference product"],
                "location": ds["location"],
                "replaces": ds.get("replaces", None),
                "replaces in": ds.get("replaces in", None),
            }
            for ds in self.database
            if "custom scenario dataset" in ds
        ]

        for ds in acts_to_regionalize:

            if ds["location"] not in regions:
                new_acts = self.fetch_proxies(
                    name=ds["name"],
                    ref_prod=ds["reference product"],
                    regions=regions,
                )

                # adjust efficiency
                new_acts = {k: adjust_efficiency(v) for k, v in new_acts.items()}
                self.database.extend(new_acts.values())
            else:
                new_acts = ws.get_many(
                    self.database,
                    ws.equals("name", ds["name"]),
                    ws.equals("reference product", ds["reference product"]),
                    ws.either(*[ws.equals("location", x) for x in regions]),
                )
                for v in new_acts:
                    adjust_efficiency(v)

            if ds["replaces"]:

                self.relink_to_new_datasets(
                    replaces=ds["replaces"],
                    replaces_in=ds.get("replaces in", None),
                    new_name=ds["name"],
                    new_ref=ds["reference product"],
                    ratio=ds.get("replacement ratio", 1),
                    regions=ds.get("regions", regions),
                )

    def get_market_dictionary_structure(self, market: dict, region: str) -> dict:
        """
        Return a dictionary for market creation.
        To be further filled with exchanges.
        :param market: YAML configuration file
        :param region: region to create the dataset for.
        :return: dictionary
        """

        return {
            "name": market["name"],
            "reference product": market["reference product"],
            "unit": market["unit"],
            "location": region,
            "database": eidb_label(self.model, self.scenario, self.year),
            "code": str(uuid.uuid4().hex),
            "exchanges": [
                {
                    "name": market["name"],
                    "product": market["reference product"],
                    "unit": market["unit"],
                    "location": region,
                    "type": "production",
                    "amount": 1,
                }
            ],
        }

    def fill_in_world_market(
        self, market: dict, regions: list, i: int, pathways: list
    ) -> dict:

        world_market = self.get_market_dictionary_structure(market, "World")
        new_excs = []

        for region in regions:
            print(market, regions, pathways)
            print(self.external_scenarios_data[i]["production volume"].variables)
            supply_share = np.clip(
                (
                    self.external_scenarios_data[i]["production volume"]
                    .sel(region=region, year=self.year, variables=pathways)
                    .sum(dim="variables")
                    / self.external_scenarios_data[i]["production volume"]
                    .sel(year=self.year, variables=pathways)
                    .sum(dim=["variables", "region"])
                ).values.item(0),
                0,
                1,
            )

            new_excs.append(
                {
                    "name": market["name"],
                    "product": market["reference product"],
                    "unit": market["unit"],
                    "location": region,
                    "type": "technosphere",
                    "amount": supply_share,
                }
            )

        world_market["exchanges"].extend(new_excs)

        return world_market

    def check_existence_of_market_suppliers(self):

        # Loop through custom scenarios
        for i, dp in enumerate(self.datapackages):

            # Open corresponding config file
            resource = dp.get_resource("config")
            config_file = yaml.safe_load(resource.raw_read())

            # Check if information on market creation is provided
            if "markets" in config_file:

                for market in config_file["markets"]:

                    # Loop through the technologies that should compose the market
                    for pathway_to_include in market["includes"]:

                        # fetch the dataset name/ref corresponding to this item
                        # under `production pathways`
                        (
                            name,
                            ref_prod,
                            exists_in_database,
                            new_dataset,
                            regionalize_dataset,
                        ) = fetch_dataset_description_from_production_pathways(
                            config_file, pathway_to_include
                        )

                        if not new_dataset:
                            regions = self.external_scenarios_data[i]["regions"]

                            # try to see if we find a provider for these regions
                            if any(region in self.regions for region in regions):
                                ecoinvent_regions = [
                                    self.geo.iam_to_ecoinvent_location(region)
                                    for region in regions
                                ]
                            else:
                                ecoinvent_regions = [
                                    r[-1]
                                    for r in [self.geo.geo.within(x) for x in regions]
                                    if r[0] == "ecoinvent" and r[-1] != "GLO"
                                ]

                            possible_locations = [
                                *regions,
                                *ecoinvent_regions,
                                "RER",
                                "Europe without Switzerland",
                                "RoW",
                                "GLO",
                            ]

                            suppliers, counter = [], 0

                            try:
                                while not suppliers:
                                    suppliers = list(
                                        ws.get_many(
                                            self.database,
                                            ws.equals("name", name),
                                            ws.equals(
                                                "reference product",
                                                ref_prod,
                                            ),
                                            ws.equals(
                                                "location", possible_locations[counter]
                                            ),
                                        )
                                    )

                                    counter += 1

                            except IndexError:
                                raise ValueError(
                                    f"Regionalized datasets for pathway {pathway_to_include} "
                                    f"with `name` {name} and `reference product` {ref_prod} "
                                    f"cannot be found in "
                                    f"locations {possible_locations}."
                                )

                            if not exists_in_database or regionalize_dataset:
                                for ds in suppliers:
                                    ds["custom scenario dataset"] = True
                                    print(ds["name"])

    def fetch_supply_share(self, i, region, var, vars):
        return np.clip(
            (
                self.external_scenarios_data[i]["production volume"].sel(
                    region=region,
                    year=self.year,
                    variables=var,
                )
                / self.external_scenarios_data[i]["production volume"]
                .sel(
                    region=region,
                    year=self.year,
                    variables=vars,
                )
                .sum(dim="variables")
            ).values.item(0),
            0,
            1,
        )

    def fetch_potential_suppliers(self, possible_locations, name, ref_prod):

        try:
            act, counter = [], 0
            while not act:
                act = list(
                    ws.get_many(
                        self.database,
                        ws.equals("name", name),
                        ws.equals(
                            "reference product",
                            ref_prod,
                        ),
                        ws.equals("location", possible_locations[counter]),
                    )
                )

                counter += 1
        except IndexError:
            print("Cannot find -> ", name, ref_prod, possible_locations)

        return act

    def write_suppliers_exchanges(self, suppliers, supply_share):

        new_excs = []

        for supplier, market_share in suppliers.items():
            provider_share = supply_share * market_share

            new_excs.append(
                {
                    "name": supplier[0],
                    "product": supplier[2],
                    "unit": supplier[-1],
                    "location": supplier[1],
                    "type": "technosphere",
                    "amount": provider_share,
                    "uncertainty type": 0,
                }
            )

        return new_excs

    def create_custom_markets(self) -> None:
        """
        Create new custom markets, and create a `World` market
        if no data is provided for it.

        """

        self.check_existence_of_market_suppliers()

        # Loop through custom scenarios
        for i, dp in enumerate(self.datapackages):

            # Open corresponding config file
            resource = dp.get_resource("config")
            config_file = yaml.safe_load(resource.raw_read())

            # Check if information on market creation is provided
            if "markets" in config_file:
                print("Create custom markets.")

                for market in config_file["markets"]:

                    # fetch all scenario file vars that
                    # relate to this market

                    pathways = market["includes"]

                    vars = fetch_var(config_file, pathways)

                    # Check if there are regions we should not
                    # create a market for
                    regions = self.external_scenarios_data[i]["regions"]

                    if "except regions" in market:
                        regions = [
                            r for r in regions if r not in market["except regions"]
                        ]

                    # Loop through regions
                    for region in regions:

                        # Create market dictionary
                        new_market = self.get_market_dictionary_structure(
                            market, region
                        )

                        new_excs = []
                        for pathway in pathways:

                            var = fetch_var(config_file, [pathway])[0]

                            # fetch the dataset name/ref corresponding to this item
                            # under `production pathways`
                            (
                                name,
                                ref_prod,
                                exists_in_database,
                                new_dataset,
                                _,
                            ) = fetch_dataset_description_from_production_pathways(
                                config_file, pathway
                            )

                            # try to see if we find a provider with that region
                            if region in self.regions:
                                ecoinvent_regions = []

                            else:
                                ecoinvent_regions = [
                                    r[-1]
                                    for r in self.geo.geo.within(region)
                                    if r[0] == "ecoinvent"
                                    and r[-1] not in ["GLO", "RoW"]
                                ]

                            possible_locations = [
                                region,
                                *ecoinvent_regions,
                                "RER",
                                "Europe without Switzerland",
                                "RoW",
                                "GLO",
                            ]

                            potential_suppliers = self.fetch_potential_suppliers(
                                possible_locations, name, ref_prod
                            )

                            # supply share = production volume of that technology in this region
                            # over production volume of all technologies in this region

                            try:
                                supply_share = self.fetch_supply_share(
                                    i, region, var, vars
                                )

                            except KeyError:
                                print(
                                    "suppliers for ",
                                    name,
                                    ref_prod,
                                    region,
                                    "not found",
                                )
                                continue

                            if supply_share > 0:

                                suppliers = get_shares_from_production_volume(
                                    potential_suppliers
                                )
                                new_excs.extend(
                                    self.write_suppliers_exchanges(
                                        suppliers, supply_share
                                    )
                                )

                        if len(new_excs) > 0:
                            total = 0
                            for exc in new_excs:
                                total += exc["amount"]
                            for exc in new_excs:
                                exc["amount"] /= total

                            new_market["exchanges"].extend(new_excs)

                            # check if we should add some additional exchanges
                            if "add" in market:

                                for additional_exc in market["add"]:

                                    name = additional_exc["name"]
                                    ref_prod = additional_exc.get("reference product")
                                    categories = additional_exc.get("categories")
                                    unit = additional_exc.get("unit")
                                    amount = additional_exc["amount"]

                                    if ref_prod:
                                        # this is a technosphere exchange
                                        ecoinvent_regions = [
                                            r[-1]
                                            for r in self.geo.geo.within(region)
                                            if r[0] == "ecoinvent"
                                            and r[-1] not in ["GLO", "RoW"]
                                        ]

                                        possible_locations = [
                                            region,
                                            *ecoinvent_regions,
                                            "RER",
                                            "Europe without Switzerland",
                                            "RoW",
                                            "GLO",
                                        ]
                                        potential_suppliers = (
                                            self.fetch_potential_suppliers(
                                                possible_locations, name, ref_prod
                                            )
                                        )
                                        suppliers = get_shares_from_production_volume(
                                            potential_suppliers
                                        )

                                        new_market["exchanges"].extend(
                                            self.write_suppliers_exchanges(
                                                suppliers, amount
                                            )
                                        )

                                    else:
                                        # this is a biosphere exchange

                                        categories = tuple(categories.split("::"))
                                        if len(categories) == 1:
                                            categories += ("unspecified",)

                                        key = (name, categories[0], categories[1], unit)

                                        new_market["exchanges"].append(
                                            {
                                                "name": name,
                                                "unit": unit,
                                                "categories": categories,
                                                "type": "biosphere",
                                                "amount": amount,
                                                "uncertainty type": 0,
                                                "input": (
                                                    "biosphere3",
                                                    self.dict_bio_flows[key],
                                                ),
                                            }
                                        )

                            # check if there are variables that
                            # relate to inefficiencies or losses

                            if "efficiency" in market:
                                for ineff in market["efficiency"]:
                                    scaling_factor = 1 / find_iam_efficiency_change(
                                        ineff["variable"],
                                        region,
                                        self.external_scenarios_data,
                                    )

                                    if not "includes" in ineff:

                                        wurst.change_exchanges_by_constant_factor(
                                            new_market, scaling_factor
                                        )

                                    else:

                                        if "technosphere" in ineff["includes"]:
                                            fltr = []
                                            for y in ineff["includes"]["technosphere"]:
                                                for k, v in y.items():
                                                    print(k, v, scaling_factor)
                                                    fltr.append(wurst.contains(k, v))

                                            for exc in ws.technosphere(
                                                new_market, *(fltr or [])
                                            ):
                                                wurst.rescale_exchange(
                                                    exc, scaling_factor
                                                )

                                        if "biosphere" in ineff["includes"]:
                                            fltr = []
                                            for y in ineff["includes"]["biosphere"]:
                                                for k, v in y.items():
                                                    print(k, v, scaling_factor)
                                                    fltr.append(wurst.contains(k, v))

                                            for exc in ws.biosphere(
                                                new_market, *(fltr or [])
                                            ):
                                                wurst.rescale_exchange(
                                                    exc, scaling_factor
                                                )

                            self.database.append(new_market)
                        else:
                            regions.remove(region)

                        # Loop through the technologies that should
                        # compose the market

                    # if so far, a market for `World` has not been created
                    # we need to create one then

                    create_world_region = True

                    if "World" in regions:
                        create_world_region = False

                    if "except regions" in market:
                        if "World" in market["except regions"]:
                            create_world_region = False

                    if create_world_region:

                        world_market = self.fill_in_world_market(
                            market, regions, i, vars
                        )
                        self.database.append(world_market)

                    # if the new markets are meant to replace for other
                    # providers in the database
                    if "replaces" in market:

                        self.relink_to_new_datasets(
                            replaces=market["replaces"],
                            replaces_in=market.get("replaces in", None),
                            new_name=market["name"],
                            new_ref=market["reference product"],
                            ratio=market.get("replacement ratio", 1),
                            regions=regions,
                        )

    def relink_to_new_datasets(
        self,
        replaces: list,
        replaces_in: list,
        new_name: str,
        new_ref: str,
        ratio,
        regions: list,
    ) -> None:
        """
        Replaces exchanges that match `old_name` and `old_ref` with exchanges that
        have `new_name` and `new_ref`. The new exchange is from an IAM region, and so, if the
        region is not part of `regions`, we use `World` instead.

        :param old_name: `name` of the exchange to replace
        :param old_ref: `product` of the exchange to replace
        :param new_name: `name`of the new provider
        :param new_ref: `product` of the new provider
        :param regions: list of IAM regions the new provider can originate from

        """

        print("Relink to new markets.")

        datasets = []

        if replaces_in:

            for k in replaces_in:
                list_fltr = []
                for f in ["name", "reference product", "location", "unit"]:
                    if f in k:
                        list_fltr.append(ws.equals(f, k[f]))

                datasets.extend(list(ws.get_many(self.database, *list_fltr)))
        else:
            datasets = self.database

        list_fltr = []
        for k in replaces:
            for f in ["name", "product", "location", "unit"]:
                if f in k:
                    list_fltr.append(ws.equals(f, k[f]))

        for ds in datasets:
            for exc in ws.technosphere(ds, ws.either(*list_fltr)):

                if ds["location"] in regions or ds["location"] == "World":
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
