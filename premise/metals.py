"""
Integrates projections regarding use of metals in the economy from:
- Mining shares
- Metal intensities
- Transport distances

"""

import uuid
from functools import lru_cache
from typing import Optional
from collections import defaultdict

import country_converter as coco
import numpy as np
import pandas as pd
import yaml

from .export import biosphere_flows_dictionary
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    Dict,
    IAMDataCollection,
    InventorySet,
    List,
    Set,
    ws,
)
from .utils import DATA_DIR
from .validation import MetalsValidation

logger = create_logger("metal")

EI311_NAME_CHANGES = {
    "sodium borate mine operation and beneficiation": "sodium borates mine operation and beneficiation",
    "gallium production, semiconductor-grade": "high-grade gallium production, from low-grade gallium",
}

EI311_PRODUCT_CHANGES = {"gallium, semiconductor-grade": "gallium, high-grade"}
EI311_LOCATION_CHANGES = {"high-grade gallium production, from low-grade gallium": "CN"}


def _update_metals(scenario, version, system_model):

    metals = Metals(
        database=scenario["database"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    metals.create_metal_markets()
    metals.update_metals_use_in_database()
    metals.relink_datasets()
    scenario["database"] = metals.database
    scenario["cache"] = metals.cache
    scenario["index"] = metals.index

    validate = MetalsValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=metals.database,
        iam_data=scenario["iam data"],
        system_model=metals.system_model,
    )

    validate.prim_sec_split = metals.prim_sec_split
    validate.interpolate_by_year = interpolate_by_year
    validate.metals_list = load_mining_shares_mapping()["Metal"].unique().tolist()
    validate.run_metals_checks()

    return scenario


def load_metals_alternative_names():
    """
    Load dataframe with alternative names for metals
    """

    filepath = DATA_DIR / "metals" / "transport_activities_mapping.yaml"

    with open(filepath, "r", encoding="utf-8") as stream:
        out = yaml.safe_load(stream)

    # this dictionary has lists as values

    # create a reversed dictionary where
    # the keys are the alternative names
    # and the values are the metals

    rev_out = {}

    for k, v in out.items():
        for i in v:
            rev_out[i] = k

    return rev_out


def load_metals_transport():
    """
    Load dataframe with metals transport
    """

    filepath = DATA_DIR / "metals" / "transport_markets_data.csv"
    df = pd.read_csv(filepath, sep=",")

    # remove rows without values under Weighted Average Distance
    df = df.loc[~df["Weighted Distance (km)"].isnull()]
    # remove rows with value 0 under Weighted Average Distance
    df = df.loc[df["Weighted Distance (km)"] != 0]

    df["country"] = df["Country"]

    return df


def load_mining_shares_mapping():
    """
    Load mapping between mining shares from the different sources and ecoinvent
    """

    filepath = DATA_DIR / "metals" / "mining_shares_mapping.xlsx"
    df = pd.read_excel(filepath, sheet_name="Shares_mapping")

    # replace all instances of "Year " in columns by ""
    df.columns = df.columns.str.replace("Year ", "")

    # remove suppliers whose markets share is below the cutoff
    cut_off = 0.01

    df_filtered = df.loc[df.loc[:, "2020":"2030"].max(axis=1) > cut_off].copy()

    # Normalize remaining data back to 100% for each metal
    years = [str(year) for year in range(2020, 2031)]
    for metal in df_filtered["Metal"].unique():
        metal_indices = df_filtered["Metal"] == metal
        df_filtered.loc[:, years] = df_filtered.groupby("Metal")[years].transform(
            lambda x: x / x.sum()
        )

    return df


def load_primary_secondary_split():
    """
    Load mapping for primary and secondary split of metal markets.
    """
    path = DATA_DIR / "metals" / "primary_secondary_split.yaml"
    with open(path, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def load_secondary_activity_routes():
    """
    Load mapping for secondary activity routes.
    """
    path = DATA_DIR / "metals" / "secondary_supply_activities.yaml"
    with open(path, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream) or {}


def load_activities_mapping():
    """
    Load mapping for the ecoinvent exchanges to be
    updated by the new metal intensities. Only rows
    where filter was set to yes are considered.
    """

    filepath = DATA_DIR / "metals" / "metal_products.xlsx"
    df = pd.read_excel(filepath, sheet_name="activities_mapping")
    df = df.loc[(df["filter"] == "Yes") | (df["filter"] == "yes")]

    return df


# Define a function to replicate rows based on the generated activity sets
def extend_dataframe(df, mapping):
    """ "
     Extend a DataFrame by duplicating rows based on a mapping dictionary.

    Parameters:
    - df (pd.DataFrame): The original DataFrame to be extended.
    - mapping (dict): A dictionary with keys corresponding to the 'technology'
                      values in the DataFrame and values that are sets of processes.
    """

    new_rows = []

    for key, processes in mapping.items():
        # Find the rows in the DataFrame where the 'technology' matches the key
        matching_rows = df[df["technology"] == key]
        # For each process in the set associated with the key, duplicate the matching rows
        for process in processes:
            temp_rows = matching_rows.copy()
            temp_rows["ecoinvent_technology"] = process["name"]
            new_rows.extend(temp_rows.to_dict("records"))
    new_df = pd.DataFrame(new_rows)

    return new_df


def get_ecoinvent_metal_factors():
    """
    Load dataframe with ecoinvent factors for metals
    and convert to xarray
    """

    filepath = DATA_DIR / "metals" / "ecoinvent_factors.csv"
    df = pd.read_csv(filepath)

    # create column "activity" as a tuple
    # of the columns "name", "product" and "location".
    df["activity"] = list(zip(df["name"], df["product"], df["location"]))
    df = df.drop(columns=["name", "product", "location"])
    df = df.melt(id_vars=["activity"], var_name="metal", value_name="value")

    # create an xarray with dimensions activity, metal and year
    ds = df.groupby(["activity", "metal"]).sum()["value"].to_xarray()

    return ds


def load_post_allocation_correction_factors():
    """
    Load yaml file with post-allocation_correction factors

    """

    filepath = DATA_DIR / "metals" / "post-allocation_correction" / "corrections.yaml"
    with open(filepath, "r", encoding="utf-8") as stream:
        factors = yaml.safe_load(stream)
    return factors


def fetch_mapping(filepath: str) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, "r", encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


def rev_metals_map(mapping: dict) -> dict:
    """Returns a reversed dictionary"""

    rev_mapping = {}
    for key, val in mapping.items():
        for v in val:
            rev_mapping[v["name"]] = key
    return rev_mapping


def load_conversion_factors():
    """
    Load dataframe with conversion factors for metals
    """

    filepath = DATA_DIR / "metals" / "conversion_factors.xlsx"
    df = pd.read_excel(filepath, sheet_name="Conversion factors")
    return df


def update_exchanges(
    activity: dict,
    new_amount: float,
    new_provider: dict,
    metal: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> dict:
    """
    Update exchanges for a given activity.

    :param activity: Activity to update
    :param new_amount: Result of the calculation
    :param new_provider: Dataset of the new provider
    :param metal: Metal name
    :param min_value: Minimum value for uncertainty
    :param max_value: Maximum value for uncertainty
    :return: Updated activity
    """
    # fetch old amount
    old_amount = sum(
        exc["amount"]
        for exc in activity["exchanges"]
        if exc.get("product", "").lower() == new_provider["reference product"].lower()
        and exc.get("type") == "technosphere"
    )

    activity["exchanges"] = [
        e
        for e in activity["exchanges"]
        if e.get("product", "").lower() != new_provider["reference product"].lower()
    ]

    new_exchange = {
        "amount": new_amount,
        "product": new_provider["reference product"],
        "name": new_provider["name"],
        "unit": new_provider["unit"],
        "location": new_provider["location"],
        "type": "technosphere",
        "uncertainty type": 0,  # assumes no uncertainty
    }

    if min_value is not None and max_value is not None:
        if min_value != max_value:
            if min_value <= new_amount <= max_value:
                new_exchange.update(
                    {
                        "uncertainty type": 5,
                        "loc": new_amount,
                        "minimum": min_value,
                        "maximum": max_value,
                        "preserve uncertainty": True,
                    }
                )
            else:
                print(
                    f"Value {new_amount} outside of range {min_value} - {max_value} for {metal} in {activity['name']}"
                )

    activity["exchanges"].append(new_exchange)

    # Log changes
    activity.setdefault("log parameters", {})
    activity["log parameters"].setdefault("old amount", {}).update({metal: old_amount})
    activity["log parameters"].setdefault("new amount", {}).update({metal: new_amount})

    return activity


def filter_technology(dataset_names, database):
    return list(
        ws.get_many(
            database,
            ws.either(*[ws.contains("name", name) for name in dataset_names]),
        )
    )


def build_ws_filter(field: str, query: dict):
    """
    Given a field and a query like {'contains': 'foo'}, return a filter function.
    """

    if not isinstance(query, list):
        queries = [query]
    else:
        queries = query

    filters = []

    for query in queries:
        for operator, value in query.items():
            if value == "":
                continue

            if operator == "contains":
                filters.append(ws.contains(field, value))
            elif operator == "equals":
                filters.append(ws.equals(field, value))
            elif operator == "startswith":
                filters.append(ws.startswith(field, value))
            elif operator == "all":
                for q in value:
                    filters += build_ws_filter(field, q)

            elif operator == "either":
                res = []
                for q in value:
                    res += build_ws_filter(field, q)
                if res:
                    filters.append(ws.either(*res))

            else:
                raise ValueError(
                    f"Unsupported operator {operator} for field {field} in query {query}"
                )

    if not filters:
        raise ValueError(f"No valid filters provided for field {field}")

    return filters


def interpolate_by_year(target_year: int, data: dict) -> float:
    """
    Interpolate (or extrapolate) a value for the given `target_year`
    from a dictionary like {2020: 0.1, 2030: 0.5, ...}.
    """
    data = {int(k): v for k, v in data.items()}
    years = sorted(data)

    if target_year in years:
        return data[target_year]
    elif target_year < years[0]:
        return data[years[0]]
    elif target_year > years[-1]:
        return data[years[-1]]
    else:
        for i in range(len(years) - 1):
            y0, y1 = years[i], years[i + 1]
            if y0 < target_year < y1:
                v0, v1 = data[y0], data[y1]
                return v0 + (v1 - v0) * (target_year - y0) / (y1 - y0)


class Metals(BaseTransformation):
    """
    Class that modifies metal demand of different technologies
    according to the Database built
    """

    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        cache: dict = None,
        index: dict = None,
    ):
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
            cache,
            index,
        )

        self.country_codes = {}
        self.version = version

        self.metals = iam_data.metals_intensity_factors  # 1
        # Precompute the median values for each metal and origin_var for the year 2020
        if self.year in self.metals.coords["year"].values:
            self.precomputed_medians = self.metals.sel(year=self.year)
        else:
            self.precomputed_medians = self.metals.interp(
                year=self.year, method="nearest", kwargs={"fill_value": "extrapolate"}
            )

        self.activities_mapping = load_activities_mapping()  # 4

        self.conversion_factors = load_conversion_factors()  # 3
        # Precompute conversion factors as a dictionary for faster lookups
        self.conversion_factors_dict = self.conversion_factors.set_index("Activity")[
            "Conversion_factor"
        ].to_dict()

        inv = InventorySet(self.database, self.version)

        self.activities_metals_map: Dict[str, Set] = (  # 2
            inv.generate_metals_activities_map()
        )

        self.rev_activities_metals_map: Dict[str, str] = rev_metals_map(
            self.activities_metals_map
        )

        self.extended_dataframe = extend_dataframe(
            self.activities_mapping, self.activities_metals_map
        )
        self.extended_dataframe["final_technology"] = self.extended_dataframe.apply(
            lambda row: (
                row["demanding_process"]
                if pd.notna(row["demanding_process"]) and row["demanding_process"] != ""
                else row["ecoinvent_technology"]
            ),
            axis=1,
        )

        self.biosphere_flow_codes = biosphere_flows_dictionary(version=self.version)
        self.metals_transport = load_metals_transport()
        self.alt_names = load_metals_alternative_names()

        self.metals_transport_activities = {
            "air": ws.get_one(
                self.database,
                ws.contains("name", "aircraft"),
                ws.contains("name", "freight"),
                ws.contains("name", "belly"),
                ws.contains("name", "long haul"),
                ws.contains("name", "market"),
                ws.equals("location", "GLO"),
            ),
            "sea": ws.get_one(
                self.database,
                ws.contains("name", "transport"),
                ws.contains("name", "freight"),
                ws.contains("name", "sea"),
                ws.contains("name", "container ship"),
                ws.contains("name", "market"),
                ws.exclude(ws.contains("name", "reefer")),
                ws.equals("location", "GLO"),
            ),
            "railway": ws.get_one(
                self.database,
                ws.contains("name", "transport"),
                ws.contains("name", "freight"),
                ws.contains("name", "train"),
                ws.contains("name", "market"),
                ws.exclude(ws.contains("name", "reefer")),
                ws.equals("location", "GLO"),
            ),
            "road": ws.get_one(
                self.database,
                ws.contains("name", "transport"),
                ws.contains("name", "freight"),
                ws.contains("name", "lorry"),
                ws.contains("name", "market"),
                ws.contains("name", "unspecified"),
                ws.exclude(ws.contains("name", "reefer")),
                ws.equals("location", "GLO"),
            ),
        }

        self.build_db_indexes()

        self.weighted_transport_distances = {
            (row["country"], row["Metal"]): row
            for _, row in self.metals_transport.iterrows()
        }

        self.transport_lookup = {
            (row["country"], row["Metal"]): row
            for _, row in self.metals_transport.iterrows()
        }

        self.prim_sec_split = load_primary_secondary_split()
        self.secondary_activity_routes = load_secondary_activity_routes()

    def update_metals_use_in_database(self):
        """
        Update the database with metals use factors.
        """

        for dataset in self.database:
            if dataset["name"] in self.rev_activities_metals_map:
                origin_var = self.rev_activities_metals_map[dataset["name"]]
                self.update_metal_use(dataset, origin_var)

    @lru_cache()
    def get_metal_market_dataset(self, metal_activity_name: str):
        if pd.notna(metal_activity_name) and isinstance(metal_activity_name, str):
            metal_markets = list(
                ws.get_many(
                    self.database,
                    ws.equals("name", metal_activity_name),
                    ws.either(
                        *[ws.equals("location", loc) for loc in ["World", "GLO", "RoW"]]
                    ),
                )
            )
            metal_markets = [ds for ds in metal_markets if self.is_in_index(ds)]
            if metal_markets:
                return metal_markets[0]
            else:
                raise ws.NoResults(
                    f"Could not find dataset for metal market {metal_activity_name}"
                )
        else:
            raise ValueError(f"Invalid metal activity name: {metal_activity_name}")

    def update_metal_use(
        self,
        dataset: dict,
        technology: str,
    ) -> None:
        """
        Update metal use based on metal intensity data.
        :param dataset: dataset to adjust metal use for
        :param technology: metal intensity variable name to look up
        :return: Does not return anything. Modified in place.
        """

        # Pre-fetch relevant data to minimize DataFrame operations
        tech_rows = self.extended_dataframe.loc[
            self.extended_dataframe["ecoinvent_technology"] == dataset["name"]
        ]

        if tech_rows.empty:
            logger.warning(
                f"No matching rows for {dataset['name']}, {dataset['location']}."
            )
            return

        conversion_factor = self.conversion_factors_dict.get(
            tech_rows["ecoinvent_technology"].iloc[0], None
        )
        available_metals = (
            self.precomputed_medians.sel(origin_var=technology)
            .dropna(dim="metal", how="all")["metal"]
            .values
        )

        unique_final_technologies = tech_rows["final_technology"].unique()

        for final_technology in unique_final_technologies:

            demanding_process_rows = tech_rows[
                (tech_rows["final_technology"] == final_technology)
                & tech_rows["demanding_process"].notna()
            ]

            if not demanding_process_rows.empty:
                for index, row in demanding_process_rows.iterrows():
                    self.process_metal_update(
                        metal_row=row,
                        dataset=dataset,
                        conversion_factor=conversion_factor,
                        final_technology=final_technology,
                        technology=technology,
                    )
            else:
                tech_specific_rows = tech_rows[
                    tech_rows["final_technology"] == final_technology
                ]
                for metal in available_metals:
                    if metal in tech_specific_rows["Element"].values:
                        match = tech_specific_rows[
                            tech_specific_rows["Element"] == metal
                        ]
                        if not match.empty:
                            metal_row = match.iloc[0]
                            self.process_metal_update(
                                metal_row=metal_row,
                                dataset=dataset,
                                conversion_factor=conversion_factor,
                                final_technology=final_technology,
                                technology=technology,
                            )

    def process_metal_update(
        self, metal_row, dataset, final_technology, technology, conversion_factor
    ):
        """
        Process the update for a given metal and technology.
        """
        conversion_factor = conversion_factor or 1
        unit_converter = metal_row.get("unit_convertor")
        metal_activity_name = metal_row["Activity"]

        if pd.notna(unit_converter) and pd.notna(metal_activity_name):
            use_factors = self.precomputed_medians.sel(
                metal=metal_row["Element"], origin_var=technology
            )
            median_value = (
                use_factors.sel(variable="median").item()
                * unit_converter
                * conversion_factor
            )

            min_value = (
                use_factors.sel(variable="min").item()
                * unit_converter
                * conversion_factor
            )
            max_value = (
                use_factors.sel(variable="max").item()
                * unit_converter
                * conversion_factor
            )

            if median_value != 0 and not np.isnan(median_value):
                try:
                    dataset_metal = self.get_metal_market_dataset(metal_activity_name)
                except ws.NoResults:
                    return

                metal_users = self.db_index_by_name.get(final_technology, [])
                for metal_user in metal_users:
                    update_exchanges(
                        activity=metal_user,
                        new_amount=median_value,
                        new_provider=dataset_metal,
                        metal=metal_row["Element"],
                        min_value=min_value,
                        max_value=max_value,
                    )
                    self.write_log(metal_user, "updated")
        else:
            print(
                f"Warning: Missing data for {metal_row['Element']} for {dataset['name']}:"
            )
            if pd.isna(unit_converter):
                print("- unit converter")
            if pd.isna(metal_activity_name):
                print("- activity name")

    def post_allocation_correction(self):
        """
        Correct for post-allocation in the database.
        """

        factors_list = load_post_allocation_correction_factors()

        for dataset in factors_list:
            filters = [
                ws.equals("name", dataset["name"]),
                ws.equals("reference product", dataset["reference product"]),
                ws.equals("unit", dataset["unit"]),
            ]

            if "location" in dataset:
                filters.append(ws.equals("location", dataset["location"]))

            for ds in ws.get_many(
                self.database,
                *filters,
            ):
                for flow in dataset["additional flow"]:
                    found = False
                    for exc in ws.biosphere(
                        ds,
                        ws.equals("name", flow["name"]),
                        ws.equals("categories", tuple(flow["categories"].split("::"))),
                    ):
                        exc["amount"] += flow["amount"]
                        found = True

                    if not found:
                        flow_key = (
                            flow["name"],
                            flow["categories"].split("::")[0],
                            flow["categories"].split("::")[1],
                            flow["unit"],
                        )

                        if flow_key in self.biosphere_flow_codes:
                            flow_code = (
                                "biosphere3",
                                self.biosphere_flow_codes[flow_key],
                            )
                        else:
                            # try with ", in ground"
                            new_name = flow["name"] + ", in ground"
                            flow_key = (
                                new_name,
                                flow["categories"].split("::")[0],
                                flow["categories"].split("::")[1],
                                flow["unit"],
                            )
                            if flow_key in self.biosphere_flow_codes:
                                flow_code = (
                                    "biosphere3",
                                    self.biosphere_flow_codes[flow_key],
                                )
                            else:
                                print(
                                    f"Warning: Flow {flow_key} not found in biosphere flows."
                                )
                                continue

                        ds["exchanges"].append(
                            {
                                "name": flow["name"],
                                "amount": flow["amount"],
                                "unit": flow["unit"],
                                "type": "biosphere",
                                "categories": tuple(flow["categories"].split("::")),
                                "input": flow_code,
                            }
                        )

                for flow in dataset["additional flow"]:
                    ds.setdefault("log parameters", {})[
                        "post-allocation correction"
                    ] = flow["amount"]

                self.write_log(ds, "updated")

    def get_shares(
        self, df: pd.DataFrame, new_locations: dict, name, ref_prod, normalize=True
    ) -> dict:
        """
        Get shares of each location in the dataframe.
        :param df: Dataframe with mining shares
        :param new_locations: List of new locations
        :return: Dictionary with shares of each location
        """
        shares = {}

        # we fetch the shares for each location in df
        # and we interpolate if necessary between the columns
        # 2020 to 2030

        for long_location, short_location in new_locations.items():
            share = df.loc[df["Country"] == long_location, "2020":"2030"]
            if len(share) > 0:

                # we interpolate depending on if self.year is between 2020 and 2030
                # otherwise, we back or forward fill

                if self.year < 2020:
                    share = share.iloc[:, 0]
                elif self.year > 2030:
                    share = share.iloc[:, -1]
                else:
                    share = share.iloc[:, self.year - 2020]

                share = share.values[0]
                shares[(name, ref_prod, short_location)] = share

        # filter-out shares that are below 1% and normalize the rest
        shares = {k: v for k, v in shares.items() if v >= 0.01}
        if not shares:
            return {}

        if normalize:
            total = sum(shares.values())
            shares = {k: v / total for k, v in shares.items()}

        return shares

    def get_geo_mapping(self, df: pd.DataFrame, new_locations: dict) -> dict:
        mapping = {}

        regions_df = df[["Country", "Region"]].drop_duplicates()
        for long_loc, iso2 in new_locations.items():
            region_row = regions_df.loc[regions_df["Country"] == long_loc]
            if not region_row.empty:
                mapping[iso2] = region_row["Region"].iloc[0]

        return mapping

    def build_db_indexes(self):
        self.db_index_by_name = defaultdict(list)
        self.db_index = defaultdict(lambda: defaultdict(list))
        self.db_index_full = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for ds in self.database:
            name = ds.get("name")
            ref_prod = ds.get("reference product")
            location = ds.get("location")

            self.db_index_by_name[name].append(ds)
            self.db_index[name][ref_prod].append(ds)
            self.db_index_full[name][ref_prod][location].append(ds)

    def create_region_specific_markets(self, df: pd.DataFrame) -> List[dict]:
        new_exchanges, new_datasets = [], []

        df["Process_str"] = df["Process"].apply(str)
        df["Reference_product_str"] = df["Reference product"].apply(str)

        for (_, _), group in df.groupby(["Process_str", "Reference_product_str"]):
            proc_filter = eval(group["Process"].iloc[0])
            ref_prod_filter = eval(group["Reference product"].iloc[0])

            try:
                filters = build_ws_filter("name", proc_filter) + build_ws_filter(
                    "reference product", ref_prod_filter
                )
                subset = list(ws.get_many(self.database, *filters))

            except Exception as e:
                logger.error(
                    f"[Metals] Error fetching datasets for process '{proc_filter}' and reference product '{ref_prod_filter}': {e}"
                )
                print(
                    f"failed with process '{proc_filter}' and reference product '{ref_prod_filter}"
                )
                continue

            if not subset:
                logger.warning(
                    f"[Metals] No datasets found for filter combination:\n"
                    f"  name: {proc_filter}\n"
                    f"  reference product: {ref_prod_filter}"
                )
                continue

            first_match = subset[0]
            name = first_match["name"]
            ref_prod = first_match["reference product"]

            new_locations = {
                c: self.country_codes[c] for c in group["Country"].unique()
            }

            # fetch shares for each location in df
            # Do not normalize yet - THIS WAS CAUSING A DOUBLE NORMALIZATION AND BREAKING THINGS!
            shares = self.get_shares(
                group, new_locations, name, ref_prod, normalize=False
            )
            geography_mapping = self.get_geo_mapping(group, new_locations)

            # if not, we create it
            datasets = self.create_new_mining_activity(
                name=name,
                reference_product=ref_prod,
                new_locations=new_locations,
                geography_mapping=geography_mapping,
            )

            new_datasets.extend(datasets.values())

            new_exchanges.extend(
                [
                    {
                        "name": k[0],
                        "product": k[1],
                        "location": k[2],
                        "unit": "kilogram",
                        "amount": share,
                        "type": "technosphere",
                    }
                    for k, share in shares.items()
                ]
            )

        # Normalize shares to sum to 1
        total = sum(exc["amount"] for exc in new_exchanges)
        if total > 0:
            for exc in new_exchanges:
                exc["amount"] /= total

        for dataset in new_datasets:
            self.database.append(dataset)
            self.add_to_index(dataset)
            self.write_log(dataset, "created")

        return new_exchanges

    def create_market(self, metal, df) -> Optional[dict]:
        """
        Create regionalized technosphere exchanges for a metal market based on production shares.

        This function reads a DataFrame containing information on mining activity filters
        (process and reference product), countries, and associated production shares.
        For each unique combination of process and reference product filters, it:
        - Finds matching datasets in the database using flexible filter logic.
        - Derives geographic mappings and production shares.
        - Creates new mining activities for countries where no dataset exists yet.
        - Appends these to the database and builds the corresponding technosphere exchanges.

        The function also normalizes all generated exchanges to sum to 1.

        Args:
            df (pd.DataFrame): A dataframe containing the following columns:
                - 'Process': dict defining filter(s) for activity names.
                - 'Reference product': dict defining filter(s) for reference products.
                - 'Country': country names to create new datasets for.
                - Production share columns per year (e.g., '2020', '2030', ...)
        """
        # check if market already exists
        # if so, remove it

        primary_exchanges = self.create_region_specific_markets(df)

        if not primary_exchanges:
            logger.warning(
                f"[Metals] No primary exchanges created for {metal}. Skipping market creation."
            )
            return None

        ref_prod = metal
        market_name = f"market for {ref_prod}"

        dataset = {
            "name": market_name,
            "location": "World",
            "exchanges": [
                {
                    "name": market_name,
                    "product": ref_prod,
                    "location": "World",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                }
            ],
            "reference product": ref_prod,
            "unit": "kilogram",
            "production amount": 1,
            "comment": "Created by premise",
            "code": str(uuid.uuid4()),
        }

        _, _, p_share, s_share = self.get_market_split_shares(metal)

        # add mining exchanges
        # dataset["exchanges"].extend(self.create_region_specific_markets(df))
        for exc in primary_exchanges:
            exc["amount"] *= p_share
        dataset["exchanges"].extend(primary_exchanges)

        # Add burden-free secondary market exchange
        secondary_exchanges = self.build_secondary_market_exchanges(
            dataset["reference product"], s_share
        )
        if secondary_exchanges:
            dataset["exchanges"].extend(secondary_exchanges)

        # add transport exchanges
        trspt_exc = self.add_transport_to_market(dataset, metal)
        for exc in trspt_exc:
            exc["amount"] *= p_share
        if len(trspt_exc) > 0:
            dataset["exchanges"].extend(trspt_exc)

        # # filter out None
        dataset["exchanges"] = [
            exc
            for exc in dataset["exchanges"]
            if self.activity_exists(exc) or exc["type"] == "production"
        ]

        # remove old market dataset
        for old_market in ws.get_many(
            self.database,
            ws.equals("name", dataset["name"]),
            ws.equals("reference product", dataset["reference product"]),
            ws.exclude(ws.equals("location", "World")),
        ):
            self.remove_from_index(old_market)
            assert (
                self.is_in_index(old_market) is False
            ), f"Market {(old_market['name'], old_market['reference product'], old_market['location'])} still in index"

        return dataset

    def activity_exists(self, exchange: dict) -> bool:
        """Check if an activity referenced by an exchange exists in the database."""
        if exchange.get("type") != "technosphere":
            return True

        activities = list(
            ws.get_many(
                self.database,
                ws.equals("name", exchange["name"]),
                ws.equals("reference product", exchange["product"]),
                ws.equals("location", exchange["location"]),
            )
        )

        return len(activities) > 0

    def substitute_old_markets(self, new_dataset: dict, df_metal: pd.DataFrame) -> None:
        """
        Substitute all old market links with the new 'World' market for the given metal key.
        Uses flexible matching based on Reference product field in Excel.
        """
        old_ref_products = set()

        for _, row in df_metal.iterrows():
            ref_field = row.get("Reference product")
            if isinstance(ref_field, dict):
                if "either" in ref_field:
                    old_ref_products.update(
                        q.get("equals") for q in ref_field["either"] if "equals" in q
                    )
                elif "equals" in ref_field:
                    old_ref_products.add(ref_field["equals"])
            elif isinstance(ref_field, str):
                old_ref_products.add(ref_field)

        for ref_prod in old_ref_products:
            old_markets = list(
                ws.get_many(
                    self.database,
                    ws.equals("name", f"market for {ref_prod}"),
                    ws.equals("reference product", ref_prod),
                    ws.either(
                        ws.equals("location", "GLO"), ws.equals("location", "RoW")
                    ),
                )
            )

            for old_market in old_markets:
                consumers = [
                    ds
                    for ds in self.database
                    if any(
                        exc.get("type") == "technosphere"
                        and exc.get("name") == old_market["name"]
                        and exc.get("product") == old_market["reference product"]
                        and exc.get("location") == old_market["location"]
                        for exc in ds.get("exchanges", [])
                    )
                ]

                for consumer in consumers:
                    modified = False
                    for exc in consumer["exchanges"]:
                        if (
                            exc.get("name") == old_market["name"]
                            and exc.get("product") == old_market["reference product"]
                            and exc.get("location") == old_market["location"]
                            and exc.get("type") == "technosphere"
                        ):
                            exc.update(
                                {
                                    "name": new_dataset["name"],
                                    "product": new_dataset["reference product"],
                                    "location": new_dataset["location"],
                                }
                            )
                            modified = True

                    if modified:
                        self.write_log(consumer, "relinked to new metal market")

    def create_new_mining_activity(
        self,
        name: str,
        reference_product: str,
        new_locations: dict,
        geography_mapping: dict = None,
    ) -> dict:
        """
        Create new mining activities for specified locations if they do not exist.

        We try to use the geography_mapping to find the correct location (Region column) in the database.
        It falls back to any matching activity if the specified region does not work.
        """

        geo_map_filtered = {
            k: self.db_index_full[name][reference_product][v][0]
            for k, v in geography_mapping.items()
            if self.db_index_full[name][reference_product].get(v)
            and not self.is_in_index(
                {"name": name, "reference product": reference_product, "location": k}
            )
        }

        if (
            not geo_map_filtered
            and name in self.db_index_full
            and reference_product in self.db_index_full[name]
        ):
            # Get any available activity to use as proxy
            available_locations = list(
                self.db_index_full[name][reference_product].keys()
            )
            if available_locations:
                proxy_activity = self.db_index_full[name][reference_product][
                    available_locations[0]
                ][0]

                logger.warning(
                    f"Falling back to proxy activity for {name}, {reference_product}. "
                    f"Using location '{available_locations[0]}' for regions: "
                    f"{[k for k in new_locations.values() if not self.is_in_index({'name': name, 'reference product': reference_product, 'location': k})]}"
                )

                # Build geo_map_filtered with this proxy for all needed locations
                geo_map_filtered = {
                    k: proxy_activity
                    for k in new_locations.values()
                    if not self.is_in_index(
                        {
                            "name": name,
                            "reference product": reference_product,
                            "location": k,
                        }
                    )
                }

        if not geo_map_filtered:
            logger.error(
                f"Failed to create activities for {name} in locations: "
                f"{list(new_locations.values())}"
            )
            return {}

        datasets = self.db_index.get(name, {}).get(reference_product, [])

        return self.fetch_proxies(
            datasets=datasets,
            regions=new_locations.values(),
            geo_mapping=geo_map_filtered,
        )

    def add_transport_to_market(self, dataset, metal) -> list:

        origin_shares = {
            e["location"]: e["amount"]
            for e in dataset["exchanges"]
            if e["type"] == "technosphere"
        }

        exc_map = defaultdict(float)

        for c, share in origin_shares.items():
            if metal in self.alt_names:
                trspt_data = self.get_weighted_average_distance(c, metal)
                for _, row in trspt_data.iterrows():
                    key = (
                        self.metals_transport_activities[
                            row["TransportMode Label"].lower()
                        ]["name"],
                        self.metals_transport_activities[
                            row["TransportMode Label"].lower()
                        ]["reference product"],
                        "GLO",  # fixed location
                        self.metals_transport_activities[
                            row["TransportMode Label"].lower()
                        ]["unit"],
                    )
                    exc_map[key] += (row["Weighted Distance (km)"] / 1000) * share

        excs = [
            {
                "name": k[0],
                "product": k[1],
                "location": k[2],
                "unit": k[3],
                "type": "technosphere",
                "amount": v,
            }
            for k, v in exc_map.items()
        ]

        return excs

    def get_weighted_average_distance(self, country, metal):
        alt_metal = self.alt_names.get(metal)
        if not alt_metal:
            return pd.DataFrame()  # fallback

        rows = [
            row
            for (c, m), row in self.transport_lookup.items()
            if c == country and m == alt_metal
        ]

        return pd.DataFrame(rows)

    def create_metal_markets(self):
        self.post_allocation_correction()

        dataframe = load_mining_shares_mapping()
        dataframe = dataframe.loc[dataframe["Work done"] == "Yes"]
        dataframe = dataframe.loc[~dataframe["Country"].isnull()]

        dataframe_shares = dataframe

        self.country_codes.update(
            dict(
                zip(
                    dataframe["Country"].unique(),
                    coco.convert(dataframe["Country"].unique(), to="ISO2"),
                )
            )
        )

        # fix France (French Guiana) to GF
        self.country_codes["France (French Guiana)"] = "GF"

        grouped_metal_dfs = dict(tuple(dataframe_shares.groupby("Metal")))

        for metal, df_metal in grouped_metal_dfs.items():
            dataset = self.create_market(metal, df_metal)
            if dataset:
                self.database.append(dataset)
                self.add_to_index(dataset)
                self.write_log(dataset, "created")
                self.substitute_old_markets(new_dataset=dataset, df_metal=df_metal)

    def get_market_split_shares(self, metal_key: str) -> tuple[str, str, float, float]:
        """
        For a given metal_key (e.g., 'copper, cathod'), return:
        - market name
        - reference product
        - primary share (interpolated)
        - secondary share (interpolated)

        For consequential system model, always return 100% primary.
        """
        if self.system_model == "consequential":
            default_name = f"market for {metal_key}"
            default_reference_product = metal_key
            return default_name, default_reference_product, 1.0, 0.0

        entry = self.prim_sec_split.get(metal_key, None)
        if not entry:
            default_name = f"market for {metal_key}"
            default_reference_product = metal_key
            logger.warning(
                f"[Metals] WARNING: No entry found for metal key: {metal_key} in 'primary_secondary_split.yaml'"
            )
            return default_name, default_reference_product, 1.0, 0.0

        name = entry["name"]
        reference_product = entry["reference product"]

        p = interpolate_by_year(self.year, entry["shares"]["primary"])
        s = interpolate_by_year(self.year, entry["shares"]["secondary"])
        total = p + s

        if total > 1:
            logger.warning(
                f"[Metals] WARNING: Total shares for {metal_key} exceed 1: {total}. Normalizing."
            )

        return (
            name,
            reference_product,
            p / total if total > 0 else 0,  # Avoid division by zero
            s / total if total > 0 else 0,  # Avoid division by zero
        )

    def build_secondary_market_exchanges(self, metal: str, s_share: float) -> list:
        """
        Build technosphere exchanges from flexible secondary activity filters in YAML.
        """

        # Check if secondary supply data is missing when it shouldn't be
        if metal not in self.secondary_activity_routes:
            # Only warn if the split actually defines a non-zero secondary share
            entry = self.prim_sec_split.get(metal.lower())
            if entry:
                try:
                    s = interpolate_by_year(self.year, entry["shares"]["secondary"])
                    if s > 0:
                        logger.warning(
                            f"[Metals] Missing secondary supply activities for '{metal}' "
                            f"despite non-zero secondary share ({s})."
                        )
                except Exception:
                    logger.warning(
                        f"[Metals] Could not interpolate secondary share for '{metal}'."
                    )

            return []

        route_entries = self.secondary_activity_routes[metal]

        # 1. Interpolate shares and cache them
        entries_with_shares = []
        total_relative_share = 0

        for entry in route_entries:
            try:
                share = interpolate_by_year(self.year, entry["shares"])
                entries_with_shares.append((entry, share))
                total_relative_share += share
            except Exception:
                logger.warning(
                    f"[Metals] Failed to interpolate shares for {metal} in entry: {entry}"
                )
                continue

        if total_relative_share == 0:
            logger.warning(
                f"[Metals] Total relative share for {metal} is zero, no exchanges created."
            )
            return []

        exchanges = []

        # 2. Build secondary exchanges with normalized amount

        for entry, share in entries_with_shares:
            filters = []
            for field in ["name", "reference product", "location"]:
                if field in entry:
                    res = build_ws_filter(field, entry[field])
                    filters += res

            candidates = list(ws.get_many(self.database, *filters))

            if not candidates:
                logger.warning(f"[Metals] No candidates found for entry: {entry}")
                continue
            if len(candidates) > 1:
                logger.warning(
                    f"[Metals] Multiple candidates found for entry: {entry}, using the first one."
                )

            ds = candidates[0]

            exchanges.append(
                {
                    "name": ds["name"],
                    "product": ds["reference product"],
                    "location": ds["location"],
                    "amount": s_share
                    * (
                        share / total_relative_share
                    ),  # Normalize by total relative share
                    "type": "technosphere",
                    "unit": ds["unit"],
                }
            )

        return exchanges

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        txt = (
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['reference product']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('post-allocation correction', '')}|"
            f"{dataset.get('log parameters', {}).get('old amount', '')}|"
            f"{dataset.get('log parameters', {}).get('new amount', '')}"
        )

        logger.info(txt)
