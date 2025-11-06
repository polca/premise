"""
Integrates projections regarding tailings treatment.
"""

import yaml
import copy
import uuid
import xarray as xr
import numpy as np
from collections import defaultdict
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    List,
    ws,
)
from .utils import DATA_DIR
from .logger import create_logger
from .geomap import Geomap
from .activity_maps import InventorySet

logger = create_logger("mining")

TAILINGS_REGIONS_FILE = DATA_DIR / "mining" / "tailings_topology.yaml"
TAILINGS_WASTE_SHARES = DATA_DIR / "mining" / "tailings_activities.yaml"


def _update_mining(scenario, version, system_model):
    mining = Mining(
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

    mining.update_tailings_treatment()
    mining.relink_datasets()
    scenario["database"] = mining.database
    scenario["cache"] = mining.cache
    scenario["index"] = mining.index

    return scenario


def load_tailings_config(model: str):
    """
    Load the tailings waste management shares.

    :param model: The IAM model name (e.g., "remind", "image").
    """

    with open(TAILINGS_WASTE_SHARES) as f:
        tech_data = yaml.safe_load(f)

    with open(TAILINGS_REGIONS_FILE) as f:
        region_map_raw = yaml.safe_load(f)

    def get_region_remap(region_map, model_name):
        remap = defaultdict(list)
        for std_region, models in region_map.items():
            remap[std_region] = models.get(model_name, [])
        return remap

    def create_dataset(data, model_name, region_map_raw):
        # Get model-specific region remapping
        region_remap = get_region_remap(region_map_raw, model_name)

        techs = []
        years = set()
        records = []

        for tech, tech_data in data.items():
            techs.append(tech)
            for raw_region, region_data in tech_data.get("share", {}).items():
                mapped_regions = region_remap.get(raw_region, None)
                if mapped_regions is None:
                    continue  # Skip unmatched regions
                for year, values in region_data.items():
                    years.add(int(year))
                    for mapped_region in mapped_regions:
                        records.append(
                            {
                                "technology": tech,
                                "region": mapped_region,
                                "year": int(year),
                                "min": values.get("min"),
                                "max": values.get("max"),
                                "mean": values.get("mean"),
                            }
                        )

        techs = sorted(set(techs))
        regions = sorted(set(r["region"] for r in records))
        years = sorted(years)

        # Build arrays
        min_data = np.full((len(techs), len(years), len(regions)), np.nan)
        max_data = np.full_like(min_data, np.nan)
        mean_data = np.full_like(min_data, np.nan)

        tech_idx = {t: i for i, t in enumerate(techs)}
        region_idx = {r: i for i, r in enumerate(regions)}
        year_idx = {y: i for i, y in enumerate(years)}

        for rec in records:
            i = tech_idx[rec["technology"]]
            j = year_idx[rec["year"]]
            k = region_idx[rec["region"]]
            min_data[i, j, k] = rec["min"]
            max_data[i, j, k] = rec["max"]
            mean_data[i, j, k] = rec["mean"]

        return xr.Dataset(
            {
                "min": (["technology", "year", "region"], min_data),
                "max": (["technology", "year", "region"], max_data),
                "mean": (["technology", "year", "region"], mean_data),
            },
            coords={
                "technology": techs,
                "year": years,
                "region": regions,
            },
        )

    return create_dataset(tech_data, model_name=model, region_map_raw=region_map_raw)


def group_dicts_by_keys(dicts: list, keys: list):
    groups = defaultdict(list)
    for d in dicts:
        group_key = tuple(d.get(k) for k in keys)
        groups[group_key].append(d)
    return list(groups.values())


class Mining(BaseTransformation):
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
        self.year = int(year)
        self.tailings_shares = load_tailings_config(model)
        self.geomap = Geomap(model)
        inv = InventorySet(database=database, version=version, model=model)
        self.mining_map = inv.generate_mining_waste_map()

    def update_tailings_treatment(self):

        processed_datasets = []

        for waste_management_type, activities in self.mining_map.items():

            activities = group_dicts_by_keys(activities, ["name", "reference product"])

            for activity in activities:
                regionalized_datasets = self.fetch_proxies(datasets=activity)

                regionalized_datasets = {
                    k: v
                    for k, v in regionalized_datasets.items()
                    if k in self.tailings_shares.region.values
                }

                processed_datasets.extend(regionalized_datasets.values())

        for dataset in processed_datasets:
            self.add_to_index(dataset)
            self.write_log(dataset, "created")
            self.database.append(dataset)

        market_datasets = ws.get_many(
            self.database, ws.contains("name", "market for sulfidic tailings")
        )

        market_datasets = group_dicts_by_keys(
            market_datasets, keys=["name", "reference product"]
        )

        processed_datasets = []

        for market_dataset in market_datasets:
            regionalized_datasets = self.fetch_proxies(datasets=market_dataset)

            regionalized_datasets = {
                k: v
                for k, v in regionalized_datasets.items()
                if k in self.tailings_shares.region.values
            }

            for region, dataset in regionalized_datasets.items():
                if self.year < self.tailings_shares.year.values.min():
                    year = self.tailings_shares.year.values.min()
                elif self.year > self.tailings_shares.year.values.max():
                    year = self.tailings_shares.year.values.max()
                else:
                    year = self.year
                shares = self.tailings_shares.sel(
                    region=region,
                ).interp(year=int(year))

                dataset["exchanges"] = [
                    e for e in dataset["exchanges"] if e["type"] == "production"
                ]

                for waste_management_type, waste_activities in self.mining_map.items():

                    if len(waste_activities) > 1:
                        if waste_management_type == "sulfidic tailings - impoundment":
                            # we have different datasets for impoundment
                            waste_activities = [
                                s
                                for s in waste_activities
                                if s["name"].split(", ")[-2] in dataset["name"]
                            ]
                        else:
                            print(
                                f"[Mining] More than one supplier found for {waste_management_type} in {region}"
                            )
                    if len(waste_activities) == 0:
                        print(
                            f"[Mining] No supplier found for {waste_management_type} in {region}"
                        )
                        continue

                    supplier = waste_activities[0]

                    dataset["exchanges"].append(
                        {
                            "type": "technosphere",
                            "name": supplier["name"],
                            "product": supplier["reference product"],
                            "amount": shares.sel(technology=waste_management_type)[
                                "mean"
                            ].values.item(0)
                            * -1,
                            "unit": supplier["unit"],
                            "location": dataset["location"],
                            "uncertainty type": 0,
                        }
                    )

            processed_datasets.extend(regionalized_datasets.values())

        for dataset in processed_datasets:
            self.add_to_index(dataset)
            self.write_log(dataset, "created")
            self.database.append(dataset)


def write_log(self, dataset, status="updated"):
    txt = f"{status}|{self.model}|{self.scenario}|{self.year}|{dataset['name']}|{dataset.get('reference product', '')}|{dataset['location']}"
    logger.info(txt)
