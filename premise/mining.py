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


def copy_exchange(exc: dict) -> dict:
    """
    Create a shallow copy of an exchange with the most relevant fields.
    """
    return {
        k: exc[k]
        for k in ["name", "product", "amount", "unit", "type", "location"]
        if k in exc
    }


def copy_activity(act: dict, location: str, product_name=None) -> dict:
    new_act = copy.deepcopy(act)
    new_act["location"] = location
    new_act["code"] = str(uuid.uuid4())

    if product_name:
        new_act["reference product"] = product_name

    has_production = (
        sum(e for e in new_act.get("exchanges", []) if e.get("type") == "production")
        > 0
    )

    if has_production:
        for exc in new_act.get("exchanges", []):
            if exc["type"] == "production":
                exc["name"] = new_act["name"]
                if product_name:
                    exc["product"] = product_name
                exc["location"] = location

    else:
        # Add production exchange if none exists
        new_act.setdefault("exchanges", []).append(
            {
                "type": "production",
                "name": new_act["name"],
                "product": new_act["reference product"],
                "amount": 1,
                "unit": new_act.get("unit", "kilogram"),
                "location": location,
            }
        )

    return new_act


def has_exchange_matching(activity, name_filters, prod_filters) -> bool:
    """
    Return True if the activity has at least one technosphere exchange that
    matches name_filters and prod_filters.
    """

    name_fltr = name_filters.get("fltr", [])
    name_mask = name_filters.get("mask", [])
    product_fltr = prod_filters.get("fltr", [])
    product_mask = prod_filters.get("mask", [])

    for exc in activity.get("exchanges", []):
        if exc.get("type") != "technosphere":
            continue
        if name_fltr and not any(f in exc.get("name", "").lower() for f in name_fltr):
            continue
        if name_mask and any(f in exc.get("name", "").lower() for f in name_mask):
            continue
        if product_fltr and not any(
            f in exc.get("product", "").lower() for f in product_fltr
        ):
            continue
        if product_mask and any(
            f in exc.get("product", "").lower() for f in product_mask
        ):
            continue
        return True
    return False


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

    def _split_exchanges(self, act, config, shares):

        original_cfg = config.get("impoundment", {})
        name_fltr = original_cfg.get("name", {})
        prod_fltr = original_cfg.get("reference product", {})

        impoundement_datasets = self.mining_map["sulfidic tailings - impoundment"]

        new_exchanges = []
        for exc in act["exchanges"]:
            if exc.get("type") != "technosphere":
                new_exchanges.append(exc)
                continue
            if exc not in impoundement_datasets:
                new_exchanges.append(exc)
                continue

            total = exc["amount"]
            for method, share in shares.items():
                if share == 0:
                    continue
                if method == "impoundment":
                    keep_exc = copy_exchange(exc)
                    keep_exc["amount"] = total * share
                    new_exchanges.append(keep_exc)
                else:
                    provider = self._get_or_create_provider(
                        method, act["location"], exc["product"]
                    )
                    if provider:
                        new_exc = copy_exchange(exc)
                        new_exc["name"] = provider["name"]
                        new_exc["product"] = provider["reference product"]
                        new_exc["location"] = provider["location"]
                        new_exc["amount"] = total * share
                        new_exchanges.append(new_exc)

        act["exchanges"] = new_exchanges

    def _get_or_create_provider(self, method, location, product):
        cfg = self.tailings_shares.get("sulfidic tailings", {}).get(method, {})
        name_filters = cfg.get("name", {})
        prod_filters = cfg.get("reference product", {})

        name_fltr = [ws.contains("name", s) for s in name_filters.get("fltr", [])]
        name_mask = [
            ws.exclude(ws.contains("name", s)) for s in name_filters.get("mask", [])
        ]
        prod_fltr = [
            ws.contains("reference product", s) for s in prod_filters.get("fltr", [])
        ]
        prod_mask = [
            ws.exclude(ws.contains("reference product", s))
            for s in prod_filters.get("mask", [])
        ]

        providers = list(
            ws.get_many(
                self.database,
                *name_fltr,
                *prod_fltr,
                *name_mask,
                *prod_mask,
                ws.equals("location", location),
            )
        )
        if providers:
            return providers[0]

        fallback = list(
            ws.get_many(
                self.database,
                *name_fltr,
                *prod_fltr,
                *name_mask,
                *prod_mask,
                ws.equals("location", "GLO"),
            )
        )
        if not fallback:
            logger.warning(
                f"[Mining] No fallback found for method {method} at {location}"
            )
            return None

        new_provider = copy_activity(fallback[0], location, product)
        self.database.append(new_provider)
        self.add_to_index(new_provider)
        self.write_log(new_provider, "created")
        return new_provider

    def update_tailings_treatment(self):

        processed_datasets = []

        for waste_management_type, activities in self.mining_map.items():
            for activity in activities:

                regionalized_datasets = self.fetch_proxies(
                    name=activity,
                    ref_prod="",
                )

                regionalized_datasets = {
                    k: v
                    for k, v in regionalized_datasets.items()
                    if k in self.tailings_shares.region.values
                }

                processed_datasets.extend(regionalized_datasets.values())

        for dataset in processed_datasets:
            #print(f"[Mining] Adding {dataset['name']} in {dataset['location']}")
            self.add_to_index(dataset)
            self.write_log(dataset, "created")
            self.database.append(dataset)

        market_datasets = set(
            [
                ds["name"]
                for ds in ws.get_many(
                    self.database, ws.contains("name", "market for sulfidic tailings")
                )
            ]
        )

        processed_datasets = []
        for market_dataset in market_datasets:
            regionalized_datasets = self.fetch_proxies(
                name=market_dataset,
                ref_prod="",
            )

            regionalized_datasets = {
                k: v
                for k, v in regionalized_datasets.items()
                if k in self.tailings_shares.region.values
            }

            for region, market_dataset in regionalized_datasets.items():

                if self.year < self.tailings_shares.year.values.min():
                    year = self.tailings_shares.year.values.min()
                elif self.year > self.tailings_shares.year.values.max():
                    year = self.tailings_shares.year.values.max()
                else:
                    year = self.year
                shares = self.tailings_shares.sel(
                    region=region,
                ).interp(year=year)

                market_dataset["exchanges"] = [
                    e for e in market_dataset["exchanges"] if e["type"] == "production"
                ]

                for waste_management_type in shares.technology.values:
                    supplier = list(
                        ws.get_many(
                            self.database,
                            ws.either(
                                *[
                                    ws.contains("name", s)
                                    for s in self.mining_map[waste_management_type]
                                ]
                            ),
                            ws.equals("location", region),
                        )
                    )
                    if len(supplier) > 1:
                        if waste_management_type == "sulfidic tailings - impoundment":
                            # we have different datasets for impoundment
                            supplier = [
                                s
                                for s in supplier
                                if s["name"].split(", ")[-2] in market_dataset["name"]
                            ]
                        else:
                            print(
                                f"[Mining] More than one supplier found for {waste_management_type} in {region}"
                            )
                    if not supplier:
                        print(
                            f"[Mining] No supplier found for {waste_management_type} in {region}"
                        )
                        continue

                    supplier = supplier[0]

                    market_dataset["exchanges"].append(
                        {
                            "type": "technosphere",
                            "name": supplier["name"],
                            "product": supplier["reference product"],
                            "amount": shares.sel(technology=waste_management_type)[
                                "mean"
                            ].values.item(0),
                            "unit": supplier["unit"],
                            "location": supplier["location"],
                            "uncertainty type": 5,
                            "loc": shares.sel(technology=waste_management_type)[
                                "mean"
                            ].values.item(0),
                            "minimum": shares.sel(technology=waste_management_type)[
                                "min"
                            ].values.item(0),
                            "maximum": shares.sel(technology=waste_management_type)[
                                "max"
                            ].values.item(0),
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
