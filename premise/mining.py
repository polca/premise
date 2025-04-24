"""
Integrates projections regarding tailings treatment.
"""

import yaml
import copy
import uuid
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

logger = create_logger("mining")

TAILINGS_REGIONS_FILE = DATA_DIR / "mining" / "tailings_topology.yaml"

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


def load_tailings_config():
    """
    Load the tailings configuration file.
    """

    filepath = DATA_DIR / "mining" / "tailing_activities.yaml"
    with open(filepath, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)

def load_tailings_topology():
    """
    Load the mapping of tailings regions to the IAM regions
    """
    with open(TAILINGS_REGIONS_FILE, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def interpolate_method_share(year: int, share_dict: dict) -> float:
    """
    Interpolates the method share for a given year and a given activity.
    For a single method (e.g. "backfill"), we have:
        share:
          2020: { min:0, max:0.1, mean:0.05 }
          2050: { ... }
    We want to pick or interpolate the 'mean' field.

    """
    all_years = sorted(share_dict.keys())

    if not all_years:
        return 1.0

    if year <= all_years[0]:
        return share_dict[all_years[0]]["mean"]
    if year >= all_years[-1]:
        return share_dict[all_years[-1]]["mean"]
    for i in range(len(all_years) - 1):
        y0, y1 = all_years[i], all_years[i + 1]
        if y0 <= year <= y1:
            f = (year - y0) / (y1 - y0)
            val0 = share_dict[y0]["mean"]
            val1 = share_dict[y1]["mean"]
            return (1 - f) * val0 + f * val1

    return share_dict[all_years[-1]]["mean"]


def parse_tailings_shares(year: int, config: dict, region: str) -> dict:
    shares = {}
    for method, cfg in config.items():
        region_data = cfg.get("share", {}).get(region, cfg.get("share", {}).get("GLO"))
        shares[method] = interpolate_method_share(year, region_data)
    return shares


def copy_exchange(exc: dict) -> dict:
    """
    Create a shallow copy of an exchange with the most relevant fields.
    """
    return {
        k: exc[k]
        for k in ["name", "product", "amount", "unit", "type", "input", "location"]
        if k in exc
    }


def copy_activity(act: dict, location: str, product_name=None) -> dict:
    new_act = copy.deepcopy(act)
    new_act["location"] = location
    new_act["code"] = str(uuid.uuid4())

    if product_name:
        new_act["reference product"] = product_name

    has_production = False
    for exc in new_act.get("exchanges", []):
        if exc["type"] == "technosphere":
            # Keep technosphere input as-is unless it matches old reference product
            if "input" not in exc:
                exc["input"] = (
                    exc["name"],
                    exc["product"],
                    exc.get("location", "GLO"),
                    exc.get("unit", "kilogram")
                )

        elif exc["type"] == "production":
            has_production = True
            exc["name"] = new_act["name"]
            if product_name:
                exc["product"] = product_name
            exc["location"] = location
            exc["input"] = (exc["name"], exc["product"], location, exc.get("unit", "kilogram"))

    # Add production exchange if none exists
    if product_name and not has_production:
        new_act.setdefault("exchanges", []).append(
            {
                "type": "production",
                "name": new_act["name"],
                "product": product_name,
                "amount": 1,  # +1 to ensure square matrix linking
                "unit": new_act.get("unit", "kilogram"),
                "location": location,
                "input": (
                    new_act["name"],
                    product_name,
                    location,
                    new_act.get("unit", "kilogram"),
                ),
            }
        )

    # Extra safety: ensure every production exchange has a valid input
    for exc in new_act["exchanges"]:
        if exc["type"] == "production" and "input" not in exc:
            exc["input"] = (
                exc["name"],
                exc["product"],
                exc["location"],
                exc.get("unit", "kilogram"),
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
        if product_fltr and not any(f in exc.get("product", "").lower() for f in product_fltr):
            continue
        if product_mask and any(f in exc.get("product", "").lower() for f in product_mask):
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
        self.tailings_config = load_tailings_config()
        self.tailings_topology = load_tailings_topology()
        self.geomap = Geomap(model)

    def _map_to_tailings_region(self, iam_loc: str) -> str:
        """
        Map IAM location to tailings region.
        """
        for region, locations in self.tailings_topology.items():
            if iam_loc in locations:
                return region
        return "GLO"

    def _make_filter(self, cfg):
        return lambda act: has_exchange_matching(act, cfg.get("name", {}), cfg.get("reference product", {}))

    def _split_exchanges(self, act, config, shares):

        original_cfg = config.get("impoundment", {})
        name_fltr = original_cfg.get("name", {})
        prod_fltr = original_cfg.get("reference product", {})

        new_exchanges = []
        for exc in act["exchanges"]:
            if exc.get("type") != "technosphere":
                new_exchanges.append(exc)
                continue
            if not has_exchange_matching({"exchanges": [exc]}, name_fltr, prod_fltr):
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
                    provider = self._get_or_create_provider(method, act["location"], exc["product"])
                    if provider:
                        new_exc = copy_exchange(exc)
                        new_exc["name"] = provider["name"]
                        new_exc["product"] = provider["reference product"]
                        new_exc["location"] = provider["location"]
                        new_exc["input"] = (provider["name"], provider["reference product"], provider["location"],
                                            provider.get("unit", "kilogram"))
                        new_exc["amount"] = total * share
                        new_exchanges.append(new_exc)

        act["exchanges"] = new_exchanges

    def _get_or_create_provider(self, method, location, product):
        cfg = self.tailings_config.get("sulfidic tailings", {}).get(method, {})
        name_filters = cfg.get("name", {})
        prod_filters = cfg.get("reference product", {})

        name_fltr = [ws.contains("name", s) for s in name_filters.get("fltr", [])]
        name_mask = [ws.exclude(ws.contains("name", s)) for s in name_filters.get("mask", [])]
        prod_fltr = [ws.contains("reference product", s) for s in prod_filters.get("fltr", [])]
        prod_mask = [ws.exclude(ws.contains("reference product", s)) for s in prod_filters.get("mask", [])]

        providers = list(ws.get_many(self.database, *name_fltr, *prod_fltr, *name_mask, *prod_mask, ws.equals("location", location)))
        if providers:
            return providers[0]

        fallback = list(ws.get_many(self.database, *name_fltr, *prod_fltr, *name_mask, *prod_mask, ws.equals("location", "GLO")))
        if not fallback:
            logger.warning(f"[Mining] No fallback found for method {method} at {location}")
            return None

        new_provider = copy_activity(fallback[0], location, product)
        self.database.append(new_provider)
        self.add_to_index(new_provider)
        self.write_log(new_provider, "created")
        return new_provider

    def update_tailings_treatment(self):
        config = self.tailings_config.get("sulfidic tailings", {})
        impound_cfg = config.get("impoundment", {})

        if not impound_cfg:
            logger.info("[Mining] No config under 'sulfidic tailings' -> 'impoundment'")
            return

        impound_filter = self._make_filter(impound_cfg)
        consumers = list(ws.get_many(self.database, impound_filter))

        for act in consumers:
            location = act["location"]
            iam_loc = self.geomap.ecoinvent_to_iam_location(location)
            tailings_region = self._map_to_tailings_region(iam_loc)
            shares = parse_tailings_shares(self.year, config, tailings_region)

            self._split_exchanges(act, config, shares)


def write_log(self, dataset, status="updated"):
    txt = f"{status}|{self.model}|{self.scenario}|{self.year}|{dataset['name']}|{dataset.get('reference product', '')}|{dataset['location']}"
    logger.info(txt)








