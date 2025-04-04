"""
Integrates projections regarding tailings treatment.
"""

import yaml
import copy
import numpy as np
import uuid
from functools import lru_cache
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
from .logger import create_logger

logger = create_logger("mining")

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

def parse_tailings_shares(year: int, config_methods: dict) -> dict:
    """
    If config_methods = {
      "backfill": {
         "name": {...},
         "reference product":{...},
         "share": {
            2020:{ min, max, mean},
            2050:{ min, max, mean}
         }
      },
      "impoundment": { ... }
    }

    We'll parse the numeric 'mean' for the given year.
    """
    out = {}
    for method, method_dict in config_methods.items():
        if "share" in method_dict:
            share_val = interpolate_method_share(year, method_dict["share"])
            out[method] = share_val
        else:
            print("Missing share for activity:", method)
            out[method] = 0.0
    return out

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
                exc["input"] = (exc["name"], exc["product"], exc.get("location", "GLO"), exc.get("unit", "kilogram"))

        elif exc["type"] == "production":
            has_production = True
            exc["name"] = new_act["name"]
            if product_name:
                exc["product"] = product_name
            exc["location"] = location
            unit = exc.get("unit", "kilogram")
            exc["input"] = (exc["name"], exc["product"], location, unit)

    # Add production exchange if none exists
    if product_name and not has_production:
        print("[DEBUG] No production exchange found. Adding a new one.")
        new_act.setdefault("exchanges", []).append({
            "type": "production",
            "name": new_act["name"],
            "product": product_name,
            "amount": 1,  # +1 to ensure square matrix linking
            "unit": new_act.get("unit", "kilogram"),
            "location": location,
            "input": (new_act["name"], product_name, location, new_act.get("unit", "kilogram"))
        })

    # Extra safety: ensure every production exchange has a valid input
    for exc in new_act["exchanges"]:
        if exc["type"] == "production" and "input" not in exc:
            exc["input"] = (exc["name"], exc["product"], exc["location"], exc.get("unit", "kilogram"))

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

    exchanges = activity.get("exchanges", [])
    for exc in exchanges:
        if exc.get("type") != "technosphere":
            continue

        exc_name = exc.get("name", "").lower()
        exc_prod = exc.get("product", "").lower()

        if name_fltr and not any(f in exc_name for f in name_fltr):
            continue
        if name_mask and any(f in exc_name for f in name_mask):
            continue
        if product_fltr and not any(f in exc_prod for f in product_fltr):
            continue
        if product_mask and any(f in exc_prod for f in product_mask):
            continue
        return True

    return False



def activity_filter_factory(name_filters: dict, prod_filters: dict) -> callable:
    """
    Return a function that checks if an activity has at least one
    technosphere exchange that matches name_filters and prod_filters.
    """
    def filter_func(activity):
        return has_exchange_matching(activity, name_filters, prod_filters)
    return filter_func

def partial_split_exchange(activity, old_cfg, bf_share, imp_share, get_backfill_provider):
    """
    Within 'activity', partial-split any exchanges that matches 'old_cfg' (impoundment config).
    So if an exchange is e.g. 1.0 kg, we keep imp_share portion for original name/product, and
    route bf_share portion to the backfill provider.
    """

    name_filters = old_cfg.get("name", {})
    prod_filters = old_cfg.get("reference product", {})

    new_exchanges = []
    splitted = False

    for exc in activity["exchanges"]:
        if splitted:
            new_exchanges.append(exc)
            continue

        if exc.get("type") == "technosphere":
            exc_name = exc.get("name", "").lower()
            exc_prod = exc.get("product", "").lower()

            matched = True

            if name_filters.get("fltr"):
                if not any(f.lower() in exc_name for f in name_filters["fltr"]):
                    matched = False
            if name_filters.get("mask"):
                if any(f.lower() in exc_name for f in name_filters["mask"]):
                    matched = False
            if prod_filters.get("fltr") and matched:
                if not any(f.lower() in exc_prod for f in prod_filters["fltr"]):
                    matched = False
            if prod_filters.get("mask") and matched:
                if any(f.lower() in exc_prod for f in prod_filters["mask"]):
                    matched = False


            if matched:
                splitted = True
                total_amount = exc["amount"]

                if imp_share > 0:
                    keep_exc = copy_exchange(exc)
                    keep_exc["amount"] = total_amount * imp_share
                    new_exchanges.append(keep_exc)
                    logger.info(f"   -> impound portion: {keep_exc['amount']}")

                if bf_share > 0:
                    provider = get_backfill_provider(activity["location"], exc["product"])

                    if provider:
                        bf_exc = copy_exchange(exc)
                        bf_exc["amount"] = total_amount * bf_share
                        bf_exc["name"] = provider["name"]
                        bf_exc["product"] = provider["reference product"]
                        bf_exc["location"] = provider["location"]
                        bf_exc["unit"] = provider.get("unit", "kilogram")
                        bf_exc["input"] = (
                            provider["name"],
                            provider["reference product"],
                            provider["location"],
                            provider.get("unit", "kilogram")
                        )
                        new_exchanges.append(bf_exc)
                        logger.info(f"   -> backfill portion: {bf_exc['amount']}")
                    else:
                        logger.warning(f"[Mining] No backfill provider found for {activity['location']}")
                continue
            else:
                print(f"[Mining] No match for exchange: {exc_name} / {exc_prod}")

        new_exchanges.append(exc)

    activity["exchanges"] = new_exchanges




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

    def update_tailings_treatment(self):
        config_glo = self.tailings_config.get("sulfidic tailings", {}).get("GLO", {})
        if not config_glo:
            logger.info("[Mining] No config under 'sulfidic tailings' -> 'GLO'")
            return

        numeric_shares = parse_tailings_shares(self.year, config_glo)
        bf_share = numeric_shares.get("backfill", 0.0)
        imp_share = numeric_shares.get("impoundment", 1.0)
        logger.info(f"[Mining] For year {self.year}, backfill={bf_share}, impoundment={imp_share}")

        backfill_cfg = config_glo.get("backfill", {})
        impound_cfg  = config_glo.get("impoundment", {})

        name_filters = impound_cfg.get("name", {})
        prod_filters = impound_cfg.get("reference product", {})

        def impound_filter_func(act):
            return has_exchange_matching(act, name_filters, prod_filters)


        impound_consumers = list(ws.get_many(self.database, impound_filter_func))

        # partial-split them
        for act in impound_consumers:
            partial_split_exchange(
                activity=act,
                old_cfg=impound_cfg,
                bf_share=bf_share,
                imp_share=imp_share,
                get_backfill_provider=self.get_or_create_local_backfill,
            )

    def get_or_create_local_backfill(self, location, product_name):
        name = "treatment of sulfidic tailings, generic, backfilling"
        generic_product = "sulfidic tailings, generic"

        providers = list(ws.get_many(
            self.database,
            ws.equals("name", name),
            ws.equals("reference product", product_name),
            ws.equals("location", location),
        ))
        if providers:
            return providers[0]

        fallback = list(ws.get_many(
            self.database,
            ws.equals("name", name),
            ws.equals("reference product", generic_product),
            ws.equals("location", "GLO"),
        ))
        if not fallback:
            print("[Mining] WARNING: No fallback found for backfill provider")
            return None

        new_provider = copy_activity(fallback[0], location, product_name)
        new_provider["reference product"] = product_name
        new_provider["unit"] = fallback[0].get("unit", "kilogram")

        self.database.append(new_provider)
        self.add_to_index(new_provider)
        self.write_log(new_provider, "created")
        return new_provider


    def write_log(self, dataset, status="updated"):
        txt = (f"{status}|{self.model}|{self.scenario}|{self.year}|"
               f"{dataset['name']}|{dataset.get('reference product','')}|{dataset['location']}")
        logger.info(txt)









