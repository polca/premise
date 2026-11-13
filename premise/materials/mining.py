"""
Integrates projections regarding tailings treatment.
"""

import yaml
import copy
import uuid
import warnings
import xarray as xr
import numpy as np
from collections import defaultdict
from ..core.transformation import (
    BaseTransformation,
    IAMDataCollection,
    List,
    ws,
)
from ..core.utils import DATA_DIR
from ..core.logger import create_logger
from ..reporting.provenance import record_change_event
from ..inventory.inventory_store import (
    get_scenario_inventory,
    replace_scenario_inventory,
)
from ..iam.geomap import Geomap
from ..inventory.activity_maps import InventorySet, get_mapping
from ..inventory.inventory_copy import clone_inventory_dataset

logger = create_logger("mining")

TAILINGS_REGIONS_FILE = DATA_DIR / "mining" / "tailings_topology.yaml"
TAILINGS_WASTE_SHARES = DATA_DIR / "mining" / "tailings_activities.yaml"


def _update_mining(
    scenario, version, system_model, family="ecoinvent", biosphere_version=None
):
    mining = Mining(
        database=get_scenario_inventory(scenario),
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
        family=family,
        biosphere_version=biosphere_version,
    )

    mining.prepare_market_templates()
    mining.update_tailings_treatment()
    mining.forward_market_consumers()
    mining.relink_datasets()
    replace_scenario_inventory(scenario, mining.database)
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
        family: str = "ecoinvent",
        biosphere_version: str = None,
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
            family=family,
            biosphere_version=biosphere_version,
        )
        self.year = int(year)
        self.tailings_shares = load_tailings_config(model)
        self.geomap = Geomap(model)
        inv = InventorySet(
            database=database, version=version, model=model, family=self.family
        )
        self.mining_map = inv.generate_mining_waste_map()

    def _market_template_copy(self, source, identity, reference, role, config):
        """Copy a mapped template; change only its declared reference convention."""
        dataset = clone_inventory_dataset(source)
        dataset.update(identity)
        dataset["code"] = uuid.uuid4().hex
        dataset["premise mining role"] = role
        dataset["comment"] = (
            config["limitations"]
            + "\nOriginal source documentation:\n"
            + source.get("comment", "")
        )
        for exchange in ws.production(dataset):
            exchange.update(
                name=dataset["name"],
                product=dataset["reference product"],
                amount=reference,
                loc=reference,
            )
            if "reference product" in exchange:
                exchange["reference product"] = dataset["reference product"]
            if "negative" in exchange:
                exchange["negative"] = reference < 0
            exchange.pop("input", None)
            exchange.pop("output", None)
        return dataset

    @staticmethod
    def _market_template_reference(dataset):
        references = list(ws.production(dataset))
        if (
            len(references) != 1
            or references[0]["amount"] not in (-1, 1)
            or isinstance(references[0]["amount"], (bool, np.bool_))
            or references[0].get("uncertainty type", 0) != 0
            or references[0].get("formula")
            or references[0].get("unit", dataset["unit"]) != dataset["unit"]
        ):
            raise ValueError(
                "Mining market template requires one deterministic +/-1 reference in the dataset unit."
            )
        return references[0]["amount"]

    def prepare_market_templates(self):
        """Supply missing market templates from explicit family YAML bindings.

        The shared mixer consumes negative-reference waste suppliers. Preserve
        the complete service inventory in a distinct, sign-converted copy so
        its original identity can later forward consumers to the regional mix.
        """
        self.market_template_bindings = []
        for role, aliases in get_mapping(
            TAILINGS_WASTE_SHARES, f"{self.family}_aliases"
        ).items():
            config = aliases.get("market_template")
            if config is None:
                continue
            warnings.warn(config["limitations"], UserWarning, stacklevel=2)
            source = ws.get_one(
                self.database,
                *[ws.equals(key, value) for key, value in config["source"].items()],
            )
            if (
                self._market_template_reference(source)
                != config["source_reference_amount"]
                or config["source_reference_amount"] != 1
                or config["target_reference_amount"] != -1
                or any(
                    config[key]["unit"] != source["unit"]
                    for key in ("supplier", "market")
                )
            ):
                raise ValueError(
                    "Mining market template requires a deterministic +1 service and a same-unit -1 waste reference."
                )
            supplier_identity = {**config["supplier"], "location": source["location"]}
            market_identity = {**config["market"], "location": source["location"]}
            markets = list(
                ws.get_many(
                    self.database,
                    *[ws.equals(key, value) for key, value in market_identity.items()],
                )
            )
            if markets and (
                len(markets) != 1
                or markets[0].get("premise mining role") != "market template"
                or self._market_template_reference(markets[0]) != -1
            ):
                raise ValueError(
                    f"Mining market {market_identity!r} requires declared template provenance and a deterministic -1 reference."
                )
            existing = list(
                ws.get_many(
                    self.database,
                    *[
                        ws.equals(key, value)
                        for key, value in supplier_identity.items()
                    ],
                )
            )
            if existing:
                if (
                    len(existing) != 1
                    or existing[0].get("premise mining role") != "impoundment supplier"
                    or self._market_template_reference(existing[0]) != -1
                ):
                    raise ValueError(
                        f"Mining supplier {supplier_identity!r} requires declared template provenance and a deterministic -1 reference."
                    )
                supplier = existing[0]
            else:
                if source.get("premise mining role") == "market forwarder":
                    raise ValueError(
                        "Cannot recover the impoundment inventory from an already forwarded service."
                    )
                supplier = self._market_template_copy(
                    source, supplier_identity, -1, "impoundment supplier", config
                )
                self.database.append(supplier)
                self.add_to_index(supplier)
                self.write_log(supplier, "created")
            self.mining_map[role] = [supplier]
            if not markets:
                market = self._market_template_copy(
                    source, market_identity, -1, "market template", config
                )
                market["exchanges"] = list(ws.production(market)) + [
                    {
                        "name": supplier["name"],
                        "product": supplier["reference product"],
                        "location": supplier["location"],
                        "unit": supplier["unit"],
                        "amount": -1,
                        "type": "technosphere",
                        "uncertainty type": 0,
                    }
                ]
                self.database.append(market)
                self.add_to_index(market)
                self.write_log(market, "created")
            self.market_template_bindings.append((source, config))

    def forward_market_consumers(self):
        """Retain native service identities while forwarding to shared IAM mixes."""
        regions = [r for r in self.regions if r in self.tailings_shares.region.values]
        for source, config in self.market_template_bindings:
            forwarders = self.fetch_proxies([source], relink=False, regions=regions)
            for region, dataset in forwarders.items():
                dataset["premise mining role"] = "market forwarder"
                dataset["comment"] = config["limitations"]
                dataset["exchanges"] = list(ws.production(dataset)) + [
                    {
                        "name": config["market"]["name"],
                        "product": config["market"]["reference product"],
                        "location": region,
                        "unit": config["market"]["unit"],
                        "amount": -1,
                        "type": "technosphere",
                        "uncertainty type": 0,
                    }
                ]
                self.database.append(dataset)
                self.add_to_index(dataset)
                self.write_log(dataset, "created")
            self.empty_original_datasets(
                [source],
                production_shares={},
                loc_map={
                    source["location"]: self.ecoinvent_to_iam_loc[source["location"]]
                },
                regions=regions,
                target=config["market"],
                conversion_factor=-1,
            )
            source["premise mining role"] = "market forwarder"
            source["comment"] = config["limitations"]

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
    """Record a structured mining provenance event."""

    record_change_event(self, dataset, status, sector="mining")
