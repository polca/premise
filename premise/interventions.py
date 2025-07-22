"""
Integrates projections regarding:
    - Sulfidic tailings treatment.
    - Slag treatment (EAF and BOF).
    - Copper treatment (recycling and incineration shares).
    - Brake wear emissions (copper and antimony ions).
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

logger = create_logger("interventions")

TAILINGS_REGIONS_FILE = DATA_DIR / "interventions" / "interventions_topology.yaml"
TAILINGS_CONFIG_FILE = DATA_DIR / "interventions" / "tailings_activities.yaml"
EAF_SLAG_CONFIG_FILE = DATA_DIR / "interventions" / "EAF_slag_activities.yaml"
BOF_SLAG_CONFIG_FILE = DATA_DIR / "interventions" / "BOF_slag_activities.yaml"
COPPER_CONFIG_FILE = DATA_DIR / "interventions" / "copper_recovery_activities.yaml"
BRAKE_WEAR_CONFIG_FILE = DATA_DIR / "interventions" / "brake_wear_activities.yaml"


def _update_interventions(scenario, version, system_model):
    """
    Update the scenario database with interventions for tailings, slag, and copper treatment.
    """
    interventions = Interventions(
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

    interventions.update_tailings_treatment()
    interventions.update_slag_treatment()
    interventions.update_copper_treatment()
    interventions.update_brake_wear()
    interventions.relink_datasets()
    scenario["database"] = interventions.database
    scenario["cache"] = interventions.cache
    scenario["index"] = interventions.index

    return scenario


def load_config(file_path, model: str):
    """
    Load and parse a YAML configuration file for interventions.
    """

    with open(file_path) as f:
        tech_data = yaml.safe_load(f)

    with open(TAILINGS_REGIONS_FILE) as f:
        region_map_raw = yaml.safe_load(f)

    def get_region_remap(region_map, model_name):
        """
        Get a remapping of regions to model-specific regions.
        """
        remap = defaultdict(list)
        for std_region, models in region_map.items():
            remap[std_region] = models.get(model_name, [])
        return remap

    def create_dataset(data, model_name, region_map_raw):
        region_remap = get_region_remap(region_map_raw, model_name)

        techs = []
        years = set()
        records = []

        for tech, tech_data in data.items():
            techs.append(tech)
            for raw_region, region_data in tech_data.get("share", {}).items():
                mapped_regions = region_remap.get(raw_region, None)
                if mapped_regions is None:
                    continue
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


class Interventions(BaseTransformation):
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
        self.geomap = Geomap(model)
        self.tailings_shares = load_config(TAILINGS_CONFIG_FILE, model)
        self.eaf_slag_shares = load_config(EAF_SLAG_CONFIG_FILE, model)
        self.bof_slag_shares = load_config(BOF_SLAG_CONFIG_FILE, model)
        self.copper_shares = load_config(COPPER_CONFIG_FILE, model)
        self.brake_wear_shares = load_config(BRAKE_WEAR_CONFIG_FILE, model)
        inv = InventorySet(database=database, version=version, model=model)
        self.tailings_map = inv.generate_mining_waste_map()
        self.eaf_slag_map = inv.generate_eaf_slag_waste_map()
        self.bof_slag_map = inv.generate_bof_slag_waste_map()
        self.copper_map = inv.generate_copper_waste_map()
        self.brake_wear_map = inv.generate_brake_wear_map()

    def update_tailings_treatment(self):


        self.process_and_add_activities(
            mapping=self.tailings_map,
            regions=self.tailings_shares.region.values.tolist(),
        )

        market_datasets = [
            ds
            for ds in ws.get_many(
                self.database, ws.contains("name", "market for sulfidic tailings")
            )
        ]


        processed_datasets = []
        for market_dataset in market_datasets:
            regionalized_datasets = self.fetch_proxies(
                datasets=market_dataset,
            )

            regionalized_datasets = {
                k: v
                for k, v in regionalized_datasets.items()
                if k in self.tailings_shares.region.values
            }

            ### Old markets were consuming positive amounts of the new markets. We need to invert this
            for ds in self.database:
                if (
                    ds["name"] == market_dataset
                    and ds["location"] not in self.tailings_shares.region.values
                ):
                    iam_region = self.geomap.ecoinvent_to_iam_location(ds["location"])
                    if iam_region in regionalized_datasets:
                        regional_market = regionalized_datasets[iam_region]

                        prod_exchanges = [
                            e for e in ds["exchanges"] if e["type"] == "production"
                        ]
                        prod_exchanges.append(
                            {
                                "type": "technosphere",
                                "name": regional_market["name"],
                                "product": regional_market["reference product"],
                                "amount": -1,
                                "unit": regional_market["unit"],
                                "location": regional_market["location"],
                            }
                        )

                        ds["exchanges"] = prod_exchanges
                        self.write_log(ds, "updated to link to regional market")

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

                    supplier = [
                        ds for ds in self.tailings_map[waste_management_type]
                        if ds["location"] == region
                    ]

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
                                f"[Interventions] More than one supplier found for {waste_management_type} in {region}"
                            )
                    if not supplier:
                        print(
                            f"[Interventions] No supplier found for {waste_management_type} in {region}"
                        )
                        continue

                    supplier = supplier[0]

                    amount_mean = -1 * shares.sel(technology=waste_management_type)[
                        "mean"
                    ].values.item(0)
                    amount_max = -1 * shares.sel(technology=waste_management_type)[
                        "min"
                    ].values.item(0)
                    amount_min = -1 * shares.sel(technology=waste_management_type)[
                        "max"
                    ].values.item(0)

                    market_dataset["exchanges"].append(
                        {
                            "type": "technosphere",
                            "name": supplier["name"],
                            "product": supplier["reference product"],
                            "amount": amount_mean,
                            "unit": supplier["unit"],
                            "location": supplier["location"],
                            "uncertainty type": 5,
                            "loc": amount_mean,
                            "minimum": amount_min,
                            "maximum": amount_max,
                        }
                    )

            processed_datasets.extend(regionalized_datasets.values())

        for dataset in processed_datasets:
            self.add_to_index(dataset)
            self.write_log(dataset, "[Interventions] created")
            self.database.append(dataset)

    def update_slag_treatment(self):
        """
        Regionalizes EAF and BOF slag treatment activities and updates slag markets
        with shares of treatment technologies per region and year.
        """

        slag_configs = {
            "EAF": (
                self.eaf_slag_map,
                self.eaf_slag_shares,
                "market for electric arc furnace slag",
            ),
            "BOF": (
                self.bof_slag_map,
                self.bof_slag_shares,
                "market for basic oxygen furnace slag",
            ),
        }



        for slag_type, (slag_map, slag_shares, market_name) in slag_configs.items():

            self.process_and_add_activities(
                mapping=slag_map,
                regions=slag_shares.region.values.tolist(),
            )

            market_datasets = [
                ds
                for ds in ws.get_many(
                    self.database,
                    ws.startswith("name", market_name),
                )
            ]

            processed_datasets = []
            for market_dataset in market_datasets:
                regionalized_datasets = self.fetch_proxies(
                    datasets=[market_dataset],
                )

                regionalized_datasets = {
                    k: v
                    for k, v in regionalized_datasets.items()
                    if k in slag_shares.region.values
                }

                for ds in self.database:
                    if (
                        ds["name"] == market_dataset
                        and ds["location"] not in slag_shares.region.values
                    ):
                        iam_region = self.geomap.ecoinvent_to_iam_location(
                            ds["location"]
                        )
                        if iam_region in regionalized_datasets:
                            regional_market = regionalized_datasets[iam_region]

                            prod_exchanges = [
                                e for e in ds["exchanges"] if e["type"] == "production"
                            ]
                            prod_exchanges.append(
                                {
                                    "type": "technosphere",
                                    "name": regional_market["name"],
                                    "product": regional_market["reference product"],
                                    "amount": -1,
                                    "unit": regional_market["unit"],
                                    "location": regional_market["location"],
                                }
                            )

                            ds["exchanges"] = prod_exchanges
                            self.write_log(ds, "updated to link to regional market")

                for region, market_ds in regionalized_datasets.items():

                    if self.year < slag_shares.year.values.min():
                        year = slag_shares.year.values.min()
                    elif self.year > slag_shares.year.values.max():
                        year = slag_shares.year.values.max()
                    else:
                        year = self.year

                    target_region = (
                        region if region in slag_shares.region.values else "GLO"
                    )

                    if target_region != region:
                        print(
                            f"[Interventions] No slag share data for region '{region}', falling back to 'GLO'."
                        )
                        if "GLO" not in slag_shares.region.values:
                            print(
                                f"[Interventions] No data for GLO either — skipping region '{region}'"
                            )
                            continue

                    shares = slag_shares.sel(region=target_region).interp(year=year)

                    market_ds["exchanges"] = [
                        e
                        for e in market_ds["exchanges"]
                        if e["type"] == "production"
                        or (
                            e["type"] == "technosphere"
                            and (
                                "transport" in e.get("name", "").lower()
                                or "freight" in e.get("name", "").lower()
                            )
                        )
                    ]

                    for treatment_type in shares.technology.values:
                        suppliers = [
                            ds for ds in slag_map[treatment_type]
                            if ds["location"] == target_region
                        ]

                        if len(suppliers) > 1:
                            print(
                                f"[Interventions] More than one supplier found for {treatment_type} in {region}"
                            )
                        if not suppliers:
                            print(
                                f"[Interventions] No supplier found for {treatment_type} in {region}"
                            )
                            continue

                        supplier = suppliers[0]

                        amount_mean = -1 * shares.sel(technology=treatment_type)[
                            "mean"
                        ].values.item(0)
                        amount_max = -1 * shares.sel(technology=treatment_type)[
                            "min"
                        ].values.item(0)
                        amount_min = -1 * shares.sel(technology=treatment_type)[
                            "max"
                        ].values.item(0)

                        market_ds["exchanges"].append(
                            {
                                "type": "technosphere",
                                "name": supplier["name"],
                                "product": supplier["reference product"],
                                "amount": amount_mean,
                                "unit": supplier["unit"],
                                "location": supplier["location"],
                                "uncertainty type": 5,
                                "loc": amount_mean,
                                "minimum": amount_min,
                                "maximum": amount_max,
                            }
                        )

                processed_datasets.extend(regionalized_datasets.values())

            for dataset in processed_datasets:
                self.add_to_index(dataset)
                self.write_log(dataset, "[Interventions] created")
                self.database.append(dataset)

    def update_copper_treatment(self):
        """
        Update waste copper incineration and recycling pathways with new exchange amounts
        for copper scrap and bottom ash based on year-specific values.
        """

        if self.year < self.copper_shares.year.values.min():
            year = self.copper_shares.year.values.min()
        elif self.year > self.copper_shares.year.values.max():
            year = self.copper_shares.year.values.max()
        else:
            year = self.year

        scrap_amounts = self.copper_shares.sel(technology="scrap copper").interp(
            year=year
        )
        ash_amounts = self.copper_shares.sel(technology="bottom ash").interp(year=year)

        activities = ws.get_many(
            self.database,
            ws.either(
                ws.startswith(
                    "name", "treatment of waste copper, municipal incineration"
                ),
                ws.startswith(
                    "name", "treatment of scrap copper, municipal incineration"
                ),
            ),
        )

        for act in activities:
            for exc in act["exchanges"]:
                if exc["type"] == "technosphere":
                    if (
                        exc["name"]
                        == "copper scrap, sorted, pressed, Recycled Content cut-off"
                        and exc["product"] == "copper scrap, sorted, pressed"
                    ):
                        exc.update(
                            {
                                "amount": scrap_amounts["mean"].item() * -1,
                                "uncertainty type": 5,
                                "loc": scrap_amounts["mean"].item() * -1,
                                "minimum": scrap_amounts["min"].item() * -1,
                                "maximum": scrap_amounts["max"].item() * -1,
                            }
                        )

                    elif exc["name"].startswith("market for bottom ash") and exc[
                        "product"
                    ].startswith("bottom ash"):
                        exc.update(
                            {
                                "amount": ash_amounts["mean"].item() * -1,
                                "uncertainty type": 5,
                                "loc": ash_amounts["mean"].item() * -1,
                                "minimum": ash_amounts["min"].item() * -1,
                                "maximum": ash_amounts["max"].item() * -1,
                            }
                        )

            self.write_log(act, "[Interventions] Updated copper treatment")

    def update_brake_wear(self):
        """
        Update biosphere flows for copper and antimony ions in brake wear emissions
        activities based on year-specific values for the appropriate region.
        """

        min_year = self.brake_wear_shares.year.values.min()
        max_year = self.brake_wear_shares.year.values.max()
        year = np.clip(self.year, min_year, max_year)

        fallback_region = None
        for r in self.brake_wear_shares.region.values:
            if "GLO" in self.geomap.iam_to_ecoinvent_location(r):
                fallback_region = r
                break

        activities = ws.get_many(
            self.database,
            ws.startswith("name", "treatment of brake wear emissions"),
        )

        for act in activities:
            iam_region = self.geomap.ecoinvent_to_iam_location(act["location"])

            matching_regions = [
                r
                for r in self.brake_wear_shares.region.values
                if iam_region in self.geomap.ecoinvent_to_iam_location(r)
            ]

            target_region = matching_regions[0] if matching_regions else fallback_region

            for exc in act["exchanges"]:
                if exc["type"] == "biosphere":
                    if exc["name"] == "Copper ion":
                        tech = "brake wear - copper"
                    elif exc["name"] == "Antimony ion":
                        tech = "brake wear - antimony"
                    else:
                        continue

                    if tech:
                        try:
                            data = self.brake_wear_shares.sel(
                                region=target_region, technology=tech
                            )
                            data = data.dropna("year", how="all")
                            share = data.interp(year=year)

                            exc.update(
                                {
                                    "amount": share["mean"].item(),
                                    "uncertainty type": 5,
                                    "loc": share["mean"].item(),
                                    "minimum": share["min"].item(),
                                    "maximum": share["max"].item(),
                                }
                            )
                        except KeyError:
                            print(
                                f"[Interventions] No data for {tech} in {target_region} at year {year}"
                            )
                            continue

            self.write_log(act, "[Interventions] Updated brake wear emissions")

    def write_log(self, dataset, status="updated"):
        txt = f"{status}|{self.model}|{self.scenario}|{self.year}|{dataset['name']}|{dataset.get('reference product', '')}|{dataset['location']}"
        logger.info(txt)
