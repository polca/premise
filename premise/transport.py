"""
transport.py contains the class Transport, which takes care of importing inventories
for a number of different vehicle types, and create fleet average vehicles based on
IAM data, and integrate them into the database.
"""

import uuid
from typing import Any, Dict, List, Union

import numpy as np
import xarray as xr
import yaml
from wurst import searching as ws

from .activity_maps import InventorySet
from .filesystem_constants import DATA_DIR, IAM_OUTPUT_DIR
from .logger import create_logger
from .transformation import BaseTransformation, IAMDataCollection
from .utils import eidb_label, rescale_exchanges
from .validation import CarValidation, TruckValidation

logger = create_logger("transport")

FILEPATH_TRUCK_LOAD_FACTORS = DATA_DIR / "transport" / "avg_load_factors.yaml"
FILEPATH_VEHICLES_MAP = DATA_DIR / "transport" / "vehicles_map.yaml"


def _update_vehicles(scenario, vehicle_type, version, system_model):

    fleet_data = {
        "car": scenario["iam data"].passenger_car_fleet,
        "truck": scenario["iam data"].road_freight_fleet,
        "bus": scenario["iam data"].bus_fleet,
        "train": scenario["iam data"].rail_freight_fleet,
        "two-wheeler": scenario["iam data"].two_wheelers_fleet,
        "ship": scenario["iam data"].sea_freight_fleet,
    }

    has_fleet = True

    if vehicle_type not in fleet_data:
        raise ValueError("Unknown vehicle type.")

    if fleet_data[vehicle_type] is None:
        print(f"No {vehicle_type} fleet scenario data available -- skipping")
        has_fleet = False

    trspt = Transport(
        database=scenario["database"],
        year=scenario["year"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        version=version,
        system_model=system_model,
        vehicle_type=vehicle_type,
        relink=False,
        has_fleet=has_fleet,
        index=scenario.get("index"),
    )

    trspt.regionalize_transport_datasets()

    if fleet_data[vehicle_type] is not None:
        trspt.create_vehicle_markets()
        trspt.relink_transport_datasets()

    scenario["database"] = trspt.database
    scenario["cache"] = trspt.cache
    scenario["index"] = trspt.index

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"][vehicle_type] = trspt.vehicle_map

    validation_func = {
        "car": CarValidation,
        "truck": TruckValidation,
    }

    if vehicle_type in validation_func:
        validate = validation_func[vehicle_type](
            model=scenario["model"],
            scenario=scenario["pathway"],
            year=scenario["year"],
            regions=scenario["iam data"].regions,
            database=trspt.database,
            iam_data=scenario["iam data"],
        )
        validate.run_checks()

    return scenario


def get_battery_size() -> dict:
    """
    Return a dictionary that contains the size of the battery
    for each vehicle type and powertrain.
    :return: dictionary with battery sizes
    """
    with open(
        DATA_DIR / "transport" / "battery_size.yaml", "r", encoding="utf-8"
    ) as stream:
        out = yaml.safe_load(stream)
        return out


def get_average_truck_load_factors() -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Load average load factors for trucks
    to convert transport demand in vkm into tkm.
    :return: dictionary with load factors per truck size class
    """
    with open(FILEPATH_TRUCK_LOAD_FACTORS, "r", encoding="utf-8") as stream:
        out = yaml.safe_load(stream)
        return out


def get_vehicles_mapping() -> Dict[str, dict]:
    """
    Return a dictionary that contains mapping
    between `carculator` terminology and `ecoinvent` terminology
    regarding size classes, powertrain types, etc.
    :return: dictionary to map terminology between carculator and ecoinvent
    """
    with open(FILEPATH_VEHICLES_MAP, "r", encoding="utf-8") as stream:
        out = yaml.safe_load(stream)
        return out


class Transport(BaseTransformation):
    """
    Class that modifies transport markets in ecoinvent based on IAM output data.

    :ivar database: database dictionary from :attr:`.NewDatabase.database`
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :ivar pathway: file path to vehicle inventories
    :ivar year: year, from :attr:`.NewDatabase.year`
    :ivar version: ecoinvent database version
    :ivar relink: whether to relink supplier of datasets to better-fitted suppliers
    :ivar vehicle_type: "two-wheeler", "car", "bus" or "truck"
    :ivar has_fleet: whether `vehicle_type` has associated fleet data or not

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
        relink: bool,
        vehicle_type: str,
        has_fleet: bool,
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
            index,
        )
        self.version = version
        self.relink = relink
        self.vehicle_type = vehicle_type
        self.has_fleet = has_fleet
        self.database = database
        self.mapping = get_vehicles_mapping()

        self.activity_mapping = InventorySet(
            database=database, version=version, model=model
        )
        self.vehicle_map = self.activity_mapping.generate_transport_map(
            transport_type=vehicle_type,
        )
        self.rev_map = {}
        for k, v in self.vehicle_map.items():
            for x in v:
                self.rev_map[x["name"]] = k

        self.vehicle_fuel_map = self.activity_mapping.generate_vehicle_fuel_map(
            transport_type=vehicle_type
        )

        self.battery_size = get_battery_size()

        # check if vehicle map is empty
        for v in self.vehicle_map.values():
            if not v:
                print(f"Vehicle map is empty for {self.vehicle_type}.")

    def regionalize_transport_datasets(self):
        """
        Regionalize transport datasets, which are currently only available in RER, CA and RoW.
        """

        # create and regionalize transport datasets
        self.process_and_add_activities(
            mapping=self.vehicle_map,
            efficiency_adjustment_fn=self.adjust_transport_efficiency,
        )

    def create_vehicle_markets(self) -> list:
        """
        Create vehicle market (fleet average) datasets.
        """

        name = f"market for {self.mapping[self.vehicle_type]['name']}"
        reference_product = self.mapping[self.vehicle_type]["name"]
        unit = self.mapping[self.vehicle_type]["unit"]

        self.process_and_add_markets(
            name=name,
            reference_product=reference_product,
            unit=unit,
            mapping=self.vehicle_map,
            system_model=self.system_model,
            production_volumes=self.iam_data.production_volumes,
        )

        # if trucks, build size-specific markets
        if self.vehicle_type == "truck":
            for size in self.mapping[self.vehicle_type]["sizes"]:
                new_name = f"{name}, {size}"
                production_volumes = self.iam_data.production_volumes.sel(
                    variables=[
                        v
                        for v in self.iam_data.production_volumes.coords[
                            "variables"
                        ].values
                        if size in v
                    ]
                )

                if production_volumes.size == 0:
                    continue

                self.process_and_add_markets(
                    name=new_name,
                    reference_product=reference_product,
                    unit=unit,
                    mapping=self.vehicle_map,
                    system_model=self.system_model,
                    production_volumes=production_volumes,
                )

        # if trucks, adjust battery size
        if self.vehicle_type == "truck":
            for ds in ws.get_many(
                self.database,
                ws.contains("name", "battery electric"),
                ws.contains("name", "truck"),
                ws.equals("unit", "unit"),
            ):
                self.adjust_battery_size(ds)

    def relink_transport_datasets(self):
        # if trucks or ships, need to reconnect everything
        # loop through datasets that use truck transport

        if "old" in self.mapping[self.vehicle_type]:
            for dataset in ws.get_many(
                self.database,
                ws.exclude(ws.contains("unit", "kilometer")),
            ):
                for exc in ws.technosphere(
                    dataset,
                    ws.either(
                        *[
                            ws.equals("name", v)
                            for v in self.mapping[self.vehicle_type]["old"]
                        ]
                    ),
                    ws.equals("unit", "ton kilometer"),
                ):

                    new_name = self.mapping[self.vehicle_type]["old"][exc["name"]][
                        self.model
                    ]
                    new_loc = self.geo.ecoinvent_to_iam_location(dataset["location"])
                    exc["name"] = new_name
                    exc["product"] = self.mapping[self.vehicle_type]["name"]
                    exc["location"] = new_loc

    def adjust_transport_efficiency(self, dataset, technology=None):
        """
        Adjust transport efficiency of transport datasets based on IAM data.

        :param dataset: dataset to adjust
        :return: dataset with adjusted transport efficiency
        """

        if self.vehicle_type == "car":
            data = self.iam_data.passenger_car_efficiencies
        elif self.vehicle_type == "truck":
            data = self.iam_data.road_freight_efficiencies
        elif self.vehicle_type == "bus":
            data = self.iam_data.bus_efficiencies
        elif self.vehicle_type == "train":
            data = self.iam_data.rail_freight_efficiencies
        elif self.vehicle_type == "two-wheeler":
            data = self.iam_data.two_wheelers_efficiencies
        elif self.vehicle_type == "ship":
            data = self.iam_data.sea_freight_efficiencies
        else:
            raise ValueError("Unknown vehicle type.")

        if data is None:
            return dataset

        variable = self.rev_map[dataset["name"]]

        if variable in data.coords["variables"].values:
            scaling_factor = 1 / self.find_iam_efficiency_change(
                data=data,
                variable=variable,
                location=dataset["location"],
            )
        else:
            # if not found, we assume that the efficiency is 1
            scaling_factor = 1

        if scaling_factor != 1:
            dataset = rescale_exchanges(
                dataset,
                scaling_factor,
                technosphere_filters=[
                    ws.either(
                        *[
                            ws.contains("name", v["name"])
                            for v in self.vehicle_fuel_map[variable]
                        ]
                    )
                ],
            )

            dataset.setdefault("log parameters", {}).update(
                {"efficiency change": scaling_factor}
            )

            txt = f" Fuel/energy efficiency adjusted by a factor of {scaling_factor} according to the scenario."
            if "comment" not in dataset:
                dataset["comment"] = txt
            else:
                dataset["comment"] += txt

        self.write_log(dataset)

        return dataset

    def adjust_battery_size(self, ds):
        """
        Adjust battery size for truck datasets.
        """

        # detect size in name
        size = [s for s in self.battery_size["truck"] if s in ds["name"]][0]

        if self.year <= min(self.battery_size["truck"][size].keys()):
            mean_battery_size = self.battery_size["truck"][size][
                min(self.battery_size["truck"][size].keys())
            ]["mean"]
            min_battery_size = self.battery_size["truck"][size][
                min(self.battery_size["truck"][size].keys())
            ]["min"]
            max_battery_size = self.battery_size["truck"][size][
                min(self.battery_size["truck"][size].keys())
            ]["max"]
        elif self.year >= max(self.battery_size["truck"][size].keys()):
            mean_battery_size = self.battery_size["truck"][size][
                max(self.battery_size["truck"][size].keys())
            ]["mean"]
            min_battery_size = self.battery_size["truck"][size][
                max(self.battery_size["truck"][size].keys())
            ]["min"]
            max_battery_size = self.battery_size["truck"][size][
                max(self.battery_size["truck"][size].keys())
            ]["max"]
        else:
            mean_battery_size = np.interp(
                self.year,
                list(self.battery_size["truck"][size].keys()),
                [v["mean"] for v in self.battery_size["truck"][size].values()],
            )
            min_battery_size = np.interp(
                self.year,
                list(self.battery_size["truck"][size].keys()),
                [v["min"] for v in self.battery_size["truck"][size].values()],
            )
            max_battery_size = np.interp(
                self.year,
                list(self.battery_size["truck"][size].keys()),
                [v["max"] for v in self.battery_size["truck"][size].values()],
            )

        for exc in ws.technosphere(ds, ws.contains("name", "market for battery")):
            exc["amount"] = mean_battery_size
            exc["uncertainty type"] = 5
            exc["loc"] = float(exc["amount"])
            exc["minimum"] = float(min_battery_size)
            exc["maximum"] = float(max_battery_size)

        ds["comment"] = f" Battery size adjusted to {mean_battery_size} kWh."

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('efficiency change', '')}"
        )
