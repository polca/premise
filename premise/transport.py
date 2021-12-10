import wurst
from .transformation import *
from .inventory_imports import VariousVehicles
from .utils import *
from .ecoinvent_modification import INVENTORY_DIR
import numpy as np
import yaml

FILEPATH_FLEET_COMP = (
    DATA_DIR / "iam_output_files" / "fleet_files" / "fleet_all_vehicles.csv"
)
FILEPATH_TWO_WHEELERS = INVENTORY_DIR / "lci-two_wheelers.xlsx"
FILEPATH_TRUCKS = INVENTORY_DIR / "lci-trucks.xlsx"
FILEPATH_BUSES = INVENTORY_DIR / "lci-buses.xlsx"
FILEPATH_PASS_CARS = INVENTORY_DIR / "lci-pass_cars.xlsx"
FILEPATH_TRUCK_LOAD_FACTORS = DATA_DIR / "transport" / "avg_load_factors.yaml"
FILEPATH_VEHICLES_MAP = DATA_DIR / "transport" / "vehicles_map.yaml"


def get_average_truck_load_factors():
    with open(FILEPATH_TRUCK_LOAD_FACTORS, "r") as stream:
        out = yaml.safe_load(stream)
    return out


def get_vehicles_mapping():
    with open(FILEPATH_VEHICLES_MAP, "r") as stream:
        out = yaml.safe_load(stream)
    return out


def normalize_exchange_amounts(list_act):

    for act in list_act:
        total = 0
        for exc in act["exchanges"]:
            if exc["type"] == "technosphere":
                total += exc["amount"]

        for exc in act["exchanges"]:
            if exc["type"] == "technosphere":
                exc["amount"] /= total

    return list_act


def create_fleet_vehicles(
    datasets, regions_mapping, vehicle_type, year, model, scenario, regions
):
    print("Create fleet average vehicles...")

    if not FILEPATH_FLEET_COMP.is_file():
        raise FileNotFoundError("The fleet composition file could not be found.")

    dataframe = pd.read_csv(FILEPATH_FLEET_COMP, sep=";")
    dataframe["region"] = dataframe["region"].map(regions_mapping)
    dataframe = dataframe.loc[~dataframe["region"].isnull()]

    arr = (
        dataframe.groupby(["year", "region", "powertrain", "construction_year", "size"])
        .sum()["vintage_demand_vkm"]
        .to_xarray()
    )
    arr = arr.fillna(0)

    vehicles_map = get_vehicles_mapping()

    constr_year_map = {
        y: int(y.split("-")[-1]) for y in arr.coords["construction_year"].values
    }

    # fleet data does not go below 2015
    if year < 2015:
        year = 2015
        print(
            "Vehicle fleet data is not available before 2015. "
            "Hence, 2015 is used as fleet year."
        )

    # fleet data does not go beyond 2050
    if year > 2050:
        year = 2050
        print(
            "Vehicle fleet data is not available beyond 2050. "
            "Hence, 2050 is used as fleet year."
        )

    # We filter electric vehicles by year of manufacture
    available_years = np.arange(2000, 2055, 5)
    ref_year = min(available_years, key=lambda x: abs(x - year))

    available_ds, d_names = [], {}

    for ds in datasets:
        if ds["name"].startswith("transport, "):
            if vehicle_type == "bus":
                if len(ds["name"].split(", ")) == 6:
                    if "battery electric" in ds["name"].split(", ")[2]:
                        _, _, pwt, _, size, y = ds["name"].split(", ")

                    else:
                        _, _, pwt, size, y, _ = ds["name"].split(", ")
                else:
                    _, _, pwt, size, y = ds["name"].split(", ")

            elif vehicle_type == "truck":
                if len(ds["name"].split(", ")) == 8:
                    if "battery electric" in ds["name"].split(", ")[3]:
                        _, _, _, pwt, _, size, y, type = ds["name"].split(", ")

                    else:
                        _, _, _, pwt, size, y, _, type = ds["name"].split(", ")
                else:
                    _, _, _, pwt, size, y, type = ds["name"].split(", ")

                size = size.replace(" gross weight", "")

            else:
                if len(ds["name"].split(", ")) == 6:
                    _, _, pwt, _, size, y = ds["name"].split(", ")
                else:
                    _, _, pwt, size, y = ds["name"].split(", ")

            if vehicle_type == "truck":
                d_names[(vehicles_map["powertrain"][pwt], size, int(y), type)] = (
                    ds["name"],
                    ds["reference product"],
                    ds["unit"],
                )
                available_ds.append((vehicles_map["powertrain"][pwt], size, int(y)))

            else:
                d_names[(vehicles_map["powertrain"][pwt], size, int(y))] = (
                    ds["name"],
                    ds["reference product"],
                    ds["unit"],
                )
                available_ds.append((vehicles_map["powertrain"][pwt], size, int(y)))

    list_act = []

    # average load factors for trucks
    # to convert vkm to tkm
    avg_load = get_average_truck_load_factors()

    # add missing IMAGE regions
    d_missing_regions = {
        "BRA": "RSAM",
        "CEU": "WEU",
        "CAN": "OCE",
        "KOR": "SEAS",
        "SAF": "WAF",
        "RUS": "UKR",
        "INDO": "SEAS",
        "ME": "TUR",
        "RSAF": "WAF",
        "EAF": "WAF",
        "MEX": "RSAM",
        "NAF": "WAF",
        "RCAM": "RSAM",
        "RSAS": "SEAS",
        "STAN": "TUR",
    }

    for region in regions:

        if region not in arr.coords["region"].values:
            fleet_region = d_missing_regions[region]
        else:
            fleet_region = region

        sel = arr.sel(
            region=fleet_region, size=vehicles_map[vehicle_type]["sizes"], year=ref_year
        )
        total_km = sel.sum()

        if total_km > 0:

            if vehicle_type == "truck":
                driving_cycles = ["urban delivery", "regional delivery", "long haul"]
            else:
                driving_cycles = [""]

            for driving_cycle in driving_cycles:
                name = (
                    f"{vehicles_map[vehicle_type]['name']}, unspecified, {driving_cycle}"
                    if driving_cycle == "truck"
                    else f"{vehicles_map[vehicle_type]['name']}, unspecified"
                )
                act = {
                    "name": name,
                    "reference product": vehicles_map[vehicle_type]["name"],
                    "unit": vehicles_map[vehicle_type]["unit"],
                    "location": region,
                    "exchanges": [
                        {
                            "name": name,
                            "product": vehicles_map[vehicle_type]["name"],
                            "unit": vehicles_map[vehicle_type]["unit"],
                            "location": region,
                            "type": "production",
                            "amount": 1,
                        }
                    ],
                    "code": str(uuid.uuid4().hex),
                    "database": eidb_label(model, scenario, year),
                    "comment": f"Fleet-average vehicle for the year {year}, for the region {region}."
                }

                for s in vehicles_map[vehicle_type]["sizes"]:
                    for y in sel.coords["construction_year"].values:
                        for pt in sel.coords["powertrain"].values:
                            indiv_km = sel.sel(
                                size=s, construction_year=y, powertrain=pt
                            )
                            if (
                                indiv_km > 0
                                and (pt, s, constr_year_map[y]) in available_ds
                            ):
                                indiv_share = (indiv_km / total_km).values.item(0)

                                if vehicle_type == "truck":
                                    load = avg_load[vehicle_type][driving_cycle][s]
                                    to_look_for = (
                                        pt,
                                        s,
                                        constr_year_map[y],
                                        driving_cycle,
                                    )
                                else:
                                    load = 1
                                    to_look_for = (pt, s, constr_year_map[y])

                                if to_look_for in d_names:

                                    name, ref, unit = d_names[to_look_for]

                                    act["exchanges"].append(
                                        {
                                            "name": name,
                                            "product": ref,
                                            "unit": unit,
                                            "location": region,
                                            "type": "technosphere",
                                            "amount": indiv_share * load,
                                        }
                                    )

                if len(act["exchanges"]) > 1:
                    list_act.append(act)

    return normalize_exchange_amounts(list_act)


class Transport(BaseTransformation):
    """
    Class that modifies transport markets in ecoinvent based on IAM output data.

    :ivar database: database dictionary from :attr:`.NewDatabase.database`
    :vartype database: dict
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :vartype model: str
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :vartype iam_data: xarray.DataArray
    :ivar year: year, from :attr:`.NewDatabase.year`
    :vartype year: int
    
    """

    def __init__(
        self,
        database,
        iam_data,
        model,
        pathway,
        year,
        version,
        relink,
        vehicle_type,
        has_fleet,
    ):
        super().__init__(database, iam_data, model, pathway, year)
        self.version = version
        self.relink = relink
        self.vehicle_type = vehicle_type
        self.has_fleet = has_fleet

    def generate_vehicles_datasets(self):

        if self.vehicle_type == "car":
            fp = FILEPATH_PASS_CARS
        elif self.vehicle_type == "truck":
            fp = FILEPATH_TRUCKS
        elif self.vehicle_type == "bus":
            fp = FILEPATH_BUSES
        else:
            fp = FILEPATH_TWO_WHEELERS

        various_veh = VariousVehicles(
            database=self.database,
            version_in="3.7",
            version_out=self.version,
            path=fp,
            year=self.year,
            regions=self.regions,
            model=self.model,
            scenario=self.scenario,
            vehicle_type=self.vehicle_type,
            relink=False,
            has_fleet=True,
        )

        various_veh.prepare_inventory()

        return various_veh

    def create_vehicle_markets(self):

        # create datasets
        datasets = self.generate_vehicles_datasets()

        list_vehicles = [
            "Bicycle,",
            "Kick-scooter,",
            "Moped,",
            "Scooter,",
            "Motorbike,",
            "urban delivery",
            "regional delivery",
            "long haul",
            "passenger bus",
        ]

        # We filter  vehicles by year of manufacture
        available_years = [2020, 2030, 2040, 2050]
        closest_year = min(available_years, key=lambda x: abs(x - self.year))

        if self.has_fleet:

            # the fleet data is originally defined for REMIND regions
            if self.model != "remind":
                region_map = {
                    self.geo.iam_to_iam_region(loc): loc for loc in self.regions
                }
            else:
                region_map = {loc: loc for loc in self.regions}

            datasets.import_db.data = [
                x
                for x in datasets.import_db.data
                if not any(y in x["name"].lower() for y in list_vehicles)
                or (
                    not any(
                        z for z in re.findall(r"\d+", x["name"]) if int(z) > self.year
                    )
                    and "label-certified electricity" not in x["name"]
                )
            ]

            fleet_act = create_fleet_vehicles(
                datasets.import_db.data,
                regions_mapping=region_map,
                vehicle_type=self.vehicle_type,
                year=self.year,
                model=self.model,
                scenario=self.scenario,
                regions=self.regions,
            )

            datasets.import_db.data.extend(fleet_act)

        else:

            datasets.import_db.data = [
                x
                for x in datasets.import_db.data
                if not any(y in x["name"] for y in list_vehicles)
                or (
                    str(closest_year) in x["name"]
                    and "label-certified electricity" not in x["name"]
                )
            ]

            # remove the year in the name
            str_to_replace = ", " + str(closest_year)
            for ds in datasets.import_db.data:
                if str_to_replace in ds["name"]:
                    ds["name"] = ds["name"].replace(str_to_replace, "")
                    for exc in ds["exchanges"]:
                        if str_to_replace in exc["name"]:
                            exc["name"] = exc["name"].replace(str_to_replace, "")

        list_new_ds = []

        # create regional variants
        for dataset in datasets.import_db.data:
            if (
                "transport, " in dataset["name"]
                and "unspecified" not in dataset["name"]
            ):
                for region in self.regions:
                    new_ds = wt.copy_to_new_location(dataset, region)

                    for exc in ws.production(new_ds):
                        if "input" in exc:
                            exc.pop("input")

                    if self.relink:
                        new_ds = relink_technosphere_exchanges(
                            new_ds, self.database, self.model, iam_regions=self.regions
                        )

                    list_new_ds.append(new_ds)

        datasets.import_db.data.extend(list_new_ds)

        # remove empty fields
        for x in datasets.import_db.data:
            for k, v in list(x.items()):
                if not v:
                    del x[k]

        # if trucks, need to reconnect everything
        # loop through datasets that use lorry transport
        if self.vehicle_type == "truck":
            for dataset in ws.get_many(
                self.database,
                ws.doesnt_contain_any("name", ["freight, lorry"]),
                ws.exclude(ws.equals("unit", "ton kilometer")),
            ):
                for exc in ws.technosphere(
                    dataset,
                    ws.contains("name", "transport, freight, lorry"),
                    ws.equals("unit", "ton kilometer"),
                ):

                    if "input" in exc:
                        del exc["input"]

                    if dataset["unit"] == "kilogram":
                        if exc["amount"] * 1000 <= 150:
                            exc[
                                "name"
                            ] = "transport, freight, lorry, unspecified, urban delivery"

                        elif 150 < exc["amount"] * 1000 <= 450:
                            exc[
                                "name"
                            ] = "transport, freight, lorry, unspecified, regional delivery"

                        else:
                            exc[
                                "name"
                            ] = "transport, freight, lorry, unspecified, long haul"
                    else:
                        exc[
                            "name"
                        ] = "transport, freight, lorry, unspecified, long haul"

                    exc["product"] = "transport, freight, lorry"
                    exc["location"] = self.geo.ecoinvent_to_iam_location(
                        dataset["location"]
                    )

        self.database = datasets.merge_inventory()

        return self.database
