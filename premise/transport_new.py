"""
transport_new.py contains the class Transport, which imports inventories
for different modes and types of transport, updates efficiencies, and creates
fleet average vehicle inventories based on IAM data. The inventories are 
integrated afterwards into the database.   
"""

import copy
import json
import numpy as np
import uuid
import xarray as xr
import yaml
from typing import Dict, List

from .filesystem_constants import DATA_DIR
from .logger import create_logger
from .transformation import BaseTransformation, IAMDataCollection
from .utils import rescale_exchanges
from wurst import searching as ws
from wurst.errors import NoResults

FILEPATH_VEHICLES_MAP = DATA_DIR / "transport" / "vehicles_map_NEW.yaml"

logger = create_logger("transport")

def _update_transport(scenario, version, system_model):
    transport = Transport(
        database=scenario["database"],
        year=scenario["year"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        version=version,
        system_model=system_model,
        index=scenario.get("index"),
    )
    
    logger.info("TESTING: _update_transport is being called.")
    
    if scenario["iam data"].roadfreight_markets is not None and scenario["iam data"].railfreight_markets is not None:
        logger.info("TESTING: transport markets found in IAM data")
        transport.generate_datasets()
        transport.generate_transport_markets()
        transport.relink_datasets()
        transport.generate_unspecified_transport_vehicles()
        transport.relink_exchanges()
        # transport.relink_datasets() # TODO: understand what exactly this does?
        scenario["database"] = transport.database 
        scenario["cache"] = transport.cache
        scenario["index"] = transport.index
        # TODO: insert transport validation here?
    
    elif scenario["iam data"].roadfreight_markets is not None and scenario["iam data"].railfreight_markets is None:
        print("No railfreight markets found in IAM data. Skipping transport.")
        
    elif scenario["iam data"].roadfreight_markets is None and scenario["iam data"].railfreight_markets is not None:
        print("No roadfreight markets found in IAM data. Skipping transport.")
        
    else:
        print("No transport markets found in IAM data. Skipping transport.")
        
    # TODO: if one transport market is not found the other one will still be updateable?
    
    return scenario


def delete_inventory_datasets(database):
    """
    The function specifies and deletes inventory datasets.
    In this case transport datasets from ecoinvent, as they
    are replaced by the additional LCI imports.
    """
    # TODO: this must be a rpelacment function? What does relink datasets do?
    
    ds_to_delete = [ # solve via .yaml? to include road
        "transport, freight train",
        "market for transport, freight train",
        "market group for transport, freight train",
        "transport, freight train, diesel",
        "transport, freight train, electricity",
        "transport, freight train, steam",
        "transport, freight train, diesel, with particle filter",
    ]

    database = [
        ds
        for ds in database
        if ds["name"] not in ds_to_delete
    ]
    
    return database

def get_vehicles_mapping() -> Dict[str, dict]:
    """
    Return a dictionary that contains mapping
    between `ecoinvent` terminology and `premise` terminology
    regarding size classes, powertrain types, etc.
    :return: dictionary to map terminology between carculator and ecoinvent
    """
    with open(FILEPATH_VEHICLES_MAP, "r", encoding="utf-8") as stream:
        out = yaml.safe_load(stream)
        return out


def normalize_exchange_amounts(list_act: List[dict]) -> List[dict]:
    """
    In vehicle market datasets, we need to ensure that the total contribution
    of single vehicle types equal 1.

    :param list_act: list of transport market activities
    :return: same list, with activity exchanges normalized to 1

    """

    for act in list_act:
        total = 0
        for exc in act["exchanges"]:
            if exc["type"] == "technosphere":
                total += exc["amount"]

        for exc in act["exchanges"]:
            if exc["type"] == "technosphere":
                exc["amount"] /= total

    return list_act


class Transport(BaseTransformation):
    """
    Class that modifies transport inventory datasets based on IAM data.
    It updates efficiencies and creates fleet average vehicle inventories.
    TODO: more description
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
    
    def generate_datasets(self):
        """
        Function that creates inventories for IAM regions
        and deletes previous datasets with ecoinvent regions.
        """
        
        logger.info("TESTING: generate_datasets function is called")
        
        ##### delete
        # clean the database of ecoinvent datasets, so that only add. inventory imports remain
        # self.database = delete_inventory_datasets(self.database)
        
        roadfreight_dataset_names = self.iam_data.roadfreight_markets.coords["variables"].values.tolist()
        railfreight_dataset_names = self.iam_data.railfreight_markets.coords["variables"].values.tolist()
        freight_transport_dataset_names = roadfreight_dataset_names + railfreight_dataset_names
        # logger.info(f"Freight transport datasets: {freight_transport_dataset_names}")
        
        # test = ws.get_one(self.database, ws.equals("name", "transport, freight, lorry, battery electric, NMC-622 battery, 18t gross weight, long haul"))
        # logger.info(f"Test dataset: {test}")
        # for d in freight_train_datasets:
        #     logger.info(f"Dataset name: {d['name']} in {d['location']}")
        
        changed_datasets_location = []
        new_datasets = []
        
        # change the location of the datasets to IAM regions
        for dataset in self.database:
            # the if statement that checks if "train" is in dataset["name"] needs to be changed if road freight transport contains more than 1 regional dataset (right noe only RER)
            if "train" in dataset["name"] and dataset["name"] in freight_transport_dataset_names:
                if dataset["location"] != "RoW":
                    region_mapping = self.region_to_proxy_dataset_mapping(
                        name=dataset["name"],
                        ref_prod=dataset["reference product"],
                    )
                    # logger.info(f"Region mapping: {region_mapping}")
                    
                    ecoinv_region = dataset["location"]
                    
                    # logger.info(f"Dataset: {dataset['name']} in {ecoinv_region}")
                    
                    for IAM_reg, eco_reg in region_mapping.items():
                        if eco_reg == ecoinv_region:
                            dataset["location"] = IAM_reg
                            break
                        
                    # logger.info(f"Modified dataset: {dataset['name']} in {dataset['location']}")
                    
                    changed_datasets_location.append([dataset["name"],dataset["location"]])
                    # logger.info(f"Changedd dataset locations: {changed_datasets_location}")
                    
                    self.adjust_transport_efficiency(dataset)

        # create new datasets for IAM regions that are not covered yet, based on the "RoW" or "RER" dataset
        for region in self.iam_data.regions:
            for freight_transport_ds in freight_transport_dataset_names:
                if [freight_transport_ds, region] not in changed_datasets_location and region != "World":                  
                    # logger.info(f"Adjsuted region: {region}, dataset: {freight_transport_ds}")
                    try: # RoW dataset to be used for other IAM regions
                        new_dataset = copy.deepcopy(ws.get_one(self.database,
                                                                ws.equals("name", freight_transport_ds), 
                                                                ws.equals("location", "RoW")
                                                                )
                                                    )
                    except NoResults: # if no RoW dataset can be found use RER dataset
                        new_dataset = copy.deepcopy(ws.get_one(self.database,
                                                                ws.equals("name", freight_transport_ds), 
                                                                ws.equals("location", "RER")
                                                            )
                                                    )
                    new_dataset["location"] = region
                    new_dataset["code"] = str(uuid.uuid4().hex)
                    new_dataset["comment"] = f"Dataset for the region {region}. {new_dataset['comment']}"
                    
                    # add to log
                    self.write_log(dataset=new_dataset, status="updated")
                    # add it to list of created datasets
                    self.add_to_index(new_dataset)
                    
                    # logger.info(f"Newly created dataset: {new_dataset['name']} in {new_dataset['location']}")
                    
                    self.adjust_transport_efficiency(new_dataset)
                    
                    new_datasets.append(new_dataset)
                    
        self.database.extend(new_datasets)

        
    def adjust_transport_efficiency(self, dataset):
        """
        The function updates the efficiencies of transport datasets
        using the transport_efficiencies, created in data_collection.py.
        """
        # logger.info("TESTING: adjust_transport_efficiency function is called")
        
        # create a list that contains all energy carrier markets used in transport
        energy_carriers = [
            "market group for electricity",
            "market for electricity",
            "market group for diesel",
            "market for diesel",
            "market for hydrogen",
            "market for natural gas",
        ]
        
        # create a list that contains all biosphere flows that are related to the direct combustion of fuel
        fuel_combustion_emissions = [
            "Ammonia",
            "Benzene",
            "Cadmium",
            "Carbon dioxide, fossil",
            "Carbon monoxide, fossil",
            "Chromium",
            "Copper ion",
            "Dinitrogen monoxide",
            "Lead",
            "Mercury",
            "Methane, fossil",
            "NMVOC, non-methane volatile organic compounds",
            "NMVOC, non-methane volatile organic compounds, unspecified origin",
            "Nickel",
            "Nitrogen oxides",
            "PAH, polycyclic aromatic hydrocarbons",
            "Particulate Matter, < 2.5 um",
            "Particulates, < 2.5 um",
            "Particulate Matter, > 10 um",
            "Particulate Matter, > 2.5 um and < 10um",
            "Selenium",
            "Sulfur dioxide",
            "Toluene",
            "Xylenes, unspecified",
            "Zinc",
        ]
        
        if "lorry" in dataset["name"]:
            scaling_factor = 1 / self.find_iam_efficiency_change(
                data=self.iam_data.roadfreight_efficiencies,
                variable=dataset["name"],
                location=dataset["location"],
            )
        elif "train" in dataset["name"]:
            scaling_factor = 1 / self.find_iam_efficiency_change(
                data=self.iam_data.railfreight_efficiencies,
                variable=dataset["name"],
                location=dataset["location"],
            )
            
        # logger.info(f"Scaling factor: {scaling_factor} for dataset {dataset['name']} in {dataset['location']}")
        
        if scaling_factor is None:
            scaling_factor = 1
            
        if scaling_factor != 1 and scaling_factor > 0:
            rescale_exchanges(
                dataset,
                scaling_factor,
                technosphere_filters=[
                    ws.either(*[ws.contains("name", x) for x in energy_carriers]) # TODO: apply diesel efficiency increase to diesel shunting for electricity and hydrogen datasets
                ],
                biosphere_filters=[ws.contains("name", x) for x in fuel_combustion_emissions],
                remove_uncertainty=False,
            )
            # logger.info(f"Dataset {dataset['name']} in {dataset['location']} has been updated.")
            ########## how can there be a scaling factor for hydrogen if FE and ES variables are 0?
            
            # Update the comments
            text = (
                f"This dataset has been modified by `premise`, according to the energy transport "
                f"efficiencies indicated by the IAM model {self.model.upper()} for the IAM "
                f"region {dataset['location']} in {self.year}, following the scenario {self.scenario}. "
                f"The energy efficiency of the process has been improved by {int((1 - scaling_factor) * 100)}%."
            )
            dataset["comment"] = text + (dataset["comment"] if dataset["comment"] is not None else "")

            if "log parameters" not in dataset:
                dataset["log parameters"] = {}

            dataset["log parameters"].update({"efficiency change": scaling_factor,})
            
    def generate_transport_markets(self):
        """
        Function that creates market processes and adds them to the database.
        It calculates the share of inputs to each market process and 
        creates the process by multiplying the share with the amount of reference product, 
        assigning it to the respective input.
        """
        
        logger.info("TESTING: generate_transport_markets function is called")

        # freight_transport_dataset_names = self.iam_data.transport_markets.coords["variables"].values.tolist()
        
        # dict of transport markets to be created (keys) with inputs list (values)
        transport_markets_tbc = {
            "market for transport, freight, lorry": 
                self.iam_data.roadfreight_markets.coords["variables"].values.tolist(),
            "market for transport, freight train": 
                self.iam_data.railfreight_markets.coords["variables"].values.tolist(),
        }
        # logger.info(f"Transport markets to be created: {transport_markets_tbc}")
        
        # create empty list to store the newly created market processes
        new_transport_markets = []
        
        # create regional market processes
        
        for markets, vehicles in transport_markets_tbc.items():
            for region in self.iam_data.regions:
                market = {
                    "name": markets, 
                    "reference product": markets.replace("market for ", ""),
                    "unit": "ton kilometer",
                    "location": region,
                    "exchanges": [
                        {
                            "name": markets,
                            "product": markets.replace("market for ", ""),
                            "unit": "ton kilometer",
                            "location": region,
                            "type": "production",
                            "amount": 1,
                        }
                    ],
                    "code": str(uuid.uuid4().hex),
                    "database": "premise",
                    "comment": f"Fleet-average vehicle for the year {self.year}, "
                    f"for the region {region}.",
                }
                
                if region != "World":
                    for vehicle in vehicles:
                        if markets == "market for transport, freight, lorry":
                            market_share = self.iam_data.roadfreight_markets.sel(region=region, variables=vehicle, year=self.year).item() #TODO: check what the transport_markets are right now!
                        elif markets == "market for transport, freight train":
                            market_share = self.iam_data.railfreight_markets.sel(region=region, variables=vehicle, year=self.year).item()
                        # logger.info(f"Market share for {vehicle} in {region}: {market_share}")
                        
                        if market_share > 0:
                            market["exchanges"].append(
                                {
                                    "name": vehicle,
                                    "product": vehicle,
                                    "unit": "ton kilometer",
                                    "location": region,
                                    "type": "technosphere",
                                    "amount": market_share,
                                }
                                )
                
                
                # logger.info(f"Market: {market['name']} in {market['location']} with exchanges: {[exchange['name'] for exchange in market['exchanges']]} created")

                new_transport_markets.append(market)
        
        dict_transport_ES_var = {
            "market for transport, freight, lorry": "ES|Transport|Freight|Road",
            "market for transport, freight train": "ES|Transport|Freight|Rail",
        }
        
        dict_regional_shares = {}
        
        for market, var in dict_transport_ES_var.items():
            for region in self.iam_data.regions:
                if region != "World":
                    # logger.info(f"Region: {region}")
                    dict_regional_shares[region] = (
                        ( 
                         self.iam_data.data.sel(
                            region=region, 
                            variables=var, 
                            year=self.year).values
                        )/(
                        self.iam_data.data.sel(
                            region="World", 
                            variables=var, 
                            year=self.year).item()
                        )
                    )
            # logger.info(f"Regional shares: {dict_regional_shares}")
            
        for ds in new_transport_markets:
            if ds["location"] == "World":
                # logger.info(f"World market before exchanges added: {ds['name']} in {ds['location']} with exchanges: {[exchange['name'] for exchange in ds['exchanges']]}")
                for region in self.iam_data.regions:
                    if region != "World":
                        ds["exchanges"].append(
                            {
                                "name": ds["name"],
                                "product": ds["name"].replace("market for ", ""),
                                "unit": "ton kilometer",
                                "location": region,
                                "type": "technosphere",
                                "amount": dict_regional_shares[region],
                            }
                        )
                # logger.info(f"World market after exchanges added: {ds['name']} in {ds['location']} with exchanges: {[exchange['name'] for exchange in ds['exchanges']]} in excahnge location {[exchange['location'] for exchange in ds['exchanges']]}")
        
        self.database.extend(new_transport_markets)
            
    
    def generate_unspecified_transport_vehicles(self):
        """
        This function generates unspecified transport vehicles for the IAM regions.
        The unspecified datasets refer to a specific size of the vehicle but forms an average for powertrain technology.
        """

        logger.info("TESTING: generate_unspecified_transport_vehicles function is called")
        
        dict_transport_ES_var = {
            "market for transport, freight, lorry, 3.5t gross weight": "ES|Transport|Freight|Road|Truck (0-3.5t)",
            "market for transport, freight, lorry, 7.5t gross weight": "ES|Transport|Freight|Road|Truck (7.5t)",
            "market for transport, freight, lorry, 18t gross weight": "ES|Transport|Freight|Road|Truck (18t)",
            "market for transport, freight, lorry, 26t gross weight": "ES|Transport|Freight|Road|Truck (26t)",
            "market for transport, freight, lorry, 40t gross weight": "ES|Transport|Freight|Road|Truck (40t)",
        }
        
        dict_vehicle_types = { 
            "Electric": "transport, freight, lorry, battery electric, NMC-622 battery",
            "Liquids": "transport, freight, lorry, diesel",
            "Gases": "transport, freight, lorry, compressed gas",
            "FCEV": "transport, freight, lorry, fuel cell electric",               
        }

        weight_specific_ds = []
        
        for region in self.iam_data.regions:
            if region != "World":
                for market, var in dict_transport_ES_var.items():
                    vehicle_unspecified = {
                        "name": market, 
                        "reference product": market.replace("market for ", ""),
                        "unit": "ton kilometer",
                        "location": region,
                        "exchanges": [
                            {
                                "name": market,
                                "product": market.replace("market for ", ""),
                                "unit": "ton kilometer",
                                "location": region,
                                "type": "production",
                                "amount": 1,
                            }
                        ],
                        "code": str(uuid.uuid4().hex),
                        "database": "premise",
                        "comment": f"Fleet-average vehicle for the year {self.year}, "
                        f"for the region {region}.",
                    }
                    
                    for vehicle_types, names in dict_vehicle_types.items():
                        # if region not in dict_regional_weight_shares:
                        #     dict_regional_weight_shares[region] = {}
                        variable_key = var + "|" + vehicle_types
                        if variable_key in self.iam_data.data.variables:
                            regional_weight_shares = (
                                ( 
                                self.iam_data.data.sel(
                                    region=region, 
                                    variables=variable_key, 
                                    year=self.year).values
                                )/(
                                self.iam_data.data.sel(
                                    region=region, 
                                    variables=var, 
                                    year=self.year).item()
                                )
                            )
                            
                            vehicle_unspecified["exchanges"].append(
                                {
                                    "name": "transport, freight, lorry, " + names + market.replace("market for transport, freight, lorry", "") + ", long haul",
                                    "product": "transport, freight, lorry, " + names + market.replace("market for transport, freight, lorry", "") + ", long haul",
                                    "unit": "ton kilometer",
                                    "location": region,
                                    "type": "technosphere",
                                    "amount": regional_weight_shares,
                                }
                            )
                    
                    weight_specific_ds.append(vehicle_unspecified)
                    
                    # logger.info(f"Unspecified vehicle: {vehicle_unspecified['name']} in {vehicle_unspecified['location']} with exchanges: {[exchange['name'] for exchange in vehicle_unspecified['exchanges']]} created.")
                    
        self.database.extend(weight_specific_ds)
                                              
        #TODO: regional unspecified vehiels per driving cycle could have smae shares but are not used for markets?
        
    
    def relink_exchanges(self):
        """
        This function goes through all datasets in the database that use transport, freight as one of their exchanges.
        It replaced those "old" transport exchanges with the new transport inventories and the newly created transport markets.
        """
        
        logger.info("TESTING: relink_exchanges function is called")
        
        vehicles_map = get_vehicles_mapping()
        
        logger.info(f"Vehicles map: {vehicles_map}")
        
        for dataset in ws.get_many(
            self.database,
            ws.doesnt_contain_any("name", "transport, freight"),
            ws.exclude(ws.equals("unit", "ton kilometer")),
            ):
            
            logger.info(f"Dataset {dataset['name']} in {dataset['location']} is being checked for exchanges.")
            
            for exc in ws.technosphere(
                dataset,
                ws.contains("name", "transport, freight"),
                ws.equals("unit", "ton kilometer"),
                ):
                
                logger.info(f"Exchanged found for dataset {dataset['name']} in {dataset['location']} with exchange {exc['name']} in {exc['location']}")
                
                key = [
                    k for k in vehicles_map['freight transport'][self.model]
                    if k.lower() in exc["name"].lower()
                ][0]
                
                logger.info(f"After calling of key: Exchanged found for dataset {dataset['name']} in {dataset['location']} with exchange {exc['name']} in {exc['location']}")
                
                if "input" in exc:
                    del exc["input"]
                    
                exc["name"] = f"{vehicles_map['freight transport'][self.model][key]}"
                exc["location"] = self.geo.ecoinvent_to_iam_location(dataset["location"])
                
                logger.info(f"Exchanged updated for dataset {dataset['name']} in {dataset['location']} with exchange {exc['name']} in {exc['location']}")