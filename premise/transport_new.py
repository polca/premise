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

from .logger import create_logger
from .transformation import BaseTransformation, IAMDataCollection, List
from .utils import rescale_exchanges
from wurst import searching as ws

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
    
    if scenario["iam data"].transport_markets is not None:
        logger.info("TESTING: transport markets found in IAM data")
        transport.generate_datasets()
        transport.generate_transport_markets()
        transport.relink_datasets()
        scenario["database"] = transport.database 
        scenario["cache"] = transport.cache
        scenario["index"] = transport.index
        # TODO: insert transport validation here?
        
    else:
        print("No transport markets found in IAM data. Skipping.")
        logger.info("TESTING: No transport markets found in IAM data.")
        
    return scenario


def delete_inventory_datasets(database):
    """
    The function specifies and deletes inventory datasets.
    In this case transport datasets from ecoinvent, as they
    are replaced by the additional LCI imports.
    """
    
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
        
        # clean the database of ecoinvent datasets, so that only add. inventory imports remain
        self.database = delete_inventory_datasets(self.database)
        
        freight_train_datasets = ws.get_many(
            self.database,
            ws.contains("name", "transport, freight train"), # to be changed when including road freight transport
            ws.equals("unit", "ton kilometer"),
        )
        freight_train_dataset_names = list(set([dataset['name'] for dataset in freight_train_datasets]))
        
        # for d in freight_train_datasets:
        #     logger.info(f"Dataset name: {d['name']} in {d['location']}")
        
        changed_datasets_location = []
        new_datasets = []
        
        # change the location of the datasets to IAM regions
        for dataset in self.database:
            if dataset["name"] in freight_train_dataset_names:
                if dataset["location"] != "RoW":
                    region_mapping = self.region_to_proxy_dataset_mapping(
                        name=dataset["name"],
                        ref_prod=dataset["reference product"],
                    )
                    ecoinv_region = dataset["location"]
                    
                    # logger.info(f"Dataset: {dataset['name']} in {ecoinv_region}")
                    
                    for IAM_reg, eco_reg in region_mapping.items():
                        if eco_reg == ecoinv_region:
                            dataset["location"] = IAM_reg
                            break
                        
                    # logger.info(f"Modified dataset: {dataset['name']} in {dataset['location']}")
                    
                    changed_datasets_location.append(dataset["location"])
                    # logger.info(f"Changedd dataset locations: {changed_datasets_location}")
                    
                    self.adjust_transport_efficiency(dataset)

        # create new datasets for IAM regions that are not covered yet, based on the "RoW" dataset
        for region in self.iam_data.regions:
            if region not in changed_datasets_location and region != "World":
                for freight_train in freight_train_dataset_names:
                    new_dataset = copy.deepcopy(ws.get_one(self.database,
                                                           ws.equals("name", freight_train), 
                                                           ws.equals("location", "RoW"))
                                                )
                    new_dataset["location"] = region
                    new_dataset["code"] = str(uuid.uuid4().hex)
                    new_dataset["comment"] = f"Dataset for the region {region}. {new_dataset['comment']}"
                    
                    # add to log
                    self.write_log(dataset=new_dataset, status="updated")
                    # add it to list of created datasets
                    self.add_to_index(new_dataset)
                    
                    # logger.info(f"New dataset: {new_dataset['name']} in {new_dataset['location']}")
                    
                    self.adjust_transport_efficiency(new_dataset)
                    
                    new_datasets.append(new_dataset)
                    
        self.database.extend(new_datasets)
        
    def adjust_transport_efficiency(self, dataset):
        """
        The function updates the efficiencies of transport datasets
        using the transport_efficiencies, created in data_collection.py.
        """
        # logger.info("TESTING: adjust_transport_efficiency function is called")
        
        # create a list that contains all biosphere flows that are related to the direct combustion of diesel
        list_biosphere_flows = [ # to be added: biosphere fuel combustion emission from trucks
            "Ammonia",
            "Benzene",
            "Cadmium II",
            "Carbon dioxide, fossil",
            "Carbon monoxide, fossil",
            "Chromium III",
            "Copper ion",
            "Dinitrogen monoxide",
            "Lead II",
            "Mercury II",
            "Methane, fossil",
            "NMVOC, non-methane volatile organic compounds",
            "Nickel II",
            "Nitrogen oxides",
            "Particulate Matter, < 2.5 um",
            "Particulate Matter, > 10 um",
            "Particulate Matter, > 2.5 um and < 10um",
            "Selenium IV",
            "Sulfur dioxide",
            "Toluene",
            "Xylenes, unspecified",
            "Zinc II"
        ]
        
        scaling_factor = 1 / self.find_iam_efficiency_change(
            data=self.iam_data.transport_efficiencies,
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
                    ws.either(*[ws.contains("name", x) for x in ["electricity", "diesel", "hydrogen"]]) # TODO: apply diesel efficiency increase to diesel shunting for electricity and hydrogen datasets
                ], # to be adapted for fuels that trucks use (make a fuel list out of it, maybe even yaml?)
                biosphere_filters=[ws.contains("name", x) for x in list_biosphere_flows],
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
      
        freight_train_datasets = ws.get_many(
            self.database,
            ws.contains("name", "transport, freight train"), # to be changed when including road freight transport
            ws.equals("unit", "ton kilometer"),
        )
        freight_train_dataset_names = list(set([dataset['name'] for dataset in freight_train_datasets]))
        
        # logger.info(f"Freight train datasets: {freight_train_dataset_names}")
        
        list_transport_markets = [] # create empty list to store the market processes
        
        # create regional market processes
        for region in self.iam_data.regions:
            market = {
                "name": "market for transport, freight train", # to be changed when making it mode agnostic
                "reference product": "transport, freight train",
                "unit": "ton kilometer",
                "location": region,
                "exchanges": [
                    {
                        "name": "market for transport, freight train",
                        "product": "transport, freight train",
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
                for vehicle in freight_train_dataset_names:
                    
                    market_share = self.iam_data.transport_markets.sel(region=region, variables=vehicle, year=self.year).item()
                        
                    # logger.info(f"Market share for {vehicle} in {region}: {market_share}")
                    
                    if market_share > 0:
                        market["exchanges"].append(
                            {
                                "name": vehicle,
                                "product": "transport, freight train",
                                "unit": "ton kilometer",
                                "location": region,
                                "type": "technosphere",
                                "amount": market_share,
                            }
                        )
            
            
            # logger.info(f"Market: {market['name']} in {market['location']} with exchanges: {market['exchanges']}")

            if len(market["exchanges"]) > 1:# not needed
                list_transport_markets.append(market)
        
        self.database.extend(list_transport_markets)
        
        #list_transport_ES_var = ["ES|Transport|Freight|Rail"] #to be adapted for raod
        dict_regional_shares = {}
        
        for region in self.iam_data.regions:
            if region != "World":
                dict_regional_shares[region] = (
                    self.iam_data.data.sel(
                        region=region, 
                        variables="ES|Transport|Freight|Rail", 
                        year=self.year).values
                )/(
                    self.iam_data.data.sel(
                        region="World", 
                        variables="ES|Transport|Freight|Rail", 
                        year=self.year).item()
                )
        logger.info(f"Regional shares: {dict_regional_shares}")
        
        # create World market process
        world_market = {
            "name": "market for transport, freight train", # to be changed when making it mode agnostic
            "reference product": "transport, freight train",
            "unit": "ton kilometer",
            "location": "World",
            "exchanges": [
                    {
                        "name": "market for transport, freight train",
                        "product": "transport, freight train",
                        "unit": "ton kilometer",
                        "location": "World",
                        "type": "production",
                        "amount": 1,
                    }
                ],
            "code": str(uuid.uuid4().hex),
            "database": "premise",
            "comment": f"Fleet-average vehicle for the year {self.year}, "
            f"for the region World.",
        }
            
        for region in self.iam_data.regions:
            logger.info(f"Region: {region}")
            if region != "World":
                world_market["exchanges"].append(
                    {
                        "name": "market for transport, freight train",
                        "product": "transport, freight train",
                        "unit": "ton kilometer",
                        "location": region,
                        "type": "technosphere",
                        "amount": dict_regional_shares[region],
                    }
                )
        logger.info(f"World market: {world_market['name']} in {world_market['location']} with exchanges: {world_market['exchanges']}")

        self.database.extend([world_market])