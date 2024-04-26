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
        # transport.generate_transport_markets()
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
        freight_traindataset_names = [dataset['name'] for dataset in freight_train_datasets]
        
        # for d in freight_train_datasets:
        #     logger.info(f"Dataset name: {d['name']} in {d['location']}")
        
        new_datasets = []
        
        for dataset in self.database:
            if dataset["name"] in freight_traindataset_names:
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
                    
                    self.adjust_transport_efficiency(dataset)

        # code to be created: needs to create new inventories for missing IAM regions
        
        # for d in freight_train_datasets:
        #     logger.info(f"Dataset name: {d['name']} in {d['location']}")
            # for region in self.iam_data.regions:
            #     if region is not 
            #         new_dataset = dataset.copy()
            #         new_dataset["location"] = region
            #         new_dataset["code"] = str(uuid.uuid4().hex)
            #         new_dataset["comment"] = f"Dataset for the region {region}. {dataset['comment']}"
                    
            #         # add to log
            #         self.write_log(dataset=new_dataset, status="updated")
            #         # add it to list of created datasets
            #         self.add_to_index(new_dataset)
                    
            #         logger.info(f"New dataset: {new_dataset['name']} in {new_dataset['location']}")
            #         new_datasets.append(new_dataset)
            
            
        # TODO: create add market inventories and add them to the freight
        

                # self.adjust_transport_efficiency(dataset)
        
        

    
    def adjust_transport_efficiency(self, dataset):
        """
        The function updates the efficiencies of transport datasets
        using the transport_efficiencies, created in data_collection.py.
        """
        logger.info("TESTING: adjust_transport_efficiency function is called")
        
        # create a list that contains all biosphere flows that are related to the direct combustion of diesel
        list_biosphere_flows = [
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
            data=self.iam_data.transport_efficiencies, # TODO: efficiencies are so far only 1.0 values, wait on response Romain
            variable=dataset["name"],
            location=dataset["location"],
        )
        logger.info(f"Scaling factor: {scaling_factor} for dataset {dataset['name']} in {dataset['location']}")
        
        if scaling_factor is None:
            scaling_factor = 1
            
        if scaling_factor != 1 and scaling_factor > 0:
            rescale_exchanges(
                dataset,
                scaling_factor,
                technosphere_filters=[
                    ws.either(*[ws.contains("name", x) for x in ["electricity", "diesel", "hydrogen"]]) # TODO: apply diesel efficiency increase to diesel shunting for electricity and hydrogen datasets
                ],
                biosphere_filters=[ws.contains("name", x) for x in list_biosphere_flows],
                remove_uncertainty=False,
            )
            logger.info(f"Dataset {dataset['name']} in {dataset['location']} has been updated.")
            
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

            dataset["log parameters"].update(
                {
                    "efficiency change": scaling_factor,
                }
            )
            
    def generate_transport_markets(self):
        """
        Function that creates market processes and adds them to the database.
        It calculates the share of inputs to each market process and 
        creates the process by multiplying the share with the amount of reference product, 
        assigning it to the respective input.
        """
      
        vehicle_types = [
            "transport, freight train, diesel-electric",
            "transport, freight train, diesel-electric",
            "transport, freight train, electric",
            "transport, freight train, fuel cell, hydrogen",
        ]
        
        iam_variables = [
            "ES|Transport|Freight|Rail|Liquids|Fossil",
            "ES|Transport|Freight|Rail|Liquids|Biomass",
            "ES|Transport|Freight|Rail|Electric",
            "ES|Transport|Freight|Rail|Liquids|Hydrogen",
        ]
        
        list_transport_vehicles = list(zip(vehicle_types, iam_variables)) # find a better way, and include road freight
        
        list_transport_markets = [] # create empty list to store the market processes
        
        for region in self.iam_data.regions:
            # create regional market processes
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
            
            for vehicle, variable in list_transport_vehicles:
                if vehicle == "transport, freight train, diesel-electric":
                    amount1 = self.iam_data.transport_markets.sel(region=region, variable=variable, year=self.year).item()
                    amount2 = self.iam_data.transport_markets.sel(region=region, variable=variable, year=self.year).item()
                    amount = amount1 + amount2
                else:
                    amount = self.iam_data.transport_markets.sel(region=region, variable=variable, year=self.year).item()
                    
                market["exchanges"].append(
                    {
                        "name": vehicle,
                        "product": "transport, freight train",
                        "unit": "ton kilometer",
                        "location": region,
                        "type": "technosphere",
                        "amount": amount,
                    }
                )
            
        if len(market["exchanges"]) > 1:# not needed
            list_transport_markets.append(market)
        
        # Remove duplicate exchanges
        list_transport_markets = [json.dumps(d) for d in list_transport_markets]
        list_transport_markets = list(set(list_transport_markets))
        list_transport_markets = [json.loads(d) for d in list_transport_markets]
        
        list_transport_markets = normalize_exchange_amounts(list_transport_markets)
            
        self.database.data.extend(list_transport_markets) # sure?