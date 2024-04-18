"""
transport_new.py contains the class Transport, which imports inventories
for different modes and types of transport, updates efficiencies, and creates
fleet average vehicle inventories based on IAM data. The inventories are 
integrated afterwards into the database.   
"""

import numpy as np
import xarray as xr
import yaml

from .transformation import BaseTransformation, IAMDataCollection, List
from .utils import rescale_exchanges

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
    
    if scenario["iam data"].transport_markets is not None:
        transport.adjust_transport_efficiencies()
        transport.generate_transport_markets()
        transport.relink_datasets()
        scenario["database"] = transport.database 
        scenario["cache"] = transport.cache
        scenario["index"] = transport.index
        # TODO: insert transport validation here?
        
    else:
        print("No transport markets found in IAM data. Skipping.")
        
    return scenario


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
    
    def adjust_transport_efficiency(self, database):
        """
        The function updates the efficiencies of transport datasets
        using the transport_efficiencies, created in data_collection.py.
        """
            
    def generate_transport_markets(self, database, ):
        """
        Function that creates market processes and adds them to the database.
        It calculates the share of inputs to each market process and 
        creates the process by multiplying the share with the amount of reference product, 
        assigning it to the respective input.
        """