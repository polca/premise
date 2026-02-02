import xarray as xr

from .hydrogen import HydrogenMixin
from .biogas import BiogasMixin
from .biofuels import BiofuelsMixin
from .liquid_fuels import SyntheticFuelsMixin
from .markets import FuelMarketsMixin
from .utils import fetch_mapping
from .config import FUEL_GROUPS
from ..transformation import (
    BaseTransformation,
)
from ..validation import FuelsValidation
from ..activity_maps import InventorySet
from ..inventory_imports import get_biosphere_code
from ..logger import create_logger

logger = create_logger("fuel")


def _update_fuels(scenario, version, system_model):

    fuels = Fuels(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    if any(
        x is not None
        for x in (
            scenario["iam data"].petrol_blend,
            scenario["iam data"].diesel_blend,
            scenario["iam data"].natural_gas_blend,
            scenario["iam data"].hydrogen_blend,
        )
    ):
        fuels.generate_hydrogen_activities()
        fuels.generate_synthetic_fuel_activities()
        fuels.generate_biogas_activities()
        fuels.relink_datasets()
        fuels.fix_fuel_market_locations()  # Fix fuel market location mismatches
        scenario["database"] = fuels.database
        scenario["cache"] = fuels.cache
        scenario["index"] = fuels.index

    else:
        print("No fuel scenario data available -- skipping")

    validate = FuelsValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=fuels.database,
        iam_data=scenario["iam data"],
    )

    validate.run_fuel_checks()

    return scenario


class Fuels(
    HydrogenMixin,
    BiogasMixin,
    BiofuelsMixin,
    SyntheticFuelsMixin,
    FuelMarketsMixin,
    BaseTransformation,
):
    """
    Combined class that inherits all fuel-related mixins and BaseTransformation.
    This class can be used as a drop-in replacement for the original Fuels class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize any additional attributes
        # or methods specific to Fuels class
        self.cached_suppliers = {}
        self.mapping = InventorySet(self.database)
        self.fuel_map = self.mapping.generate_fuel_map(model=self.model)

        self.rev_fuel_map = {
            activity["name"]: fuel
            for fuel, activities in self.fuel_map.items()
            for activity in activities
        }
        self.fuel_groups = fetch_mapping(FUEL_GROUPS)
        self.biosphere_flows = get_biosphere_code(self.version)
        self.iam_fuel_markets = self.iam_data.production_volumes.sel(
            variables=[
                g
                for g in [
                    item
                    for sublist in list(self.fuel_groups.values())
                    for item in sublist
                ]
                if g
                in self.iam_data.production_volumes.coords["variables"].values.tolist()
            ]
        )

        self.fuel_efficiencies = xr.DataArray(
            dims=["variables"], coords={"variables": []}
        )
        for efficiency in [
            self.iam_data.petrol_technology_efficiencies,
            self.iam_data.diesel_technology_efficiencies,
            self.iam_data.gas_technology_efficiencies,
            self.iam_data.hydrogen_technology_efficiencies,
        ]:
            if efficiency is not None:
                self.fuel_efficiencies = xr.concat(
                    [self.fuel_efficiencies, efficiency],
                    dim="variables",
                )

        self.new_fuel_markets = {}

    def fix_fuel_market_locations(self):
        """
        Fix references to fuel markets that have been relocated to IAM regions.
        
        After fuel market creation, many ecoinvent datasets still reference fuel markets
        in their original ecoinvent locations (CH, RER, ZA, etc.) but these markets
        now exist in IAM regions. This method updates those references.
        """
        from ..transformation import ws
        
        # List of fuel market names that were relocated
        fuel_market_names = [
            "market for diesel",
            "market for diesel, low-sulfur",
            "market group for diesel, low-sulfur",
            "market group for diesel",
            "market for petrol",
            "market for petrol, low-sulfur",
            "market for petrol, unleaded",
            "market for natural gas, high pressure",
            "market group for natural gas, high pressure",
            "market for natural gas, low pressure",
            "market for hydrogen, gaseous, low pressure",
            "market for kerosene",
            "market for liquefied petroleum gas",
        ]
        
        # Build a set of available fuel markets (name, location) for quick lookup
        available_fuel_markets = set()
        for ds in self.database:
            if ds.get("name") in fuel_market_names:
                available_fuel_markets.add((ds["name"], ds["location"]))
        
        # Count of fixes for reporting
        fix_count = 0
        
        # Update all datasets that reference fuel markets
        for dataset in self.database:
            for exc in ws.technosphere(dataset):
                exc_name = exc.get("name", "")
                exc_location = exc.get("location", "")
                
                # Check if this is a fuel market reference
                if exc_name in fuel_market_names:
                    # Check if the referenced market exists at that location
                    if (exc_name, exc_location) not in available_fuel_markets:
                        # Market doesn't exist at this location - need to redirect
                        
                        # Get the IAM region for this dataset
                        if dataset["location"] in self.regions:
                            iam_location = dataset["location"]
                        else:
                            iam_location = self.geo.ecoinvent_to_iam_location(dataset["location"])
                        
                        # Check if market exists in the IAM region
                        if (exc_name, iam_location) in available_fuel_markets:
                            exc["location"] = iam_location
                            fix_count += 1
                        # Otherwise try GLO
                        elif (exc_name, "GLO") in available_fuel_markets:
                            exc["location"] = "GLO"
                            fix_count += 1
                        # Last resort: try World
                        elif (exc_name, "World") in available_fuel_markets:
                            exc["location"] = "World"
                            fix_count += 1
        
        if fix_count > 0:
            print(f"Fixed {fix_count} fuel market location references")

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('initial amount of fossil CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('new amount of fossil CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('new amount of biogenic CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('initial energy input for hydrogen production', '')}|"
            f"{dataset.get('log parameters', {}).get('new energy input for hydrogen production', '')}|"
            f"{dataset.get('log parameters', {}).get('fuel conversion efficiency', '')}|"
            f"{dataset.get('log parameters', {}).get('land footprint', '')}|"
            f"{dataset.get('log parameters', {}).get('land use CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('fossil CO2 per kg fuel', '')}|"
            f"{dataset.get('log parameters', {}).get('non-fossil CO2 per kg fuel', '')}|"
            f"{dataset.get('log parameters', {}).get('lower heating value', '')}"
        )
