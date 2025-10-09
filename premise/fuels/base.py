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
        scenario["database"] = fuels.database
        scenario["cache"] = fuels.cache
        scenario["index"] = fuels.index

        if "mapping" not in scenario:
            scenario["mapping"] = {}
        scenario["mapping"]["fuels"] = fuels.fuel_map

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
