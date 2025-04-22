import xarray as xr

from .hydrogen import HydrogenMixin
from .biogas import BiogasMixin
from .biofuels import BiofuelsMixin
from .synfuels import SyntheticFuelsMixin
from .markets import FuelMarketsMixin
from .utils import fetch_mapping
from .config import FUEL_GROUPS
from ..transformation import (
    BaseTransformation,
    get_shares_from_production_volume,
    get_suppliers_of_a_region,
)
from ..validation import FuelsValidation
from ..activity_maps import InventorySet
from ..inventory_imports import get_biosphere_code


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
        fuels.generate_fuel_markets()
        fuels.relink_datasets()
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
        # Initialize any additional attributes or methods specific to Fuels class
        self.cached_suppliers = {}
        self.mapping = InventorySet(self.database)
        self.fuel_map = self.mapping.generate_fuel_map()
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

    def find_suppliers(
        self,
        name: str,
        ref_prod: str,
        unit: str,
        loc: str,
        exclude=None,
        subset: list = None,
    ):
        """
        Return a list of potential suppliers given a name, reference product,
        unit and location, with their respective supply share (based on production volumes).

        :param name: the name of the activity
        :param ref_prod: the reference product of the activity
        :param unit: the unit of the activity
        :param loc: the location of the activity
        :param exclude: a list of activities to exclude from the search
        :param subset: a list of activities to search in
        :return: a dictionary of potential suppliers with their respective supply share
        """

        # if we find a result in the cache dictionary, return it
        if exclude is None:
            exclude = []
        key = (name, ref_prod, loc)

        if key in self.cached_suppliers:
            return self.cached_suppliers[key]

        ecoinvent_regions = self.geo.iam_to_ecoinvent_location(loc)
        # search first for suppliers in `loc`, but also in the ecoinvent
        # locations to are comprised in `loc`, and finally in `RER`, `RoW` and `GLO`,
        possible_locations = [
            [loc],
            ecoinvent_regions,
            ["RER"],
            ["RoW"],
            ["GLO"],
            ["CH"],
        ]
        suppliers, counter = [], 0

        # while we do not find a result
        while len(suppliers) == 0:
            try:
                suppliers = list(
                    get_suppliers_of_a_region(
                        database=subset or self.database,
                        locations=possible_locations[counter],
                        names=[name] if isinstance(name, str) else name,
                        reference_prod=ref_prod,
                        unit=unit,
                        exclude=exclude,
                    )
                )
                counter += 1
            except IndexError as err:
                raise IndexError(
                    f"Could not find any supplier for {name} {ref_prod} in {possible_locations}."
                ) from err

        suppliers = [s for s in suppliers if s]  # filter out empty lists

        # find production volume-based share
        suppliers = get_shares_from_production_volume(suppliers)

        # store the result in cache for next time
        self.cached_suppliers[key] = suppliers

        return suppliers
