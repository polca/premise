import xarray as xr

from ..activity_maps import InventorySet
from ..inventory_imports import get_biosphere_code
from ..logger import create_logger
from ..transformation import BaseTransformation
from ..validation import FuelsValidation
from .biofuels import BiofuelsMixin
from .biogas import BiogasMixin
from .config import FUEL_GROUPS
from .hydrogen import HydrogenMixin
from .liquid_fuels import SyntheticFuelsMixin
from .markets import FuelMarketsMixin
from .utils import fetch_mapping

logger = create_logger("fuel")

HYDROGEN_LOG_COLUMNS = [
    "hydrogen report type",
    "hydrogen sector",
    "hydrogen subsector",
    "hydrogen demand node type",
    "hydrogen demand nodes",
    "hydrogen demand nodes rounded up",
    "hydrogen demand t per year",
    "hydrogen demand t per node per year",
    "hydrogen demand t per node per day",
    "hydrogen distribution compressed gaseous truck",
    "hydrogen distribution compressed gaseous pipeline",
    "hydrogen distribution liquid truck",
    "hydrogen exchange location",
    "hydrogen exchange amount",
    "old generic hydrogen market",
    "new sector specific hydrogen market",
]


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
        try:
            fuels.set_hydrogen_logistics()
            scenario["hydrogen demand nodes"] = fuels.hydrogen_demand_nodes
            fuels.write_hydrogen_demand_node_logs()
        except Exception as exc:
            print(f"Could not create hydrogen demand nodes analysis: {exc}")

        fuels.generate_hydrogen_activities()
        fuels.relink_hydrogen_consumers_to_sector_markets()
        fuels.write_hydrogen_sector_market_relink_logs()
        fuels.generate_synthetic_fuel_activities()
        fuels.generate_biogas_activities()
        fuels.relink_datasets()
        scenario["database"] = fuels.database
        scenario["cache"] = fuels.cache
        scenario["index"] = fuels.index
        scenario["unmatched hydrogen consumers"] = (
            fuels.unmatched_hydrogen_consumers
        )
        scenario["hydrogen consumers matched to sector markets"] = (
            fuels.matched_hydrogen_consumers
        )
        scenario["hydrogen consumers kept on general market"] = (
            fuels.skipped_hydrogen_consumers
        )
        scenario["generated hydrogen sector markets"] = getattr(
            fuels, "generated_hydrogen_sector_markets", []
        )
        scenario["generated hydrogen sector market regions"] = getattr(
            fuels, "generated_hydrogen_sector_market_regions", {}
        )
        scenario["skipped hydrogen sector markets"] = getattr(
            fuels, "skipped_hydrogen_sector_markets", []
        )

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
                in self.iam_data.production_volumes.coords[
                    "variables"
                ].values.tolist()
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
        hydrogen_log_parameters = dataset.get("log parameters", {})

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
            f"{dataset.get('log parameters', {}).get('lower heating value', '')}|"
            f"{self._format_hydrogen_log_parameters(hydrogen_log_parameters)}"
        )

    @staticmethod
    def _format_log_value(value):
        if value is None:
            return ""
        try:
            if value != value:
                return ""
        except (TypeError, ValueError):
            pass
        if isinstance(value, (list, tuple, set)):
            return ", ".join(str(item) for item in value)
        return str(value).replace("|", "/")

    @classmethod
    def _format_hydrogen_log_parameters(cls, parameters):
        return "|".join(
            cls._format_log_value(parameters.get(column))
            for column in HYDROGEN_LOG_COLUMNS
        )

    def _write_hydrogen_log(self, status, dataset, parameters):
        dataset = {
            "name": dataset.get("name", ""),
            "location": dataset.get("location", ""),
            "log parameters": parameters,
        }
        self.write_log(dataset, status=status)

    def write_hydrogen_demand_node_logs(self):
        demand_nodes = getattr(self, "hydrogen_demand_nodes", None)
        if demand_nodes is None or getattr(demand_nodes, "empty", True):
            return

        for row in demand_nodes.to_dict(orient="records"):
            parameters = {
                "hydrogen report type": "demand node",
                "hydrogen sector": row.get("sector"),
                "hydrogen subsector": row.get("subsector"),
                "hydrogen demand node type": row.get("demand_node_type"),
                "hydrogen demand nodes": row.get("demand_nodes"),
                "hydrogen demand nodes rounded up": row.get(
                    "demand_nodes_rounded_up"
                ),
                "hydrogen demand t per year": row.get(
                    "hydrogen_demand_t_per_year"
                ),
                "hydrogen demand t per node per year": row.get(
                    "hydrogen_demand_t_per_node_per_year"
                ),
                "hydrogen demand t per node per day": row.get(
                    "hydrogen_demand_t_per_node_per_day"
                ),
                "hydrogen distribution compressed gaseous truck": row.get(
                    "compressed_gaseous_truck"
                ),
                "hydrogen distribution compressed gaseous pipeline": row.get(
                    "compressed_gaseous_pipeline"
                ),
                "hydrogen distribution liquid truck": row.get(
                    "liquid_hydrogen_truck"
                ),
            }
            dataset = {
                "name": "hydrogen demand nodes",
                "location": row.get("region", ""),
            }
            self._write_hydrogen_log(
                status="created (hydrogen demand node)",
                dataset=dataset,
                parameters=parameters,
            )

    def write_hydrogen_sector_market_relink_logs(self):
        matched_consumers = getattr(
            self, "matched_hydrogen_consumers", []
        )
        for consumer in matched_consumers:
            parameters = {
                "hydrogen report type": "sector market relink",
                "hydrogen sector": consumer.get("sector"),
                "hydrogen exchange location": consumer.get(
                    "hydrogen exchange location"
                ),
                "hydrogen exchange amount": consumer.get(
                    "hydrogen exchange amount"
                ),
                "old generic hydrogen market": consumer.get(
                    "old generic hydrogen market"
                ),
                "new sector specific hydrogen market": consumer.get(
                    "new sector specific hydrogen market"
                ),
            }
            dataset = {
                "name": consumer.get("name", ""),
                "location": consumer.get("location", ""),
            }
            self._write_hydrogen_log(
                status="updated (hydrogen sector market relink)",
                dataset=dataset,
                parameters=parameters,
            )
