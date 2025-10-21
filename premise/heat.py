"""
Integrates projections regarding heat production and supply.
"""

from .activity_maps import InventorySet
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    List,
    ws,
)
from .validation import HeatValidation
from .inventory_imports import get_biosphere_code

logger = create_logger("heat")


def _update_heat(scenario, version, system_model):

    heat = Heat(
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

    heat.fetch_fuel_market_co2_emissions()
    heat.regionalize_activities()
    heat.adjust_carbon_dioxide_emissions()

    if scenario["iam data"].buildings_heating_mix is not None:
        heat.create_heat_markets(
            technologies=[
                tech
                for tech in heat.iam_data.buildings_heating_mix.variables.values
                if "buildings" in tech.lower()
            ],
            name="market for heat, for buildings",
            reference_product="heat, central or small-scale",
        )
        heat.relink_heat_markets(
            current_input=[
                {
                    "name": "market for heat, central or small-scale, other than natural gas",
                    "reference product": "heat, central or small-scale, other than natural gas",
                },
                {
                    "name": "market for heat, central or small-scale, biomethane",
                    "reference product": "heat, central or small-scale, biomethane",
                },
                {
                    "name": "market for heat, central or small-scale, Jakobsberg",
                    "reference product": "heat, central or small-scale, Jakobsberg",
                },
                {
                    "name": "market for heat, central or small-scale, natural gas",
                    "reference product": "heat, central or small-scale, natural gas",
                },
                {
                    "name": "market for heat, central or small-scale, natural gas and heat pump, Jakobsberg",
                    "reference product": "heat, central or small-scale, natural gas and heat pump, Jakobsberg",
                },
                {
                    "name": "market for heat, central or small-scale, natural gas, Jakobsberg",
                    "reference product": "heat, central or small-scale, natural gas, Jakobsberg",
                },
            ],
            new_input={
                "name": "market for heat, for buildings",
                "reference product": "heat, central or small-scale",
            },
        )
    else:
        print("No buildings heat scenario data available -- skipping")

    if scenario["iam data"].industrial_heat_mix is not None:
        heat.create_heat_markets(
            technologies=[
                tech
                for tech in heat.iam_data.industrial_heat_mix.variables.values
                if "industrial" in tech.lower()
            ],
            name="market for heat, district or industrial",
            reference_product="heat, district or industrial",
        )
        heat.relink_heat_markets(
            current_input=[
                {
                    "name": "market for heat, district or industrial, natural gas",
                    "reference product": "heat, district or industrial, natural gas",
                },
                {
                    "name": "market group for heat, district or industrial, natural gas",
                    "reference product": "heat, district or industrial, natural gas",
                },
                {
                    "name": "market for heat, district or industrial, other than natural gas",
                    "reference product": "heat, district or industrial, other than natural gas",
                },
                {
                    "name": "market group for heat, district or industrial, other than natural gas",
                    "reference product": "heat, district or industrial, other than natural gas",
                },
                {
                    "name": "market for heat, from steam, in chemical industry",
                    "reference product": "heat, from steam, in chemical industry",
                },
            ],
            new_input={
                "name": "market for heat, district or industrial",
                "reference product": "heat, district or industrial",
            },
        )
    else:
        print("No industrial heat scenario data available -- skipping")

    if scenario["iam data"].daccs_energy_use is not None:
        heat.create_heat_markets(
            technologies=[
                tech for tech in heat.iam_data.daccs_energy_use.variables.values
            ],
            name="market for energy, for direct air capture and storage",
            reference_product="energy, for direct air capture and storage",
        )
    else:
        print("No DAC energy mix data available -- skipping")

    if scenario["iam data"].ewr_energy_use is not None:
        heat.create_heat_markets(
            technologies=[
                tech for tech in heat.iam_data.ewr_energy_use.variables.values
            ],
            name="market for energy, for enhanced rock weathering",
            reference_product="energy, for enhanced rock weathering",
        )
    else:
        print("No EWR energy mix data available -- skipping")

    heat.relink_datasets()

    validate = HeatValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=heat.database,
        iam_data=scenario["iam data"],
    )

    validate.run_heat_checks()

    scenario["database"] = heat.database
    scenario["cache"] = heat.cache
    scenario["index"] = heat.index

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["heat"] = heat.heat_techs

    return scenario


class Heat(BaseTransformation):
    """
    Class that modifies fuel inventories and markets
    in ecoinvent based on IAM output data.
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
        cache: dict = None,
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
            cache,
            index,
        )

        self.carbon_intensity_markets = {}
        self.mapping = InventorySet(self.database)
        self.heat_techs = self.mapping.generate_heat_map(model=self.model)
        self.biosphere_flows = get_biosphere_code(self.version)

    def fetch_fuel_market_co2_emissions(self):
        """
        Fetch CO2 emissions from fuel markets.
        """
        fuel_markets = [
            "market for diesel, low-sulfur",
            "market for petrol, low-sulfur",
            "market for natural gas, high pressure",
        ]

        for dataset in ws.get_many(
            self.database,
            ws.either(*[ws.equals("name", n) for n in fuel_markets]),
        ):
            if "log parameters" in dataset:
                if "fossil CO2 per kg fuel" in dataset["log parameters"]:
                    self.carbon_intensity_markets[
                        (dataset["name"], dataset["location"])
                    ] = {"fossil": dataset["log parameters"]["fossil CO2 per kg fuel"]}
                if "non-fossil CO2 per kg fuel" in dataset["log parameters"]:
                    self.carbon_intensity_markets[
                        (dataset["name"], dataset["location"])
                    ].update(
                        {
                            "non-fossil": dataset["log parameters"][
                                "non-fossil CO2 per kg fuel"
                            ]
                        }
                    )

        # add "market for diesel" to self.carbon_intensity_markets
        # by duplicating the "market for low-sulfur" entries
        # add "market for natural gas, low pressure" to self.carbon_intensity_markets
        # by duplicating the "market for natural gas, high pressure" entries

        new_keys = {}
        for key, value in self.carbon_intensity_markets.items():
            if key[0] == "market for diesel, low-sulfur":
                new_keys[("market for diesel", key[1])] = value
                new_keys[("market group for diesel", key[1])] = value
                new_keys[("market group for diesel, low-sulfur", key[1])] = value
            if key[0] == "market for petrol, low-sulfur":
                new_keys[("market for petrol", key[1])] = value
                new_keys[("market for petrol, unleaded", key[1])] = value
            if key[0] == "market for natural gas, high pressure":
                new_keys[("market group for natural gas, high pressure", key[1])] = (
                    value
                )
                new_keys[("market for natural gas, low pressure", key[1])] = value

        self.carbon_intensity_markets.update(new_keys)

    def regionalize_activities(self):

        production_volumes_vars = [
            v
            for v in self.heat_techs.keys()
            if v in self.iam_data.production_volumes.coords["variables"].values
        ]

        production_volumes = None
        if production_volumes_vars:
            production_volumes = self.iam_data.production_volumes.sel(
                variables=production_volumes_vars
            )

        self.process_and_add_activities(
            mapping=self.heat_techs,
            production_volumes=production_volumes,
        )
        self.heat_techs = self.mapping.generate_heat_map(model=self.model)

    def adjust_carbon_dioxide_emissions(self):
        """
        Regionalize heat production.

        """

        for heat_tech, heat_datasets in self.heat_techs.items():
            for dataset in heat_datasets:
                fossil_co2, non_fossil_co2 = 0.0, 0.0
                for exc in ws.technosphere(dataset):
                    if (
                        exc["name"],
                        exc["location"],
                    ) in self.carbon_intensity_markets:
                        fossil_co2 += (
                            exc["amount"]
                            * self.carbon_intensity_markets[
                                (exc["name"], exc["location"])
                            ]["fossil"]
                        )

                        non_fossil_co2 += (
                            exc["amount"]
                            * self.carbon_intensity_markets[
                                (exc["name"], exc["location"])
                            ]["non-fossil"]
                        )

                if fossil_co2 + non_fossil_co2 > 0:

                    initial_fossil_co2 = sum(
                        [
                            exc["amount"]
                            for exc in ws.biosphere(dataset)
                            if exc["name"] == "Carbon dioxide, fossil"
                        ]
                    )
                    initial_non_fossil_co2 = sum(
                        [
                            exc["amount"]
                            for exc in ws.biosphere(dataset)
                            if exc["name"] == "Carbon dioxide, non-fossil"
                        ]
                    )

                    dataset["exchanges"] = [
                        e
                        for e in dataset["exchanges"]
                        if e["name"]
                        not in (
                            "Carbon dioxide, fossil",
                            "Carbon dioxide, non-fossil",
                        )
                    ]

                    if fossil_co2 > 0:
                        dataset["exchanges"].append(
                            {
                                "uncertainty type": 0,
                                "loc": float(fossil_co2),
                                "amount": float(fossil_co2),
                                "name": "Carbon dioxide, fossil",
                                "categories": ("air",),
                                "type": "biosphere",
                                "unit": "kilogram",
                                "input": (
                                    "biosphere3",
                                    self.biosphere_flows[
                                        (
                                            "Carbon dioxide, fossil",
                                            "air",
                                            "unspecified",
                                            "kilogram",
                                        )
                                    ],
                                ),
                            }
                        )

                    if non_fossil_co2 > 0:

                        dataset["exchanges"].append(
                            {
                                "uncertainty type": 0,
                                "loc": float(non_fossil_co2),
                                "amount": float(non_fossil_co2),
                                "name": "Carbon dioxide, non-fossil",
                                "categories": ("air",),
                                "type": "biosphere",
                                "unit": "kilogram",
                                "input": (
                                    "biosphere3",
                                    self.biosphere_flows[
                                        (
                                            "Carbon dioxide, non-fossil",
                                            "air",
                                            "unspecified",
                                            "kilogram",
                                        )
                                    ],
                                ),
                            }
                        )

                    dataset.setdefault("log parameters", {})[
                        "initial amount of fossil CO2"
                    ] = initial_fossil_co2
                    dataset["log parameters"]["new amount of fossil CO2"] = float(
                        fossil_co2
                    )
                    dataset["log parameters"][
                        "initial amount of biogenic CO2"
                    ] = initial_non_fossil_co2
                    dataset["log parameters"]["new amount of biogenic CO2"] = float(
                        non_fossil_co2
                    )

    def create_heat_markets(
        self,
        technologies,
        name,
        reference_product,
    ):

        # Get the possible names of ecoinvent datasets
        ecoinvent_technologies = {
            technology: self.heat_techs[technology] for technology in technologies
        }

        self.process_and_add_markets(
            name=name,
            reference_product=reference_product,
            unit="megajoule",
            mapping=ecoinvent_technologies,
            production_volumes=self.iam_data.production_volumes,
            system_model=self.system_model,
        )

    def relink_heat_markets(self, current_input: list, new_input: dict):

        for dataset in self.database:
            for exc in ws.technosphere(
                dataset,
                ws.either(
                    *[ws.equals("name", n) for n in [x["name"] for x in current_input]]
                ),
                ws.either(
                    *[
                        ws.equals("product", n["reference product"])
                        for n in current_input
                    ]
                ),
            ):
                exc["name"] = new_input["name"]
                exc["product"] = new_input["reference product"]
                exc["location"] = (
                    self.ecoinvent_to_iam_loc[dataset["location"]]
                    if dataset["location"] not in self.regions
                    else dataset["location"]
                )
                if "input" in exc:
                    del exc["input"]

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('initial amount of fossil CO2')}|"
            f"{dataset.get('log parameters', {}).get('new amount of fossil CO2')}|"
            f"{dataset.get('log parameters', {}).get('initial amount of biogenic CO2')}|"
            f"{dataset.get('log parameters', {}).get('new amount of biogenic CO2')}"
        )
