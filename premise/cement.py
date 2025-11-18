"""
cement.py contains the class `Cement`, which inherits from `BaseTransformation`.
This class transforms the cement markets and clinker and cement production activities
of the wurst database, based on projections from the IAM scenario.
It eventually re-links all the cement-consuming activities (e.g., concrete production)
of the wurst database to the newly created cement markets.

"""

import copy
import uuid

from .export import biosphere_flows_dictionary
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    InventorySet,
    List,
    np,
    ws,
)
from .validation import CementValidation

logger = create_logger("cement")


def _update_cement(scenario, version, system_model):

    if scenario["iam data"].cement_technology_mix is None:
        print("No cement scenario data available -- skipping")
        return scenario

    cement = Cement(
        database=scenario["database"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    if scenario["iam data"].cement_technology_mix is not None:
        cement.create_cement_CCS_datasets()
        cement.create_clinker_technology_datasets()
        cement.replace_clinker_production_with_markets()
        cement.build_clinker_production_datasets()
        cement.create_clinker_market_datasets()
        cement.create_cement_production_datasets()
        cement.create_cement_market_datasets()
        cement.relink_datasets()
        scenario["database"] = cement.database
        scenario["index"] = cement.index
        scenario["cache"] = cement.cache

        validate = CementValidation(
            model=scenario["model"],
            scenario=scenario["pathway"],
            year=scenario["year"],
            regions=scenario["iam data"].regions,
            database=cement.database,
            iam_data=scenario["iam data"],
        )

        validate.run_cement_checks()
    else:
        print("No cement markets found in IAM data. Skipping.")

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["cement"] = cement.cement_map

    return scenario


class Cement(BaseTransformation):
    """
    Class that modifies clinker and cement production datasets in ecoinvent.
    It creates region-specific new clinker production datasets (and deletes the original ones).
    It adjusts the kiln efficiency based on the improvement indicated in the IAM file, relative to 2020.
    It adds CCS, if indicated in the IAM file.
    It creates regions-specific cement production datasets (and deletes the original ones).
    It adjusts electricity consumption in cement production datasets.
    It creates regions-specific cement market datasets (and deletes the original ones).


    :ivar database: wurst database, which is a list of dictionaries
    :ivar iam_data: IAM data
    :ivar model: name of the IAM model (e.g., "remind", "image")
    :ivar year: year of the pathway (e.g., 2030)
    :ivar version: version of ecoinvent database (e.g., "3.7")
    :ivar system_model: name of the system model (e.g., "attributional", "consequential")

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
        self.version = version

        mapping = InventorySet(self.database, self.version, self.model)
        self.cement_fuels_map: dict = mapping.generate_cement_fuels_map()
        self.cement_map = mapping.generate_cement_map()

        # reverse the fuel map to get a mapping from ecoinvent to premise
        self.fuel_map_reverse: dict = {}

        self.fuel_map: dict = mapping.generate_fuel_map()

        for key, value in self.fuel_map.items():
            for v in list(value):
                self.fuel_map_reverse[v["name"]] = key

        self.biosphere_dict = biosphere_flows_dictionary(self.version)

    def build_clinker_production_datasets(self) -> list:
        """
        Builds clinker production datasets for each IAM region.
        Adds CO2 capture and Storage, if needed.

        :return: a dictionary with IAM regions as keys
        and clinker production datasets as values.
        """

        self.process_and_add_activities(
            efficiency_adjustment_fn=self.adjust_process_efficiency,
            mapping=self.cement_map,
            production_volumes=self.iam_data.production_volumes,
        )

    def adjust_process_efficiency(self, dataset, technology):

        # from Kellenberger at al. 2007, the total energy
        # input per ton of clinker is 3.4 GJ/ton clinker
        current_energy_input_per_ton_clinker = 3400

        # calculate the scaling factor
        # the correction factor applied to hard coal input
        # we assume that any fuel use reduction would in priority
        # affect hard coal use

        scaling_factor = 1 / self.find_iam_efficiency_change(
            data=self.iam_data.cement_technology_efficiencies,
            variable=technology,
            location=dataset["location"],
        )

        new_energy_input_per_ton_clinker = 3400

        dataset.setdefault("log parameters", {})[
            "initial energy input per ton clinker"
        ] = current_energy_input_per_ton_clinker

        if not np.isnan(scaling_factor):
            # calculate new thermal energy
            # consumption per kg clinker
            new_energy_input_per_ton_clinker = (
                current_energy_input_per_ton_clinker * scaling_factor
            )
            # put a floor value of 3100 kj/kg clinker
            if new_energy_input_per_ton_clinker < 3100:
                new_energy_input_per_ton_clinker = 3100
            # and a ceiling value of 5000 kj/kg clinker
            elif new_energy_input_per_ton_clinker > 5000:
                new_energy_input_per_ton_clinker = 5000

            # but if efficient kiln,
            # set the energy input to 3000 kJ/kg clinker
            if technology.startswith("cement, dry feed rotary kiln, efficient"):
                new_energy_input_per_ton_clinker = 3000

            dataset["log parameters"]["new energy input per ton clinker"] = int(
                new_energy_input_per_ton_clinker
            )

            scaling_factor = (
                new_energy_input_per_ton_clinker / current_energy_input_per_ton_clinker
            )

            dataset["log parameters"]["energy scaling factor"] = scaling_factor

            # rescale hard coal consumption and related emissions
            coal_specs = self.fuels_specs["hard coal"]
            coal_lhv = coal_specs["lhv"]["value"]
            old_coal_input, new_coal_input = 0, 0
            for exc in ws.technosphere(
                dataset,
                ws.contains("name", "hard coal"),
            ):
                # in kJ
                old_coal_input = float(exc["amount"] * coal_lhv)
                # in MJ
                new_coal_input = old_coal_input - (
                    (
                        current_energy_input_per_ton_clinker
                        - new_energy_input_per_ton_clinker
                    )
                    / 1000
                )
                exc["amount"] = np.clip(new_coal_input / coal_lhv, 0, None)

            # rescale combustion-related fossil CO2 emissions
            for exc in ws.biosphere(
                dataset,
                ws.contains("name", "Carbon dioxide"),
            ):
                if exc["name"] == "Carbon dioxide, fossil":
                    dataset["log parameters"]["initial fossil CO2"] = float(
                        exc["amount"]
                    )
                    co2_reduction = (old_coal_input - new_coal_input) * coal_specs[
                        "co2"
                    ]
                    exc["amount"] -= co2_reduction
                    dataset["log parameters"]["new fossil CO2"] = float(exc["amount"])

                if exc["name"] == "Carbon dioxide, non-fossil":
                    dataset["log parameters"]["initial biogenic CO2"] = float(
                        exc["amount"]
                    )

        # add 0.005 kg/kg clinker of ammonia use for NOx removal
        # according to Muller et al., 2024
        for exc in ws.technosphere(
            dataset,
            ws.contains("name", "market for ammonia"),
        ):
            if technology == "cement, dry feed rotary kiln, efficient, with MEA CCS":
                exc["amount"] = 0.00662
            else:
                exc["amount"] = 0.005

        # reduce NOx emissions
        # according to Muller et al., 2024
        for exc in ws.biosphere(
            dataset,
            ws.contains("name", "Nitrogen oxides"),
        ):
            if technology in [
                "cement, dry feed rotary kiln, efficient, with on-site CCS",
                "cement, dry feed rotary kiln, efficient, with oxyfuel CCS",
            ]:
                exc["amount"] = 1.22e-5
            elif technology == "cement, dry feed rotary kiln, efficient, with MEA CCS":
                exc["amount"] = 3.8e-4
            else:
                exc["amount"] = 7.6e-4

        # reduce Mercury and SOx emissions
        # according to Muller et al., 2024
        if technology in [
            "cement, dry feed rotary kiln, efficient, with on-site CCS",
            "cement, dry feed rotary kiln, efficient, with oxyfuel CCS",
            "cement, dry feed rotary kiln, efficient, with MEA CCS",
        ]:
            for exc in ws.biosphere(
                dataset,
                ws.either(
                    *[
                        ws.contains("name", name)
                        for name in [
                            "Mercury",
                            "Sulfur dioxide",
                        ]
                    ]
                ),
            ):
                exc["amount"] *= 1 - 0.999

        # add CCS datasets
        ccs_datasets = {
            "cement, dry feed rotary kiln, efficient, with on-site CCS": {
                "name": "carbon dioxide, captured, at cement production plant, using direct separation",
                "reference product": "carbon dioxide, captured",
                "capture share": 0.95,  # 95% of process emissions (calcination) are captured
            },
            "cement, dry feed rotary kiln, efficient, with oxyfuel CCS": {
                "name": "carbon dioxide, captured, at cement production plant, using oxyfuel",
                "reference product": "carbon dioxide, captured",
                "capture share": 0.9,
            },
            "cement, dry feed rotary kiln, efficient, with MEA CCS": {
                "name": "carbon dioxide, captured, at cement production plant, using monoethanolamine",
                "reference product": "carbon dioxide, captured",
                "capture share": 0.9,
            },
        }

        if technology in ccs_datasets:
            CO2_amount = sum(
                e["amount"]
                for e in ws.biosphere(
                    dataset,
                    ws.contains("name", "Carbon dioxide"),
                )
            )
            if (
                technology
                == "cement, dry feed rotary kiln, efficient, with on-site CCS"
            ):
                # only 95% of process emissions (calcination) are captured
                CCS_amount = 0.543 * ccs_datasets[technology]["capture share"]
            else:
                CCS_amount = CO2_amount * ccs_datasets[technology]["capture share"]

            dataset["log parameters"]["carbon capture rate"] = CCS_amount / CO2_amount

            ccs_exc = {
                "uncertainty type": 0,
                "loc": float(CCS_amount),
                "amount": float(CCS_amount),
                "type": "technosphere",
                "production volume": 0,
                "name": ccs_datasets[technology]["name"],
                "unit": "kilogram",
                "location": dataset["location"],
                "product": ccs_datasets[technology]["reference product"],
            }
            dataset["exchanges"].append(ccs_exc)

            # Update CO2 exchanges
            for exc in ws.biosphere(
                dataset,
                ws.contains("name", "Carbon dioxide, fossil"),
            ):
                if (
                    technology
                    != "cement, dry feed rotary kiln, efficient, with on-site CCS"
                ):
                    exc["amount"] *= (CO2_amount - CCS_amount) / CO2_amount
                else:
                    exc["amount"] -= CCS_amount

                # make sure it's not negative
                if exc["amount"] < 0:
                    exc["amount"] = 0

                dataset["log parameters"]["new fossil CO2"] = exc["amount"]

            # Update biogenic CO2 exchanges
            if (
                technology
                != "cement, dry feed rotary kiln, efficient, with on-site CCS"
            ):
                for exc in ws.biosphere(
                    dataset,
                    ws.contains("name", "Carbon dioxide, non-fossil"),
                ):
                    dataset["log parameters"]["initial biogenic CO2"] = float(
                        exc["amount"]
                    )
                    exc["amount"] *= (CO2_amount - CCS_amount) / CO2_amount

                    # make sure it's not negative
                    if exc["amount"] < 0:
                        exc["amount"] = 0

                    dataset["log parameters"]["new biogenic CO2"] = exc["amount"]

                    biogenic_CO2_reduction = (
                        dataset["log parameters"]["initial biogenic CO2"]
                        - dataset["log parameters"]["new biogenic CO2"]
                    )
                    # add a flow of "Carbon dioxide, in air" to reflect
                    # the permanent storage of biogenic CO2
                    dataset["exchanges"].append(
                        {
                            "uncertainty type": 0,
                            "loc": float(biogenic_CO2_reduction),
                            "amount": float(biogenic_CO2_reduction),
                            "type": "biosphere",
                            "name": "Carbon dioxide, in air",
                            "unit": "kilogram",
                            "categories": (
                                "natural resource",
                                "in air",
                            ),
                            "comment": "Permanent storage of biogenic CO2",
                            "input": (
                                "biosphere3",
                                self.biosphere_dict[
                                    (
                                        "Carbon dioxide, in air",
                                        "natural resource",
                                        "in air",
                                        "kilogram",
                                    )
                                ],
                            ),
                        }
                    )

        return dataset

    def replace_clinker_production_with_markets(self):
        """
        Some cement production datasets in ecoinvent receive an input from clinker production datasets.
        This is problematic because it will not benefit from the new cement markets, containing alternative clinker production pathways.
        So we replace the clinker production datasets with the clinker markets.
        """

        for ds in ws.get_many(
            self.database,
            ws.contains("name", "cement production"),
            ws.contains("reference product", "cement"),
            ws.equals("unit", "kilogram"),
        ):
            for exc in ws.technosphere(ds):
                if exc["name"] == "clinker production" and exc["product"] == "clinker":
                    exc["name"] = "market for clinker"

    def create_clinker_market_datasets(self) -> None:
        """
        Runs a series of methods that create new clinker and cement production datasets
        and new cement market datasets.
        :return: Does not return anything. Modifies in place.
        """

        self.process_and_add_markets(
            name="market for clinker",
            reference_product="clinker",
            unit="kilogram",
            mapping=self.cement_map,
            production_volumes=self.iam_data.production_volumes,
            system_model=self.system_model,
        )

    def create_cement_market_datasets(self):
        # exclude the regionalization of these datasets
        # because they are very rarely used in the database
        excluded = [
            "factory",
            "tile",
            "sulphate",
            "plaster",
            "Portland Slag",
            "CP II-Z",
            "CP IV",
            "CP V RS",
            "Portland SR3",
            "CEM II/A-S",
            "CEM II/A-V",
            "CEM II/B-L",
            "CEM II/B-S",
            "type I (SM)",
            "type I-PM",
            "type IP/P",
            "type IS",
            "type S",
            "CEM III/C",
            "CEM V/A",
            "CEM V/B",
            "CEM II/A-L",
            "CEM III/B",
            "Pozzolana Portland",
            "ART",
            "type IP",
            "CEM IV/A",
            "CEM IV/B",
            "type ICo",
            "carbon",
            "unspecified" "mortar",
        ]

        # cement markets
        markets = list(
            ws.get_many(
                self.database,
                ws.contains("name", "market for cement"),
                ws.contains("reference product", "cement"),
                ws.doesnt_contain_any(
                    "name",
                    excluded,
                ),
                ws.doesnt_contain_any("location", self.regions),
            )
        )
        markets = list(
            set([(m["name"], m["reference product"], m["unit"]) for m in markets])
        )

        for market in markets:

            mapping = {
                "cement": [
                    ds
                    for ds in self.database
                    if ds["unit"] == "kilogram"
                    and ds["reference product"] == market[1]
                    and ds["name"]
                    == market[0].replace("market for cement", "cement production")
                ]
            }

            if len(mapping["cement"]) == 0:
                continue

            self.process_and_add_markets(
                name=market[0],
                reference_product=market[1],
                unit=market[2],
                mapping=mapping,
                system_model=self.system_model,
            )

    def create_cement_production_datasets(self):
        # cement production
        production_datasets = [
            ds
            for ds in self.database
            if "cement production" in ds["name"]
            and "cement" in ds["reference product"]
            and ds.get("regionalized", False) is False
        ]

        cement = {"cement": production_datasets}

        self.process_and_add_activities(
            mapping=cement,
        )

    def create_clinker_technology_datasets(self):

        clinker_dataset = ws.get_one(
            self.database,
            ws.equals("name", "clinker production"),
            ws.equals("reference product", "clinker"),
            ws.equals("unit", "kilogram"),
            ws.equals("location", "Europe without Switzerland"),
        )

        for technolgy in self.cement_map.keys():

            if technolgy == "cement, dry feed rotary kiln":
                continue

            new_dataset = copy.deepcopy(clinker_dataset)
            new_dataset["name"] = technolgy.replace("cement", "clinker production")
            new_dataset["code"] = uuid.uuid4().hex

            for e in ws.production(new_dataset):
                e["name"] = technolgy.replace("cement", "clinker production")
                if "input" in e:
                    del e["input"]

            self.cement_map[technolgy].append(new_dataset)

            self.add_to_index(new_dataset)
            self.write_log(new_dataset, "created")
            self.database.append(new_dataset)

    def create_cement_CCS_datasets(self):

        # add CCS datasets
        ccs_datasets = {
            "on-site CCS": {
                "name": "carbon dioxide, captured, at cement production plant, using direct separation",
                "reference product": "carbon dioxide, captured",
            },
            "oxyfuel CCS": {
                "name": "carbon dioxide, captured, at cement production plant, using oxyfuel",
                "reference product": "carbon dioxide, captured",
            },
            "MEA CCS": {
                "name": "carbon dioxide, captured, at cement production plant, using monoethanolamine",
                "reference product": "carbon dioxide, captured",
            },
        }

        ccs_mapping = {
            k: [
                ws.get_one(
                    self.database,
                    ws.equals("name", v["name"]),
                    ws.equals("reference product", v["reference product"]),
                )
            ]
            for k, v in ccs_datasets.items()
        }

        self.process_and_add_activities(
            mapping=ccs_mapping,
        )

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('initial energy input per ton clinker', '')}|"
            f"{dataset.get('log parameters', {}).get('energy scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('new energy input per ton clinker', '')}|"
            f"{dataset.get('log parameters', {}).get('carbon capture rate', '')}|"
            f"{dataset.get('log parameters', {}).get('initial fossil CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('initial biogenic CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('new fossil CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('new biogenic CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('electricity generated', '')}|"
            f"{dataset.get('log parameters', {}).get('electricity consumed', '')}"
        )
