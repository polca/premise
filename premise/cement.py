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
    It adjusts accounted kiln fuel demand based on the improvement indicated
    in the IAM file, relative to 2020.
    It accounts for secondary fuel energy that is represented in ecoinvent
    emissions but not as burdened technosphere fuel inputs.
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

    @staticmethod
    def _exchange_text(exchange: dict) -> str:
        return f"{exchange.get('name', '')} {exchange.get('product', '')}".lower()

    @staticmethod
    def _format_comment_value(value, unit: str = "") -> str:
        if value in (None, ""):
            return "not available"
        return f"{float(value):.6g}{unit}"

    @staticmethod
    def _append_comment(entity: dict, comment: str) -> None:
        existing_comment = str(entity.get("comment", "") or "").strip()
        if comment in existing_comment:
            return

        entity["comment"] = (
            f"{existing_comment}\n\n{comment}" if existing_comment else comment
        )

    def _get_clinker_fuel_lhv(self, exchange: dict) -> float | None:
        text = self._exchange_text(exchange)
        unit = exchange.get("unit")

        fuel_keys = (
            "hard coal",
            "petroleum coke",
            "heavy fuel oil",
            "light fuel oil",
            "diesel",
            "pulverised lignite",
            "lignite",
            "meat and bone meal",
        )

        for fuel_key in fuel_keys:
            if fuel_key in text and fuel_key in self.fuels_specs:
                return self.fuels_specs[fuel_key]["lhv"]["value"]

        if unit == "cubic meter" and "natural gas" in text:
            return self.fuels_specs["natural gas"]["lhv"]["value"]

        if (
            "wood chips" in text or "biomass" in text
        ) and "biomass" in self.fuels_specs:
            return self.fuels_specs["biomass"]["lhv"]["value"]

        if "waste plastic" in text and "waste" in self.fuels_specs:
            return self.fuels_specs["waste"]["lhv"]["value"]

        return None

    def _get_clinker_fuel_exchange_energy(self, exchange: dict) -> float | None:
        if exchange["type"] != "technosphere":
            return None

        amount = float(exchange.get("amount", 0))
        unit = exchange.get("unit")

        if unit == "megajoule" and amount > 0:
            return amount

        if unit not in ("kilogram", "cubic meter"):
            return None

        lhv = self._get_clinker_fuel_lhv(exchange)
        if lhv is None:
            return None

        return abs(amount) * lhv

    def _get_clinker_visible_fuel_energy(self, dataset: dict) -> float:
        fuel_energy = 0.0

        for exc in dataset["exchanges"]:
            exchange_energy = self._get_clinker_fuel_exchange_energy(exc)
            if exchange_energy is not None:
                fuel_energy += exchange_energy

        return fuel_energy

    def _get_hard_coal_energy_and_exchanges(self, dataset: dict) -> tuple[float, list]:
        coal_lhv = self.fuels_specs["hard coal"]["lhv"]["value"]
        coal_exchanges = [
            exc
            for exc in ws.technosphere(dataset, ws.contains("name", "hard coal"))
            if exc.get("unit") == "kilogram" and float(exc.get("amount", 0)) > 0
        ]
        coal_energy = sum(float(exc["amount"]) * coal_lhv for exc in coal_exchanges)

        return coal_energy, coal_exchanges

    def _document_clinker_fuel_adjustment(
        self,
        dataset: dict,
        technology: str,
        fuel_exchange_state: dict,
        coal_exchange_ids: set[int],
    ) -> None:
        log_parameters = dataset.get("log parameters", {})
        scenario_label = " ".join(
            str(value)
            for value in (
                getattr(self, "model", ""),
                getattr(self, "scenario", ""),
                getattr(self, "year", ""),
            )
            if value not in ("", None)
        )
        if scenario_label:
            scenario_label = f" for {scenario_label}"

        visible_energy = log_parameters.get("visible fuel energy per kg clinker")
        hidden_energy = log_parameters.get(
            "hidden secondary fuel energy per kg clinker"
        )
        accounted_initial_energy = log_parameters.get(
            "accounted initial fuel energy per kg clinker"
        )
        new_accounted_energy = log_parameters.get(
            "new accounted fuel energy per kg clinker",
            accounted_initial_energy,
        )
        target_energy = log_parameters.get("new energy input per ton clinker")
        initial_coal_energy = log_parameters.get(
            "initial hard coal energy per kg clinker"
        )
        new_coal_energy = log_parameters.get("new hard coal energy per kg clinker")
        applied_coal_change = log_parameters.get(
            "applied hard coal energy change per kg clinker"
        )
        unmet_energy_change = log_parameters.get(
            "unmet thermal energy change per kg clinker"
        )

        dataset_comment = (
            f"premise clinker fuel adjustment{scenario_label} for technology "
            f"'{technology}': original visible fuel inputs listed as "
            "technosphere exchanges provide "
            f"{self._format_comment_value(visible_energy, ' MJ/kg clinker')}. "
            "An inferred hidden secondary-fuel contribution of "
            f"{self._format_comment_value(hidden_energy, ' MJ/kg clinker')} "
            "is kept as bookkeeping because part of the secondary-fuel use is "
            "represented by emissions but not by burdened fuel exchanges. "
            "The original accounted kiln fuel demand is "
            f"{self._format_comment_value(accounted_initial_energy, ' MJ/kg clinker')} "
            "(3.4 GJ/t clinker baseline). The new accounted kiln fuel demand "
            "after the applied hard-coal change is "
            f"{self._format_comment_value(new_accounted_energy, ' MJ/kg clinker')}"
        )

        if target_energy not in (None, ""):
            target_energy_gj = self._format_comment_value(
                target_energy / 1000,
                " GJ/t clinker",
            )
            dataset_comment += (
                f". The IAM/floor target was {target_energy_gj}"
            )

        dataset_comment += (
            ". The required energy change is applied to aggregate hard coal "
            "inputs only: hard coal changes from "
            f"{self._format_comment_value(initial_coal_energy, ' MJ/kg clinker')} "
            f"to {self._format_comment_value(new_coal_energy, ' MJ/kg clinker')}, "
            "with an applied hard-coal energy change of "
            f"{self._format_comment_value(applied_coal_change, ' MJ/kg clinker')}."
        )

        if unmet_energy_change not in (None, "") and abs(unmet_energy_change) > 1e-12:
            dataset_comment += (
                " The remaining energy change not absorbed by hard coal is "
                f"{self._format_comment_value(unmet_energy_change, ' MJ/kg clinker')}."
            )

        dataset_comment += (
            " The hidden secondary-fuel energy is not added as a technosphere "
            "exchange."
        )

        self._append_comment(dataset, dataset_comment)

        coal_scaling_factor = log_parameters.get("hard coal energy scaling factor")
        for exc in dataset["exchanges"]:
            state = fuel_exchange_state.get(id(exc))
            if state is None:
                continue

            new_energy = self._get_clinker_fuel_exchange_energy(exc)
            old_amount = state["amount"]
            new_amount = float(exc.get("amount", 0))
            unit = exc.get("unit", state.get("unit", ""))
            amount_unit = f" {unit}/kg clinker"
            old_amount_text = self._format_comment_value(old_amount, amount_unit)
            new_amount_text = self._format_comment_value(new_amount, amount_unit)
            old_energy_text = self._format_comment_value(
                state["energy"],
                " MJ/kg clinker",
            )
            new_energy_text = self._format_comment_value(
                new_energy,
                " MJ/kg clinker",
            )

            if id(exc) in coal_exchange_ids:
                exchange_comment = (
                    "premise clinker fuel adjustment: this hard coal exchange "
                    "absorbs part of the aggregate kiln-fuel energy change. "
                    "Its amount changes from "
                    f"{old_amount_text} to {new_amount_text}, equivalent to "
                    f"{old_energy_text} to {new_energy_text}. "
                    "All hard coal suppliers are scaled proportionally by "
                    f"{self._format_comment_value(coal_scaling_factor)}."
                )
            else:
                exchange_comment = (
                    "premise clinker fuel accounting: this exchange is counted "
                    "as visible kiln fuel energy using its amount and approximate "
                    "lower heating value, or directly as MJ if already expressed "
                    "as energy. It is not changed by the cement efficiency "
                    "adjustment. Its amount remains "
                    f"{new_amount_text}, equivalent to {old_energy_text}."
                )

            self._append_comment(exc, exchange_comment)

        fossil_co2_initial = log_parameters.get("initial fossil CO2")
        fossil_co2_final = log_parameters.get("new fossil CO2")
        biogenic_co2_initial = log_parameters.get("initial biogenic CO2")
        biogenic_co2_final = log_parameters.get(
            "new biogenic CO2",
            biogenic_co2_initial,
        )
        carbon_capture_rate = log_parameters.get("carbon capture rate")

        for exc in ws.biosphere(dataset, ws.contains("name", "Carbon dioxide")):
            if exc["name"] == "Carbon dioxide, fossil":
                final_amount = fossil_co2_final
                if final_amount in (None, ""):
                    final_amount = exc.get("amount")
                initial_text = self._format_comment_value(
                    fossil_co2_initial,
                    " kg/kg clinker",
                )
                final_text = self._format_comment_value(
                    final_amount,
                    " kg/kg clinker",
                )
                exchange_comment = (
                    "premise clinker CO2 accounting: amount before the fuel "
                    "efficiency adjustment was "
                    f"{initial_text}; final amount after fuel efficiency"
                )
                if carbon_capture_rate not in (None, ""):
                    exchange_comment += " and CCS"
                exchange_comment += (
                    f" handling is {final_text}. "
                    "The fuel-efficiency part follows the applied aggregate "
                    "hard-coal energy change; calcination CO2 is not scaled by "
                    "the fuel-efficiency adjustment."
                )
                self._append_comment(exc, exchange_comment)

            if exc["name"] == "Carbon dioxide, non-fossil":
                final_amount = biogenic_co2_final
                if final_amount in (None, ""):
                    final_amount = exc.get("amount")
                initial_text = self._format_comment_value(
                    biogenic_co2_initial,
                    " kg/kg clinker",
                )
                final_text = self._format_comment_value(
                    final_amount,
                    " kg/kg clinker",
                )
                exchange_comment = (
                    "premise clinker CO2 accounting: amount before the fuel "
                    "efficiency adjustment was "
                    f"{initial_text}; final amount after fuel efficiency"
                )
                if carbon_capture_rate not in (None, ""):
                    exchange_comment += " and CCS"
                exchange_comment += (
                    f" handling is {final_text}. "
                    "Non-fossil CO2 from secondary fuels is not changed by the "
                    "non-CCS fuel-efficiency adjustment."
                )
                self._append_comment(exc, exchange_comment)

    def adjust_process_efficiency(self, dataset, technology):
        """
        Adjust accounted clinker thermal energy demand for one regional dataset.

        The source clinker inventories include emissions from several secondary
        fuels that are not listed as burdened technosphere fuel inputs. Therefore,
        the energy ledger combines visible fuel exchanges with an inferred hidden
        secondary-fuel amount so that the starting point remains the 3.4 GJ/t
        clinker reported by Kellenberger et al. (2007).

        IAM efficiency changes are applied to this accounted thermal energy.
        The resulting target is constrained by practical kiln fuel-demand floors:
        3.1 GJ/t clinker for ordinary clinker production and 3.0 GJ/t clinker
        for efficient dry preheater/precalciner kiln technologies. These values
        are practical BAT-style lower bounds, not the theoretical chemical heat
        requirement for clinker formation.

        Only hard coal inputs are changed, because fuel-use reductions are
        assumed to affect coal first. Fossil CO2 is adjusted from the aggregate
        hard-coal energy change; non-fossil CO2 is left unchanged outside the
        separate CCS handling below.
        """

        # From Kellenberger et al. (2007), total clinker thermal energy is
        # 3.4 GJ/t clinker. This is an accounted energy baseline: part of the
        # secondary fuel energy is represented through emissions, but not by
        # explicit burdened technosphere fuel exchanges.
        current_energy_input_per_ton_clinker = 3400
        current_energy_input_per_kg_clinker = (
            current_energy_input_per_ton_clinker / 1000
        )

        # Calculate the efficiency scaling factor relative to 2020.
        scaling_factor = 1 / self.find_iam_efficiency_change(
            data=self.iam_data.cement_technology_efficiencies,
            variable=technology,
            location=dataset["location"],
        )

        new_energy_input_per_ton_clinker = 3400

        log_parameters = dataset.setdefault("log parameters", {})
        log_parameters["initial energy input per ton clinker"] = (
            current_energy_input_per_ton_clinker
        )

        visible_fuel_energy = self._get_clinker_visible_fuel_energy(dataset)
        hidden_secondary_fuel_energy = max(
            current_energy_input_per_kg_clinker - visible_fuel_energy, 0
        )
        accounted_initial_fuel_energy = (
            visible_fuel_energy + hidden_secondary_fuel_energy
        )

        log_parameters["visible fuel energy per kg clinker"] = visible_fuel_energy
        log_parameters["hidden secondary fuel energy per kg clinker"] = (
            hidden_secondary_fuel_energy
        )
        log_parameters["accounted initial fuel energy per kg clinker"] = (
            accounted_initial_fuel_energy
        )
        fuel_exchange_state = {
            id(exc): {
                "amount": float(exc.get("amount", 0)),
                "energy": exchange_energy,
                "unit": exc.get("unit", ""),
            }
            for exc in dataset["exchanges"]
            if (exchange_energy := self._get_clinker_fuel_exchange_energy(exc))
            is not None
        }
        coal_exchange_ids = set()

        if np.isfinite(scaling_factor):
            # Calculate the target accounted thermal energy demand.
            new_energy_input_per_ton_clinker = (
                current_energy_input_per_ton_clinker * scaling_factor
            )
            # Use practical kiln fuel-demand bounds, not the theoretical
            # chemical heat requirement for clinker formation.
            if new_energy_input_per_ton_clinker < 3100:
                new_energy_input_per_ton_clinker = 3100
            elif new_energy_input_per_ton_clinker > 5000:
                new_energy_input_per_ton_clinker = 5000

            # Efficient dry preheater/precalciner kilns can reach the BAT-style
            # lower bound of 3.0 GJ/t clinker.
            if technology.startswith("cement, dry feed rotary kiln, efficient"):
                new_energy_input_per_ton_clinker = 3000

            log_parameters["new energy input per ton clinker"] = int(
                new_energy_input_per_ton_clinker
            )

            scaling_factor = (
                new_energy_input_per_ton_clinker / current_energy_input_per_ton_clinker
            )

            log_parameters["energy scaling factor"] = scaling_factor

            # Rescale hard coal consumption and related fossil CO2 emissions.
            # The aggregate energy change is distributed over all hard-coal
            # suppliers to avoid over-correcting split market inputs.
            coal_specs = self.fuels_specs["hard coal"]
            old_coal_input, coal_exchanges = self._get_hard_coal_energy_and_exchanges(
                dataset
            )
            coal_exchange_ids = {id(exc) for exc in coal_exchanges}
            target_energy_input = new_energy_input_per_ton_clinker / 1000
            required_energy_change = target_energy_input - accounted_initial_fuel_energy
            applied_energy_change = 0.0
            coal_scaling_factor = 1.0

            if old_coal_input > 0:
                new_coal_input = max(old_coal_input + required_energy_change, 0)
                applied_energy_change = new_coal_input - old_coal_input
                coal_scaling_factor = new_coal_input / old_coal_input

                for exc in coal_exchanges:
                    exc["amount"] = float(exc["amount"] * coal_scaling_factor)
            else:
                new_coal_input = 0.0

            log_parameters["initial hard coal energy per kg clinker"] = old_coal_input
            log_parameters["new hard coal energy per kg clinker"] = new_coal_input
            log_parameters["hard coal energy scaling factor"] = coal_scaling_factor
            log_parameters["applied hard coal energy change per kg clinker"] = (
                applied_energy_change
            )
            log_parameters["unmet thermal energy change per kg clinker"] = (
                required_energy_change - applied_energy_change
            )
            log_parameters["new visible fuel energy per kg clinker"] = (
                visible_fuel_energy + applied_energy_change
            )
            log_parameters["new accounted fuel energy per kg clinker"] = (
                accounted_initial_fuel_energy + applied_energy_change
            )

            # rescale combustion-related fossil CO2 emissions
            for exc in ws.biosphere(
                dataset,
                ws.contains("name", "Carbon dioxide"),
            ):
                if exc["name"] == "Carbon dioxide, fossil":
                    log_parameters["initial fossil CO2"] = float(exc["amount"])
                    exc["amount"] += applied_energy_change * coal_specs["co2"]
                    log_parameters["new fossil CO2"] = float(exc["amount"])

                if exc["name"] == "Carbon dioxide, non-fossil":
                    log_parameters["initial biogenic CO2"] = float(exc["amount"])

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

        self._document_clinker_fuel_adjustment(
            dataset=dataset,
            technology=technology,
            fuel_exchange_state=fuel_exchange_state,
            coal_exchange_ids=coal_exchange_ids,
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
