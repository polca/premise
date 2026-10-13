from .carbon import is_vehicle_fuel_consumer, reclassify_fuel_co2
from .properties import CNG_ENERGY_MJ_PER_KG
from .utils import fetch_mapping
from .config import METHANE_SOURCES
from ..transformation import ws

from collections import defaultdict


class BiogasMixin:
    def generate_biogas_activities(self):
        """
        Generate region-specific biogas datasets from methane source mappings.
        """
        fuel_activities = fetch_mapping(METHANE_SOURCES)

        methane_map = {
            k: list(
                ws.get_many(
                    self.database,
                    ws.either(*[ws.equals("name", x["name"]) for x in v]),
                    ws.either(
                        *[
                            ws.equals("reference product", x["reference product"])
                            for x in v
                        ]
                    ),
                )
            )
            for k, v in fuel_activities.items()
            if k in self.fuel_map
        }

        if not methane_map:
            return

        self.process_and_add_activities(
            mapping=methane_map, production_volumes=self.iam_data.production_volumes
        )

        self.fuel_map = self.mapping.generate_fuel_map()

        # create markets for natural gas and biogas
        # check that IAM data has "natural_gas_blend" attribute
        natural_gas_blend = getattr(self.iam_data, "natural_gas_blend", None)
        if natural_gas_blend is not None:
            blend_variables = set(natural_gas_blend.coords["variables"].values)
            mapping = {
                k: v
                for k, v in self.fuel_map.items()
                if any(k.startswith(x) for x in ["natural gas", "methane"])
                and k in blend_variables
                and natural_gas_blend.sel(variables=k).sum() > 0
            }
            if mapping:
                for market_name in [
                    "market for natural gas, high pressure",
                    "market group for natural gas, high pressure",
                    "market for natural gas, low pressure",
                ]:
                    self.process_and_add_fuel_markets(
                        reference_fuel="natural gas",
                        name=market_name,
                        reference_product=market_name.replace(
                            "market for ", ""
                        ).replace("market group for ", ""),
                        unit="cubic meter",
                        mapping=mapping,
                        system_model=self.system_model,
                        production_volumes=self.iam_data.production_volumes,
                        technology_shares=natural_gas_blend,
                        flip_treatment_supplier_sign=True,
                        blacklist={
                            "consequential": [
                                "methane, from biomass",
                            ]
                        },
                    )

                self.regionalize_vehicle_grade_gas()

                self.update_carbon_dioxide_emissions()

    def regionalize_vehicle_grade_gas(self):
        """Link CNG markets through regional compression to regional energy.

        Compression owns the source inventory's m3/kg conversion, electricity
        demand, infrastructure and losses. Regionalize that provider before
        its kg-based market, without changing these physical coefficients.
        """
        product = "natural gas, medium pressure, vehicle grade"
        for name in (
            "natural gas production, medium pressure, vehicle grade",
            "market for natural gas, medium pressure, vehicle grade",
        ):
            activities = list(
                ws.get_many(
                    self.database,
                    ws.equals("name", name),
                    ws.equals("reference product", product),
                    ws.equals("unit", "kilogram"),
                )
            )
            if not activities:
                continue

            self.process_and_add_activities(
                mapping={"vehicle-grade natural gas": activities}
            )

            # Original geographic activities only forward the same product.
            # Use one kg from their IAM replacement, including World. The
            # generic helper otherwise spreads World forwarding over non-World
            # regions with weights that also include World in the denominator.
            for dataset in activities:
                if not dataset.get("emptied", False):
                    continue
                location = self.ecoinvent_to_iam_loc.get(dataset["location"], "World")
                if not self.is_in_index(dataset, location):
                    location = "World"
                if self.is_in_index(dataset, location):
                    dataset["exchanges"] = list(ws.production(dataset)) + [
                        {
                            "name": name,
                            "product": product,
                            "location": location,
                            "unit": "kilogram",
                            "type": "technosphere",
                            "amount": 1.0,
                            "uncertainty type": 0,
                        }
                    ]

    def update_carbon_dioxide_emissions(self):
        """
        Update carbon dioxide emissions for biogas datasets.
        """
        # Filter only relevant fuels
        filtered_mapping = {
            k: v
            for k, v in self.fuel_map.items()
            if k.startswith(("natural gas", "methane"))
        }

        _, tech_shares, region_weights = (
            self.get_technology_and_regional_production_shares(
                production_volumes=self.iam_data.production_volumes,
                mapping=filtered_mapping,
            )
        )

        # Build nested fuel share dictionary
        fuel_shares = defaultdict(dict)
        for (fuel, region), value in tech_shares.items():
            fuel_shares[region][fuel] = float(value)

        fuel_shares = {k: v for k, v in fuel_shares.items() if sum(v.values()) > 0}

        # Compute global weighted mix
        world_mix = defaultdict(float)
        total_weight = 0.0
        for region, fuels in fuel_shares.items():
            weight = region_weights.get(region)
            if weight:
                total_weight += weight
                for fuel, share in fuels.items():
                    world_mix[fuel] += share * weight

        # Normalize global mix
        fuel_shares["World"] = {
            fuel: value / total_weight
            for fuel, value in world_mix.items()
            if total_weight > 0
        }

        # Relevant natural gas market names
        gas_names = {
            "market for natural gas, high pressure",
            "market group for natural gas, high pressure",
            "market for natural gas, low pressure",
        }
        vehicle_gas_name = "market for natural gas, medium pressure, vehicle grade"
        consumer_gas_names = gas_names | {vehicle_gas_name}

        # Find and process datasets
        datasets = ws.get_many(
            self.database,
            ws.exclude(ws.either(*[ws.equals("name", name) for name in gas_names])),
        )

        for ds in datasets:
            # Sum relevant technosphere exchanges and remap locations
            sum_ng = 0
            non_fossil_ng = 0
            for exc in ws.technosphere(
                ds, ws.either(*[ws.equals("name", name) for name in consumer_gas_names])
            ):

                if ds["location"] in self.regions:
                    new_loc = ds["location"]
                else:
                    new_loc = self.ecoinvent_to_iam_loc.get(ds["location"], "World")

                if self.is_in_index(exc, new_loc):
                    exc["location"] = new_loc
                    if exc["amount"] <= 0:
                        continue
                    if exc.get("unit") == "cubic meter":
                        co2 = exc["amount"] * 2.12
                    elif (
                        exc["name"] == vehicle_gas_name
                        and exc.get("unit") == "kilogram"
                    ):
                        # Same reference energy and carbon factor as car_energy.
                        co2 = exc["amount"] * CNG_ENERGY_MJ_PER_KG * 0.06
                    else:
                        continue
                    mix_loc = exc["location"]
                    mix_loc = (
                        mix_loc
                        if mix_loc in fuel_shares
                        else self.ecoinvent_to_iam_loc.get(mix_loc, "World")
                    )
                    mix = fuel_shares.get(mix_loc, {})
                    if not mix:
                        continue
                    share = 1 - sum(
                        mix.get(k, 0.0) for k in ("natural gas", "methane, from coal")
                    ) / sum(mix.values())
                    sum_ng += co2
                    non_fossil_ng += co2 * min(1.0, max(0.0, share))

            if sum_ng == 0 or is_vehicle_fuel_consumer(ds):
                continue

            reclassify_fuel_co2(
                ds,
                sum_ng,
                non_fossil_ng / sum_ng,
                self.biosphere_flows,
                "natural gas",
            )
