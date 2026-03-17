from collections import defaultdict

from ..transformation import ws


class SyntheticFuelsMixin:
    def generate_synthetic_fuel_activities(self):
        """
        Generate region-specific synthetic fuel datasets.
        """
        synfuel_map = {k: v for k, v in self.fuel_map.items() if "synthetic" in k}

        if synfuel_map:
            self.process_and_add_activities(
                mapping=synfuel_map, production_volumes=self.iam_data.production_volumes
            )

        methanol_map = {
            k: v for k, v in self.fuel_map.items() if k.startswith("methanol")
        }

        if methanol_map:
            self.process_and_add_activities(
                mapping=methanol_map,
            )

        # Create other liquid fuels
        self.generate_biofuel_activities()

        # Create markets for liquid fuels
        # update the fuel map to include all liquid fuels
        self.fuel_map = self.mapping.generate_fuel_map(model=self.model)

        # gasoline
        # check that IAM data has "petrol_blend" attribute
        if hasattr(self.iam_data, "petrol_blend"):
            mapping = {
                k: v
                for k, v in self.fuel_map.items()
                if any(
                    k.startswith(x)
                    for x in ("gasoline", "bioethanol", "ethanol", "petrol", "methanol")
                )
                and self.iam_data.petrol_blend.sel(variables=k).sum() > 0
            }
            if mapping:
                for market_name in [
                    "market for petrol",
                    "market for petrol, low-sulfur",
                ]:
                    self.process_and_add_markets(
                        name=market_name,
                        reference_product=market_name.replace("market for ", ""),
                        unit="kilogram",
                        mapping=mapping,
                        system_model=self.system_model,
                        production_volumes=self.iam_data.production_volumes,
                    )

                    self.update_fuel_carbon_dioxide_emissions(
                        variables=[
                            k
                            for k in self.fuel_map.keys()
                            if any(
                                k.startswith(x)
                                for x in (
                                    "gasoline",
                                    "bioethanol",
                                    "ethanol",
                                    "petrol",
                                    "methanol",
                                )
                            )
                        ],
                        market_names=[
                            "market for petrol, low-sulfur",
                            "market for petrol, unleaded",
                        ],
                        co2_intensity=3.15,
                        fossil_variables=[
                            "gasoline",
                            "petrol",
                            "petrol, synthetic, from coal",
                            "petrol, synthetic, from coal, with CCS",
                        ],
                    )

        # diesel
        # check that IAM data has "diesel_blend" attribute
        if hasattr(self.iam_data, "diesel_blend"):
            mapping = {
                k: v
                for k, v in self.fuel_map.items()
                if any(k.startswith(x) for x in ("diesel", "biodiesel"))
                and self.iam_data.diesel_blend.sel(variables=k).sum() > 0
            }
            if mapping:
                for market_name in [
                    "market for diesel",
                    "market for diesel, low-sulfur",
                    "market group for diesel, low-sulfur",
                    "market group for diesel",
                ]:
                    self.process_and_add_markets(
                        name=market_name,
                        reference_product=market_name.replace(
                            "market for ", ""
                        ).replace("market group for ", ""),
                        unit="kilogram",
                        mapping=mapping,
                        system_model=self.system_model,
                        production_volumes=self.iam_data.production_volumes,
                    )

                self.update_fuel_carbon_dioxide_emissions(
                    variables=[
                        k
                        for k in self.fuel_map.keys()
                        if any(k.startswith(x) for x in ("diesel", "biodiesel"))
                    ],
                    market_names=[
                        "market for diesel",
                        "market for diesel, low-sulfur",
                        "market group for diesel, low-sulfur",
                        "market group for diesel",
                    ],
                    co2_intensity=3.15,
                    fossil_variables=[
                        "diesel",
                        "diesel, synthetic, from natural gas",
                        "diesel, synthetic, from natural gas, with CCS",
                        "diesel, synthetic, from coal",
                        "diesel, synthetic, from coal, with CCS",
                    ],
                )

        # jet fuel
        # check that IAM data has "kerosene_blend" attribute
        if hasattr(self.iam_data, "kerosene_blend"):
            mapping = {
                k: v
                for k, v in self.fuel_map.items()
                if k.startswith("kerosene")
                and self.iam_data.kerosene_blend.sel(variables=k).sum() > 0
            }
            if mapping:
                self.process_and_add_markets(
                    name="market for kerosene",
                    reference_product="kerosene",
                    unit="kilogram",
                    mapping=mapping,
                    system_model=self.system_model,
                    production_volumes=self.iam_data.production_volumes,
                )

                self.update_fuel_carbon_dioxide_emissions(
                    variables=[
                        k
                        for k in self.fuel_map.keys()
                        if any(k.startswith(x) for x in ("kerosene",))
                    ],
                    market_names=[
                        "market for kerosene",
                    ],
                    co2_intensity=3.15,
                    fossil_variables=[
                        "kerosene, from petroleum",
                        "kerosene, synthetic, from natural gas, energy allocation",
                        "kerosene, synthetic, from coal, energy allocation",
                        "kerosene, synthetic, from coal, energy allocation, with CCS",
                    ],
                )

        # lpg
        # check that IAM data has "lgp_blend" attribute
        if hasattr(self.iam_data, "lpg_blend"):
            mapping = {
                k: v
                for k, v in self.fuel_map.items()
                if k.startswith("liquefied petroleum gas")
                and self.iam_data.lpg_blend.sel(variables=k).sum() > 0
            }
            if mapping:
                self.process_and_add_markets(
                    name="market for liquefied petroleum gas",
                    reference_product="liquefied petroleum gas",
                    unit="kilogram",
                    mapping=mapping,
                    system_model=self.system_model,
                    production_volumes=self.iam_data.production_volumes,
                )

                self.update_fuel_carbon_dioxide_emissions(
                    variables=[
                        k
                        for k in self.fuel_map.keys()
                        if any(k.startswith(x) for x in ("liquefied petroleum gas",))
                    ],
                    market_names=[
                        "market for liquefied petroleum gas",
                    ],
                    co2_intensity=2.88,
                    fossil_variables=[
                        "liquefied petroleum gas, synthetic, from natural gas, with CCS",
                        "liquefied petroleum gas, synthetic, from natural gas",
                        "liquefied petroleum gas, synthetic, from coal",
                        "liquefied petroleum gas",
                        "liquefied petroleum gas, synthetic, from coal, with CCS",
                    ],
                )

    def update_fuel_carbon_dioxide_emissions(
        self, variables, market_names, co2_intensity, fossil_variables
    ):
        """
        Update carbon dioxide emissions for biogas datasets.
        """
        # Filter only relevant fuels
        filtered_mapping = {k: v for k, v in self.fuel_map.items() if k in variables}

        _, tech_shares, region_weights = (
            self.get_technology_and_regional_production_shares(
                production_volumes=self.iam_data.production_volumes,
                mapping=filtered_mapping,
            )
        )

        # Build nested fuel share dictionary
        fuel_shares = defaultdict(dict)
        for (fuel, region), value in tech_shares.items():
            fuel_shares[region][fuel] = round(value, 2)

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
            fuel: round(value / total_weight, 2) for fuel, value in world_mix.items()
        }

        # Find and process datasets
        datasets = ws.get_many(
            self.database,
            ws.exclude(ws.either(*[ws.equals("name", name) for name in market_names])),
        )

        for ds in datasets:
            # Sum relevant technosphere exchanges and remap locations
            sum_fuel = 0
            for exc in ws.technosphere(
                ds,
                ws.either(*[ws.equals("name", name) for name in market_names]),
                ws.equals("unit", "kilogram"),
            ):

                if ds["location"] in self.regions:
                    new_loc = ds["location"]
                else:
                    new_loc = self.ecoinvent_to_iam_loc.get(ds["location"], "World")

                if self.is_in_index(exc, new_loc):
                    exc["location"] = new_loc
                    sum_fuel += exc["amount"]

            if sum_fuel == 0:
                continue

            fossil_co2 = sum(
                exc["amount"]
                for exc in ws.biosphere(
                    ds,
                    ws.contains("name", "Carbon dioxide, fossil"),
                    ws.equals("unit", "kilogram"),
                )
            )
            if fossil_co2 == 0:
                continue

            loc = (
                ds["location"]
                if ds["location"] in fuel_shares
                else self.ecoinvent_to_iam_loc[ds["location"]]
            )
            share_non_fossil = 1 - sum(
                fuel_shares[loc].get(x, 0.0) for x in fossil_variables
            )

            if share_non_fossil > 0:
                non_fossil_CO2 = sum_fuel * share_non_fossil * co2_intensity

                for e in ws.biosphere(ds, ws.equals("name", "Carbon dioxide, fossil")):
                    e["amount"] = max(0, e["amount"] - non_fossil_CO2)
                    break  # only adjust one exchange

                # Add the non-fossil CO2 exchange
                ds["exchanges"].append(
                    {
                        "uncertainty type": 0,
                        "amount": non_fossil_CO2,
                        "type": "biosphere",
                        "name": "Carbon dioxide, non-fossil",
                        "unit": "kilogram",
                        "categories": ("air",),
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
