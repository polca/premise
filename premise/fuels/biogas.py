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
        if hasattr(self.iam_data, "natural_gas_blend"):
            mapping = {
                k: v
                for k, v in self.fuel_map.items()
                if any(k.startswith(x) for x in ["natural gas", "methane"])
                and self.iam_data.natural_gas_blend.sel(variables=k).sum() > 0
            }
            if mapping:
                for market_name in [
                    "market for natural gas, high pressure",
                    "market group for natural gas, high pressure",
                    "market for natural gas, low pressure",
                ]:
                    self.process_and_add_markets(
                        name=market_name,
                        reference_product=market_name.replace(
                            "market for ", ""
                        ).replace("market group for ", ""),
                        unit="cubic meter",
                        mapping=mapping,
                        system_model=self.system_model,
                        production_volumes=self.iam_data.production_volumes,
                        blacklist={
                            "consequential": [
                                "methane, from biomass",
                            ]
                        },
                        conversion_factor={
                            "methane, from biomass": 0.716,
                            "methane, synthetic": 0.716,
                            "methane, from coal": 0.716,
                        },
                    )

                self.update_carbon_dioxide_emissions()

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

        # Relevant natural gas market names
        gas_names = {
            "market for natural gas, high pressure",
            "market group for natural gas, high pressure",
            "market for natural gas, low pressure",
        }

        # Find and process datasets
        datasets = ws.get_many(
            self.database,
            ws.exclude(ws.either(*[ws.equals("name", name) for name in gas_names])),
        )

        for ds in datasets:
            # Sum relevant technosphere exchanges and remap locations
            sum_ng = 0
            for exc in ws.technosphere(
                ds, ws.either(*[ws.equals("name", name) for name in gas_names])
            ):

                if ds["location"] in self.regions:
                    new_loc = ds["location"]
                else:
                    new_loc = self.ecoinvent_to_iam_loc.get(ds["location"], "World")

                if self.is_in_index(exc, new_loc):
                    exc["location"] = new_loc
                    sum_ng += exc["amount"]

            if sum_ng == 0:
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
            share_non_fossil = 1 - fuel_shares[loc].get("natural gas", 1.0)

            if share_non_fossil > 0:
                non_fossil_CO2 = (
                    sum_ng * share_non_fossil * 2.12
                )  # 2.12 kg CO2 per m3 of natural gas

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
