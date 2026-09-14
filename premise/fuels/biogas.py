from .carbon import reclassify_fuel_co2
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
                    self.process_and_add_markets(
                        name=market_name,
                        reference_product=market_name.replace(
                            "market for ", ""
                        ).replace("market group for ", ""),
                        unit="cubic meter",
                        mapping=mapping,
                        system_model=self.system_model,
                        production_volumes=self.iam_data.production_volumes,
                        flip_treatment_supplier_sign=True,
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
                ds, ws.either(*[ws.equals("name", name) for name in gas_names])
            ):

                if ds["location"] in self.regions:
                    new_loc = ds["location"]
                else:
                    new_loc = self.ecoinvent_to_iam_loc.get(ds["location"], "World")

                if self.is_in_index(exc, new_loc):
                    exc["location"] = new_loc
                    if exc.get("unit") != "cubic meter" or exc["amount"] <= 0:
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
                    sum_ng += exc["amount"]
                    non_fossil_ng += exc["amount"] * min(1.0, max(0.0, share))

            if sum_ng == 0:
                continue

            reclassify_fuel_co2(
                ds,
                sum_ng * 2.12,
                non_fossil_ng / sum_ng,
                self.biosphere_flows,
                "natural gas",
            )
