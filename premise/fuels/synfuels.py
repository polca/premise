from .utils import fetch_mapping
from .config import LIQUID_FUEL_SOURCES
from ..transformation import ws


class SyntheticFuelsMixin:
    def generate_synthetic_fuel_activities(self):
        """
        Generate region-specific synthetic fuel datasets.
        """
        synfuel_map = {k: v for k, v in self.fuel_map.items() if "synthetic" in k}

        self.process_and_add_activities(
            mapping=synfuel_map,
        )

        # Create other liquid fuels
        self.generate_biofuel_activities()

        # Create markets for liquid fuels

        # gasoline
        self.process_and_add_markets(
            name="market for petrol, low-sulfur",
            reference_product="gasoline",
            unit="kilogram",
            mapping={
                k: v
                for k, v in self.fuel_map.items()
                if any(
                    k.startswith(x)
                    for x in ("gasoline", "bioethanol", "ethanol", "petrol", "methanol")
                )
            },
            system_model=self.system_model,
            production_volumes=self.iam_data.production_volumes,
        )

        # diesel
        for market_name in [
            "market for diesel",
            "market for diesel, low-sulfur",
            "market group for diesel, low-sulfur",
        ]:
            self.process_and_add_markets(
                name=market_name,
                reference_product="diesel, low-sulfur",
                unit="kilogram",
                mapping={
                    k: v
                    for k, v in self.fuel_map.items()
                    if any(k.startswith(x) for x in ("diesel", "biodiesel"))
                },
                system_model=self.system_model,
                production_volumes=self.iam_data.production_volumes,
            )

        # jet fuel
        self.process_and_add_markets(
            name="market for kerosene",
            reference_product="kerosene",
            unit="kilogram",
            mapping={
                k: v for k, v in self.fuel_map.items() if k.startswith("kerosene")
            },
            system_model=self.system_model,
            production_volumes=self.iam_data.production_volumes,
        )

        # lpg
        self.process_and_add_markets(
            name="market for liquefied petroleum gas",
            reference_product="liquefied petroleum gas",
            unit="kilogram",
            mapping={
                k: v
                for k, v in self.fuel_map.items()
                if k.startswith("liquefied petroleum gas")
            },
            system_model=self.system_model,
            production_volumes=self.iam_data.production_volumes,
        )
