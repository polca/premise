from .utils import fetch_mapping
from .config import METHANE_SOURCES


class BiogasMixin:
    def generate_biogas_activities(self):
        """
        Generate region-specific biogas datasets from methane source mappings.
        """
        fuel_activities = fetch_mapping(METHANE_SOURCES)

        methane_map = {
            k: ws.get_one(
                self.database,
                ws.equals("name", v["name"]),
                ws.equals("reference product", v["reference product"]),
            )
            for k, v in fuel_activities.items()
        }

        self.process_and_add_activities(
            mapping=methane_map,
        )

        # create markets for natural gas and biogas

        for market_name in [
            "market for natural gas, high pressure",
            "market group for natural gas, high pressure",
        ]:
            self.process_and_add_markets(
                name=market_name,
                reference_product="natural gas, high pressure",
                unit="cubic meter",
                mapping={
                    k: v
                    for k, v in self.fuel_map.items()
                    if k.startswith("methane")
                },
                system_model=self.system_model,
                production_volumes=self.iam_data.production_volumes
            )
