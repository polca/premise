from .utils import fetch_mapping
from .config import LIQUID_FUEL_SOURCES
from ..transformation import ws


class SyntheticFuelsMixin:
    def generate_synthetic_fuel_activities(self):
        """
        Generate region-specific synthetic fuel datasets.
        """
        fuel_activities = fetch_mapping(LIQUID_FUEL_SOURCES)
        processed_datasets = []

        for fuel_type, activities in fuel_activities.items():
            datasets = [
                ds
                for ds in self.database
                if any(ds["name"].startswith(activity) for activity in activities)
            ]
            for dataset in datasets:
                if dataset["name"] in processed_datasets:
                    continue
                processed_datasets.append(dataset["name"])

                new_datasets = self.fetch_proxies(datasets=[dataset], relink=False)

                for region, new_dataset in new_datasets.items():
                    for exc in ws.production(new_dataset):
                        exc.pop("input", None)

                    for exc in ws.technosphere(new_dataset):
                        if "carbon dioxide, captured from atmosphere" in exc["name"]:
                            co2_amount = exc["amount"]
                            try:
                                dac_suppliers = self.find_suppliers(
                                    name=(
                                        "carbon dioxide, captured from atmosphere, "
                                        "with a solvent-based direct air capture system, 1MtCO2, "
                                        "with heat pump heat, and grid electricity"
                                    ),
                                    ref_prod="carbon dioxide, captured from atmosphere",
                                    unit="kilogram",
                                    loc=region,
                                )
                            except IndexError:
                                dac_suppliers = None

                            if dac_suppliers:
                                new_dataset["exchanges"].remove(exc)
                                new_dataset["exchanges"].extend(
                                    {
                                        "uncertainty type": 0,
                                        "amount": co2_amount * share,
                                        "type": "technosphere",
                                        "product": supplier[2],
                                        "name": supplier[0],
                                        "unit": supplier[-1],
                                        "location": supplier[1],
                                    }
                                    for supplier, share in dac_suppliers.items()
                                )

                    new_dataset = self.relink_technosphere_exchanges(new_dataset)
                    self.database.append(new_dataset)
                    self.write_log(dataset)
                    self.add_to_index(dataset)
