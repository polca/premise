from .utils import fetch_mapping
from .config import METHANE_SOURCES


class BiogasMixin:
    def generate_biogas_activities(self):
        """
        Generate region-specific biogas datasets from methane source mappings.
        """
        fuel_activities = fetch_mapping(METHANE_SOURCES)

        for fuel, dataset_names in fuel_activities.items():
            for details in dataset_names:
                datasets = [
                    ds
                    for ds in self.database
                    if ds["name"] == details["name"]
                    and ds["reference product"] == details["reference product"]
                ]

                new_ds = self.fetch_proxies(datasets=datasets)

                for new_dataset in new_ds.values():
                    self.database.append(new_dataset)
                    self.write_log(new_dataset)
                    self.add_to_index(new_dataset)
