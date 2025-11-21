"""
This module contains the class to create capacity datasets.
"""

from typing import List, Set
import uuid

from .data_collection import IAMDataCollection
from .transformation import BaseTransformation, InventorySet
from .activity_maps import get_capacity_addition_dataset_names


def _update_capacity(
    scenario,
    version,
    system_model,
):

    capacity = Capacity(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
    )

    # We use the yaml to generate capacity addition datasets
    capacity.generate_capacity_addition_datasets()
    scenario["database"] = capacity.database
    scenario["index"] = capacity.index
    scenario["cache"] = capacity.cache

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["capacity"] = capacity.map

    return scenario


class Capacity(BaseTransformation):
    """
    Class that creates capacity datasets.

    :ivar database: database dictionary from :attr:`.NewDatabase.database`
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :ivar year: year, from :attr:`.NewDatabase.year`

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
    ) -> None:
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
            cache,
        )
        self.version = version

        # Track infrastructure datasets to prevent them
        # from being removed from operations
        self.protected_infrastructure_names: Set[str] = set()

        mapping = InventorySet(database=database, version=version, model=model)
        self.map = mapping.generate_capacity_addition_filters()

    def generate_capacity_addition_datasets(self):
        """
        Generate capacity addition datasets using simplified approach.

        For each capacity addition key:
        1. Find all datasets matching the filter criteria
        2. Remove them from operational datasets (zero them out) - UNLESS they are 'protected'
        3. Create one unified capacity dataset combining all found datasets

        Supports multiple filter blocks for combining different technologies
        (e.g., wind fixed + moving parts) and AND logic within filter lists
        (e.g., solar photovoltaic AND installation).
        """

        print("🔧 Generating capacity addition datasets...\n")

        for key, config in self.map.items():
            print(f"🧩 Processing key: '{key}'")

            # Use the shared utility function
            new_name, new_ref_prod, unit = get_capacity_addition_dataset_names(key)
            is_transport = key.startswith("Sales - Transport")

            self.process_capacity_addition(
                key, config, new_name, new_ref_prod, is_transport
            )

    def process_capacity_addition(
        self, key, config, new_name, new_ref_prod, is_transport=False
    ):
        """
        Process a single capacity addition configuration.

        Creates one exchange per filter block.

        :param key: capacity addition key (e.g., "New Cap - Electricity - Solar")
        :param config: configuration dictionary from YAML
        :param new_name: name for the new capacity dataset
        :param new_ref_prod: reference product for the new capacity dataset
        :param is_transport: whether this is a transport sales dataset
        """

        if "fltr" not in config:
            print(f"  ⚠️ No fltr specified in config")
            return

        # Find all datasets matching the filter criteria
        matched_datasets = self.get_datasets_from_filter(config)

        if not matched_datasets:
            print(f"  ⚠️ No datasets found with filter: {config}")
            return

        print(f"  ✅ Found {len(matched_datasets)} datasets to process")

        # Remove all matched datasets from operational datasets (zero them out)
        self.remove_infrastructure_from_operations(matched_datasets)

        # Create capacity dataset using all matched datasets
        self.create_unified_capacity_dataset(
            matched_datasets, new_name, new_ref_prod, key, is_transport
        )

        print(f"  ✅ Completed processing for {key}")

    def remove_infrastructure_from_operations(self, infrastructure_datasets):
        """
        Remove infrastructure datasets from all operational datasets.

        This "zeros out" the infrastructure datasets by removing them from
        all operational activities that might consume them.

        :param infrastructure_datasets: list of infrastructure datasets to remove
                                      Can be either datasets or (dataset, filter_index) tuples
        """

        # Handle both formats: datasets or (dataset, filter_index) tuples
        if infrastructure_datasets and isinstance(infrastructure_datasets[0], tuple):
            datasets = [ds for ds, _ in infrastructure_datasets]
        else:
            datasets = infrastructure_datasets

        infrastructure_names = [
            ds["name"]
            for ds in datasets
            if ds["name"] not in self.protected_infrastructure_names
        ]
        total_removed = 0

        for op_dataset in self.database:
            # Skip capacity addition datasets themselves
            if "capacity addition," in op_dataset["name"]:
                continue
            original_count = len(op_dataset.get("exchanges", []))
            op_dataset["exchanges"] = [
                exc
                for exc in op_dataset.get("exchanges", [])
                if not (
                    exc["type"] == "technosphere"
                    and exc["name"] in infrastructure_names
                )
            ]
            removed_count = original_count - len(op_dataset["exchanges"])
            total_removed += removed_count

        print(f"    📊 Total infrastructure exchanges removed: {total_removed}")

    def create_unified_capacity_dataset(
        self,
        matched_datasets_with_filter,
        new_name,
        new_ref_prod,
        key,
        is_transport=False,
    ):
        """
        Create a unified capacity dataset from matched infrastructure datasets.

        - Cancels ALL matched datasets from operations
        - For single filter: selects ONE representative dataset (first one)
        - For multiple filters: selects ONE representative per filter block
        - Scales to 1GW capacity

        :param matched_datasets_with_filter: list of (dataset, filter_index) tuples
        :param new_name: name for the capacity dataset
        :param new_ref_prod: reference product for the capacity dataset
        :param is_transport: whether this is a transport sales dataset
        """

        if not matched_datasets_with_filter:
            return

        # Extract just the datasets for cancellation
        matched_datasets = [ds for ds, _ in matched_datasets_with_filter]

        print(f"    📊 Processing {len(matched_datasets)} matched datasets")
        print(f"    🧹 Cancelled {len(matched_datasets)} datasets from operations")

        # Group datasets by filter index
        filter_groups = {}
        for ds, filter_idx in matched_datasets_with_filter:
            if filter_idx not in filter_groups:
                filter_groups[filter_idx] = []
            filter_groups[filter_idx].append(ds)

        # Select one representative from each filter group
        infrastructure_exchanges = []

        for filter_idx, datasets_in_group in filter_groups.items():
            # Select first dataset from this filter group as representative
            representative_ds = datasets_in_group[0]

            print(
                f"    📊 Selected representative from filter {filter_idx}: {representative_ds['name']}"
            )

            self.protected_infrastructure_names.add(representative_ds["name"])

            if is_transport:
                individual_scaling = 1_000_000  # Scale transport to 1 million units
                print(
                    f"      📊 {representative_ds['name']}: 1 unit → {individual_scaling:,} units"
                )
            else:
                # Extract capacity and calculate scaling factor
                individual_kw = self.extract_installed_capacity(representative_ds)
                if individual_kw is None:
                    individual_scaling = 1
                    print(
                        f"      ⚠️ Could not extract capacity for {representative_ds['name']}, using scaling factor 1"
                    )
                else:
                    # Scale to 1GW (1,000,000 kW)
                    individual_scaling = 1_000_000 / individual_kw
                    print(
                        f"      📊 {representative_ds['name']}: {individual_kw / 1000:.0f} MW → factor {individual_scaling:.2f}"
                    )

            # Create exchange for this representative dataset
            infrastructure_exchanges.append(
                {
                    "name": representative_ds["name"],
                    "product": representative_ds["reference product"],
                    "amount": individual_scaling,
                    "unit": representative_ds["unit"],
                    "type": "technosphere",
                    "location": representative_ds["location"],
                }
            )

        # Use first representative for location/unit (they should be similar anyway)
        first_representative = list(filter_groups.values())[0][0]

        # Create capacity dataset
        capacity_dataset = self.create_capacity_dataset(
            new_name,
            new_ref_prod,
            first_representative["location"],
            first_representative["unit"],
            infrastructure_exchanges,
            f"Representative dataset(s) selected from {len(matched_datasets)} cancelled datasets",
        )

        self.database.append(capacity_dataset)
        self.add_to_index([capacity_dataset])

        print(
            f"    ✅ Created capacity dataset with {len(infrastructure_exchanges)} infrastructure input(s)"
        )

        for i, exchange in enumerate(infrastructure_exchanges):
            print(
                f"      Input {i + 1}: {exchange['name']} (amount: {exchange['amount']:.2f})"
            )

        # Create regional variants
        regionalized = self.fetch_proxies(datasets=[capacity_dataset])
        for reg_ds in regionalized.values():
            if not any(
                ds["name"] == reg_ds["name"] and ds["location"] == reg_ds["location"]
                for ds in self.database
            ):
                self.database.append(reg_ds)
                self.add_to_index([reg_ds])

        self.map[key] = regionalized.values()

        print(f"    🌍 Added {len(regionalized)} regional variants")

    def get_datasets_from_filter(self, ecoinvent_aliases):
        """
        Get datasets using filtering with support for multiple filter blocks.

        Supports the new YAML structure where fltr can be:
        - A single filter object
        - A list of filter objects (results are combined)

        Within each filter object:
        - Lists use AND logic (all terms must appear)
        - Multiple filter objects use OR logic (any can match)

        :param ecoinvent_aliases: dictionary containing filter specifications
        :return: list of (dataset, filter_index) tuples to track which filter each dataset came from
        """

        fltr = ecoinvent_aliases.get("fltr", {})

        if not fltr:
            return []

        # Handle new structure where fltr can be a list of filter objects
        if isinstance(fltr, list):
            # Multiple filter blocks - track which block each dataset came from
            all_datasets = []
            for i, filter_block in enumerate(fltr):
                datasets = self.apply_single_filter(filter_block)
                for ds in datasets:
                    all_datasets.append((ds, i))
            return all_datasets
        else:
            # Single filter block - all datasets come from filter 0
            datasets = self.apply_single_filter(fltr)
            return [(ds, 0) for ds in datasets]

    def apply_single_filter(self, filter_block):
        """
        Apply a single filter block to find matching datasets.

        Supports filtering by:
        - name: substring matching with AND logic for lists
        - reference product: substring matching with AND logic for lists
        - unit: exact matching
        - mask: exclusion filtering

        :param filter_block: single filter specification dictionary
        :return: list of matching datasets
        """

        # Start with all datasets
        datasets = list(self.database)

        # Apply name filter
        if "name" in filter_block:
            names = filter_block["name"]
            if isinstance(names, list):
                # AND logic: all names must appear in the dataset name
                for name in names:
                    datasets = [ds for ds in datasets if name in ds["name"]]
            else:
                # Single name: must appear in dataset name
                datasets = [ds for ds in datasets if names in ds["name"]]

        # Apply reference product filter
        if "reference product" in filter_block:
            ref_prods = filter_block["reference product"]
            if isinstance(ref_prods, list):
                # AND logic: all reference products must appear in the dataset reference product
                for ref_prod in ref_prods:
                    datasets = [
                        ds for ds in datasets if ref_prod in ds["reference product"]
                    ]
            else:
                # Single reference product: must appear in dataset reference product
                datasets = [
                    ds for ds in datasets if ref_prods in ds["reference product"]
                ]

        # Apply unit filter (exact match)
        if "unit" in filter_block:
            unit_filter = filter_block["unit"]
            datasets = [ds for ds in datasets if ds.get("unit") == unit_filter]

        # Apply mask (exclusions)
        if "mask" in filter_block:
            mask = filter_block["mask"]
            if isinstance(mask, str):
                datasets = [ds for ds in datasets if mask not in ds["name"]]
            elif isinstance(mask, list):
                datasets = [
                    ds for ds in datasets if not any(m in ds["name"] for m in mask)
                ]

        return datasets

    def create_capacity_dataset(
        self, name, ref_prod, location, unit, exchanges, comment_suffix
    ):
        """
        Create a capacity dataset with standard structure.

        :param name: dataset name
        :param ref_prod: reference product
        :param location: dataset location
        :param unit: dataset unit
        :param exchanges: list of input exchanges
        :param comment_suffix: additional comment text
        :return: capacity dataset dictionary
        """

        capacity_dataset = {
            "name": name,
            "reference product": ref_prod,
            "location": location,
            "unit": unit,
            "comment": f"Created by premise. Infrastructure-only dataset for capacity addition (normalized to 1 GW). {comment_suffix}",
            "code": str(uuid.uuid4()),
            "exchanges": exchanges.copy(),
            "type": "process",
        }

        # Add production exchange
        capacity_dataset["exchanges"].append(
            {
                "name": name,
                "product": ref_prod,
                "amount": 1,
                "unit": unit,
                "location": location,
                "type": "production",
            }
        )

        return capacity_dataset
