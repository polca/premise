"""
This module contains the class to create final energy use
datasets based on IAM output data.
"""

from typing import List, Set
import uuid
import re
from pathlib import Path
import yaml
import sys

from .data_collection import IAMDataCollection
from .transformation import BaseTransformation, InventorySet

from wurst import searching as ws


def _update_final_energy(
    scenario,
    version,
    system_model,
    split_capacity_operation=False,
):

    if scenario["iam data"].final_energy_use is None:
        print("No final energy scenario data available -- skipping")
        return scenario

    final_energy = FinalEnergy(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        split_capacity_operation=split_capacity_operation,
    )

    final_energy.regionalize_heating_datasets()
    if final_energy.split_capacity_operation:
        # We use the yaml to generate capacity addition datasets
        final_energy.generate_capacity_addition_datasets()
        # and patch the scenario with the yaml data (in memory, not in disk)
        scenario["configurations"] = scenario.get("configurations", {})
        scenario["configurations"][
            "capacity addition"
        ] = final_energy.get_patched_yaml_data()

    final_energy.relink_datasets()
    scenario["database"] = final_energy.database
    scenario["index"] = final_energy.index
    scenario["cache"] = final_energy.cache

    return scenario


class FinalEnergy(BaseTransformation):
    """
    Class that creates heating datasets based on IAM output data.

    It also adds capacity addition datasets based on capacity_addition.yaml

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
        split_capacity_operation: bool = False,
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
        self.split_capacity_operation = split_capacity_operation

        # Track infrastructure datasets to prevent them from being removed from operations
        self.protected_infrastructure_names: Set[str] = set()

        mapping = InventorySet(database=database, version=version, model=model)
        self.final_energy_map = mapping.generate_final_energy_map()
        self.capacity_addition_map = mapping.generate_capacity_addition_map()

    def regionalize_heating_datasets(self):

        self.process_and_add_activities(
            mapping=self.final_energy_map,
        )

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

        for key, config in self.capacity_addition_map.items():
            print(f"🧩 Processing key: '{key}'")

            is_transport = key.startswith("Sales - Transport")

            if is_transport:
                suffix = key.replace("Sales - Transport - ", "").strip()
                new_name = f"transport capacity addition, 1 million units, {suffix}"
                new_ref_prod = f"transport capacity addition, 1 million units, {suffix}"
            else:
                # Generate clean capacity addition name based on key
                suffix = key.replace("New Cap - ", "").strip()
                new_name = f"capacity addition, 1GW, {suffix}"
                new_ref_prod = f"capacity addition, 1GW, {suffix}"

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
            matched_datasets, new_name, new_ref_prod, is_transport
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
        self, matched_datasets_with_filter, new_name, new_ref_prod, is_transport=False
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

    def extract_installed_capacity(self, dataset):
        """
        Try to infer installed capacity and unit from dataset name.
        E.g. 'heat pump, brine-water, 10kW' → 10.0
             'photovoltaic installation, 156 kWp' → 156.0
             'market for turbine, 0.5 MW'   → 500.0

        If not found in name, tries the dataset description/comment.

        Supports units: kW, kWp, MW, MWp, GW, GWp, TW — returns in kW.
        Returns None if not parsable.
        """

        unit_factors = {
            "kw": 1,
            "kwp": 1,  # kWp is same as kW for capacity
            "mw": 1e3,
            "mwp": 1e3,  # MWp is same as MW for capacity
            "gw": 1e6,
            "gwp": 1e6,  # GWp is same as GW for capacity
            "tw": 1e9,
        }

        # Updated pattern to handle optional 'p' suffix
        pattern = r"([\d\.]+)\s*(kWp?|MWp?|GWp?|TWp?)(?!h)"
        m = re.search(pattern, dataset["name"], flags=re.IGNORECASE)

        if m:
            value = float(m.group(1))
            unit = m.group(2).lower()
            factor = unit_factors.get(unit)

            if factor:
                return value * factor  # Return in kW
            else:
                print(f"⚠️ Unrecognized unit: {unit}")
                return None

        # If not found in name, try the reference product
        ref_product = dataset.get("reference product", "")
        if ref_product:
            m = re.search(pattern, ref_product, flags=re.IGNORECASE)
            if m:
                value = float(m.group(1))
                unit = m.group(2).lower()
                factor = unit_factors.get(unit)

                if factor:
                    return value * factor  # Return in kW
                else:
                    print(f"⚠️ Unrecognized unit: {unit}")
                    return None

        # If not found in reference product, try the comment
        comment = dataset.get("comment", "")
        if comment:
            m = re.search(pattern, comment, flags=re.IGNORECASE)
            if m:
                value = float(m.group(1))
                unit = m.group(2).lower()
                factor = unit_factors.get(unit)

                if factor:
                    return value * factor  # Return in kW
                else:
                    print(f"⚠️ Unrecognized unit: {unit}")
                    return None

    def get_patched_yaml_data(self):
        """
        Generate YAML configuration for capacity datasets.

        Creates configuration entries for each capacity dataset that was
        successfully created, mapping them to IAM aliases for integration.

        :return: dictionary with patched YAML configuration
        """

        yaml_file = (
            Path(__file__).parent / "iam_variables_mapping" / "capacity_addition.yaml"
        )

        if not yaml_file.exists():
            print("⚠️ capacity_addition.yaml not found")
            return {}

        with open(yaml_file, "r") as f:
            original_yaml = yaml.safe_load(f)

        patched = {}
        capacity_units = {}

        for variable in self.capacity_addition_map.keys():
            if variable.startswith("New Cap - "):
                suffix = variable.replace("New Cap - ", "").strip()
                capacity_name = f"capacity addition, 1GW, {suffix}"
            elif variable.startswith("Sales - Transport - "):
                suffix = variable.replace("Sales - Transport - ", "").strip()
                capacity_name = (
                    f"transport capacity addition, 1 million units, {suffix}"
                )

            # Look for capacity addition datasets (any region)
            capacity_datasets = list(
                ws.get_many(self.database, ws.equals("name", capacity_name))
            )

            if not capacity_datasets:
                print(f"⚠️ No capacity dataset found for {variable}")
                continue

            # Use first found dataset as template for YAML
            template_ds = capacity_datasets[0]

            # Get IAM aliases from the original YAML
            original_config = original_yaml.get(variable, {})
            original_iam_aliases = original_config.get("iam_aliases", {})

            if self.model not in original_iam_aliases:
                print(f"⚠️ No IAM alias found for {self.model} in {variable} - skipping")
                continue

            iam_variable = original_iam_aliases[self.model]

            patched[variable] = {
                "ecoinvent_aliases": {
                    "fltr": {
                        "name": template_ds["name"],
                        "reference product": template_ds["reference product"],
                    }
                },
                "iam_aliases": {self.model: iam_variable},
            }

            if (
                hasattr(self.iam_data.final_energy_use, "attrs")
                and "unit" in self.iam_data.final_energy_use.attrs
            ):
                if iam_variable in self.iam_data.final_energy_use.attrs["unit"]:
                    capacity_units[iam_variable] = self.iam_data.final_energy_use.attrs[
                        "unit"
                    ][iam_variable]
                    print(
                        f"✅ Found unit for {iam_variable} in final_energy_use: {capacity_units[iam_variable]}"
                    )

            # If not found, check in production_volumes
            if iam_variable not in capacity_units:
                if (
                    hasattr(self.iam_data.production_volumes, "attrs")
                    and "unit" in self.iam_data.production_volumes.attrs
                ):
                    if iam_variable in self.iam_data.production_volumes.attrs["unit"]:
                        capacity_units[iam_variable] = (
                            self.iam_data.production_volumes.attrs["unit"][iam_variable]
                        )
                        print(
                            f"✅ Found unit for {iam_variable} in production_volumes: {capacity_units[iam_variable]}"
                        )

            # If still not found, check in the main data array
            if iam_variable not in capacity_units:
                if (
                    hasattr(self.iam_data.data, "attrs")
                    and "unit" in self.iam_data.data.attrs
                ):
                    if iam_variable in self.iam_data.data.attrs["unit"]:
                        capacity_units[iam_variable] = self.iam_data.data.attrs["unit"][
                            iam_variable
                        ]
                        print(
                            f"✅ Found unit for {iam_variable} in main data: {capacity_units[iam_variable]}"
                        )

            print(f"✅ {variable} → {original_iam_aliases[self.model]}")

        # Store the patched data globally in memory
        if patched:
            # Store in the premise.final_energy module
            final_energy_module = sys.modules["premise.final_energy"]
            final_energy_module._PATCHED_CAPACITY_ADDITION = patched
            final_energy_module._PATCHED_CAPACITY_UNITS = capacity_units
            print(f"📦 Stored {len(patched)} capacity variables globally in memory")
            print(f"📦 Found units for {len(capacity_units)} IAM variables")

        return patched
