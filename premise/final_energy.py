"""
This module contains the class to create final energy use
datasets based on IAM output data.
"""

from typing import List

from wurst import searching as ws

from .data_collection import IAMDataCollection
from .transformation import BaseTransformation, InventorySet

import uuid
import re

import contextlib

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
        scenario["configurations"]["capacity addition"] = final_energy.get_patched_yaml_data()

    final_energy.relink_datasets()
    scenario["database"] = final_energy.database
    scenario["index"] = final_energy.index
    scenario["cache"] = final_energy.cache

    return scenario


class FinalEnergy(BaseTransformation):
    """
    Class that creates heating datasets based on IAM output data.

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

        mapping = InventorySet(database=database, version=version, model=model)
        self.final_energy_map = mapping.generate_final_energy_map()
        self.capacity_addition_map = mapping.generate_capacity_addition_map()

    def regionalize_heating_datasets(self):

        new_datasets, processed_datasets = [], []

        for dataset in ws.get_many(
            self.database,
            ws.either(
                *[
                    ws.equals("name", name)
                    for name in list(
                        set(
                            [
                                ", ".join(sorted(s))
                                for s in self.final_energy_map.values()
                            ]
                        )
                    )
                ]
            ),
        ):

            if dataset["name"] in processed_datasets:
                continue
            if dataset["location"] in self.regions:
                continue
            if any(self.is_in_index(dataset, region) for region in self.regions):
                continue

            datasets = self.fetch_proxies(
                name=dataset["name"],
                ref_prod=dataset["reference product"],
            )
            new_datasets.append(datasets)

            processed_datasets.append(dataset["name"])

        for datasets in new_datasets:
            self.database.extend(datasets.values())
            self.add_to_index(datasets.values())

    @contextlib.contextmanager
    def generate_capacity_addition_datasets(self):
        """
        Create standalone capacity datasets and zero out their use from operation datasets.
        """

        print("🔧 Generating capacity addition datasets...\n")

        for key, dataset_names in self.capacity_addition_map.items():
            print(f"🧩 Processing key: '{key}'")

            for name in dataset_names:
                matches = ws.get_many(self.database, ws.equals("name", name))

                if not matches:
                    print(f"  ⚠️ No match found for: {name}")
                    continue

                for orig in matches:
                    print(f"  ✅ Matched dataset: '{orig['name']}' @ {orig['location']}")

                    # Infer capacity
                    installed_kw = self.extract_installed_capacity(orig)
                    if installed_kw is None:
                        print(
                            f"    ⚠️ Could not infer installed capacity for '{orig['name']}' — skipping normalization.")
                        scaling_factor = 1
                    else:
                        scaling_factor = 1_000_000 / installed_kw
                        print(f"    🔄 Scaling factor to 1 GW: {scaling_factor:.0f} (from {installed_kw} kW/unit)")

                    # Remove from operation datasets
                    for op in self.database:
                        if any(exc["type"] == "technosphere" and exc["name"] == name for exc in
                               op.get("exchanges", [])):
                            before = len(op["exchanges"])
                            op["exchanges"] = [
                                exc for exc in op["exchanges"]
                                if not (exc["type"] == "technosphere" and exc["name"] == name)
                            ]
                            after = len(op["exchanges"])
                            print(f"    🧹 Removed {before - after} exchanges from '{op['name']}'")

                    # Build new dataset manually
                    new_name = self.rename_to_1GW(orig["name"]) + ", capacity addition"
                    new_ref_prod = orig["reference product"] + ", capacity addition"
                    new_ds = {
                        "name": new_name,
                        "reference product": new_ref_prod,
                        "location": orig["location"],
                        "unit": orig["unit"],
                        "comment": "Created by premise. Infrastructure-only dataset for capacity addition (normalized to 1 GW).",
                        "code": str(uuid.uuid4()),
                        "exchanges": [],
                        "type": "process",
                    }

                    # Add scaled exchanges
                    for exc in orig.get("exchanges", []):
                        if exc["type"] in ("technosphere", "biosphere") and "amount" in exc:
                            new_exc = exc.copy()
                            new_exc["amount"] *= scaling_factor
                            new_ds["exchanges"].append(new_exc)

                    # Add production exchange
                    new_ds["exchanges"].append({
                        "name": new_name,
                        "product": new_ref_prod,
                        "amount": 1,
                        "unit": orig["unit"],
                        "location": orig["location"],
                        "type": "production",
                    })

                    self.database.append(new_ds)
                    self.add_to_index([new_ds])
                    print(f"    ✅ Added new capacity dataset: {new_ds['name']}")

    def extract_installed_capacity(self, dataset):
        """
        Try to infer installed capacity and unit from dataset name.
        E.g. 'heat pump, brine-water, 10kW' → 10.0
             'market for turbine, 0.5 MW'   → 500.0

        Supports units: kW, MW, GW, TW — returns in kW.
        Returns None if not parsable.
        """

        unit_factors = {
            "kw": 1,
            "mw": 1e3,
            "gw": 1e6,
            "tw": 1e9,
        }

        pattern = r"([\d\.]+)\s*(kW|MW|GW|TW)"
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
        else:
            return None

    def rename_to_1GW(self, name: str) -> str:
        """
        Replace any installed capacity info in the format '123kW', '5 MW', '0.5TW' with '1GW'.
        """
        return re.sub(r"\b\d+(\.\d+)?\s?(W|kW|MW|GW|TW)\b", "1GW", name, flags=re.IGNORECASE)

    def get_patched_yaml_data(self):
        patched = {}

        for variable, dataset_names in self.capacity_addition_map.items():
            for name in dataset_names:
                for ds in ws.get_many(self.database, ws.contains("name", "capacity addition")):
                    if name in ds["name"]:
                        patched[variable] = {
                            "ecoinvent_aliases": {
                                "fltr": {
                                    "name": ds["name"],
                                    "reference product": ds["reference product"],
                                }
                            },
                            "iam_aliases": {
                                self.model: self.iam_data.final_energy_use.get_key(variable)
                            },
                        }
        return patched




