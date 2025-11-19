"""
This module contains the PathwaysDataPackage class, which is
used to create a data package for scenario analysis.
"""

import json
import csv
import shutil
from datetime import date
from pathlib import Path
from typing import List
from .transformation import ws
import xarray as xr
import yaml
import sys
from datapackage import Package

from . import __version__
from .new_database import NewDatabase
from .inventory_imports import get_classifications
from .utils import load_database


class PathwaysDataPackage:
    def __init__(
        self,
        scenarios: List[dict],
        years: List[int] = range(2005, 2105, 5),
        source_version: str = "3.10",
        source_type: str = "brightway",
        key: bytes = None,
        source_db: str = None,
        source_file_path: str = None,
        additional_inventories: List[dict] = None,
        system_model: str = "cutoff",
        system_args: dict = None,
        gains_scenario="CLE",
        use_absolute_efficiency=False,
        biosphere_name="biosphere3",
        split_capacity_operation: bool = False,
        split_external_capacity_operation: bool = False,
        generate_reports: bool = True,
    ):
        self.years = years
        self.scenarios = []
        for year in years:
            for scenario in scenarios:
                new_entry = scenario.copy()
                new_entry["year"] = year
                self.scenarios.append(new_entry)

        self.source_db = source_db
        self.source_version = source_version
        self.key = key

        self.datapackage = NewDatabase(
            scenarios=self.scenarios,
            source_version=source_version,
            source_type=source_type,
            key=key,
            source_db=source_db,
            source_file_path=source_file_path,
            additional_inventories=additional_inventories,
            system_model=system_model,
            system_args=system_args,
            gains_scenario=gains_scenario,
            use_absolute_efficiency=use_absolute_efficiency,
            biosphere_name=biosphere_name,
            generate_reports=generate_reports,
            split_capacity_operation=split_capacity_operation,
            split_external_capacity_operation=split_external_capacity_operation,
        )

        self.scenario_names = []
        self.classifications = get_classifications()

    def create_datapackage(
        self,
        name: str = f"pathways_{date.today()}",
        contributors: list = None,
        transformations: list = None,
        strip_cdr_energy: bool = False,
    ):
        """
        Create and export a scenario datapackage.

        :param name: Name of the datapackage.
        :param contributors: List of contributors to the datapackage.
        :param transformations: List of transformations to apply to the datapackage.
        :param strip_cdr_energy: If True, remove energy inputs (electricity, heat) from CDR
        datasets to avoid double-counting when using "startswith('SE - cdr')" variables in pathways.
        Default is False.
        """
        if transformations:
            self.datapackage.update(transformations)
        else:
            self.datapackage.update()

        self._export_datapackage(
            name=name,
            contributors=contributors,
            strip_cdr_energy=strip_cdr_energy,
        )

    def _export_datapackage(
        self,
        name: str,
        contributors: list = None,
        strip_cdr_energy: bool = False,
    ):

        for scenario in self.datapackage.scenarios:
            load_database(scenario, self.datapackage.database)

        # first, delete the content of the "pathways_temp" folder
        shutil.rmtree(Path.cwd() / "pathways_temp", ignore_errors=True)

        if strip_cdr_energy:
            # remove energy inputs from CDR datasets
            self._create_energy_stripped_cdr_datasets()

        # create matrices in current directory
        self.datapackage.write_db_to_matrices(
            filepath=str(Path.cwd() / "pathways_temp" / "inventories"),
        )
        self.variables_name_change = {}
        self._add_variables_mapping()
        self._add_scenario_data()
        self._add_classifications_file()
        self._build_datapackage(name, contributors)

    def _create_energy_stripped_cdr_datasets(self):
        """
        Create modified CDR datasets with energy inputs removed for pathways analysis.

        Removes electricity (kilowatt hour) and heat (megajoule) inputs to avoid
        double-counting when using "startswith('SE - cdr')" variables in pathways.
        Material inputs, infrastructure, and negative CO2 emissionsa are preserved.

        The mapping is then updated to point to these modified datasets.
        """
        import uuid

        modifications_report = []

        for scenario in self.datapackage.scenarios:
            if "mapping" not in scenario or "cdr" not in scenario["mapping"]:
                continue

            energy_stripped_mapping = {}
            total_removed = 0

            for cdr_tech, cdr_datasets in scenario["mapping"]["cdr"].items():

                energy_stripped_mapping[cdr_tech] = []

                for cdr_ds_info in cdr_datasets:
                    # 1. Find the original dataset in the database
                    try:
                        original_dataset = ws.get_one(
                            scenario["database"],
                            ws.equals("name", cdr_ds_info["name"]),
                            ws.equals(
                                "reference product", cdr_ds_info["reference product"]
                            ),
                            ws.equals("location", cdr_ds_info["location"]),
                        )
                    except:
                        # Dataset not found, keep original
                        energy_stripped_mapping[cdr_tech].append(cdr_ds_info)
                        print(
                            f"WARNING: CDR dataset not found: {cdr_ds_info['name']} "
                            f"in {cdr_ds_info['location']}"
                        )
                        modifications_report.append(
                            {
                                "model": scenario["model"],
                                "pathway": scenario["pathway"],
                                "year": scenario["year"],
                                "technology": cdr_tech,
                                "location": cdr_ds_info["location"],
                                "original_name": cdr_ds_info["name"],
                                "new_name": "NOT FOUND - SKIPPED",
                                "electricity_removed_kwh": 0,
                                "heat_removed_mj": 0,
                            }
                        )
                        continue
                    # 2. Create new dataset based on original, removing energy inputs
                    new_dataset = {
                        "name": f"{original_dataset['name']}, energy-free for pathways",
                        "location": original_dataset["location"],
                        "reference product": original_dataset["reference product"],
                        "unit": original_dataset["unit"],
                        "code": str(uuid.uuid4()),
                        "database": original_dataset.get("database"),
                        "comment": (
                            f"Modified version of '{original_dataset['name']}' for pathways analysis. "
                            f"Energy inputs (electricity, heat) removed to avoid double-counting "
                            f"when using final energy variables. Material inputs, infrastructure, "
                            f"and negative CO2 emissions preserved."
                        ),
                        "exchanges": [],
                    }

                    for field in ["production amount", "categories"]:
                        if field in original_dataset:
                            new_dataset[field] = original_dataset[field]

                    removed_electricity = 0
                    removed_heat = 0

                    for exc in original_dataset.get("exchanges", []):
                        if exc["type"] == "production":
                            new_dataset["exchanges"].append(exc.copy())
                            continue
                        if exc["type"] == "biosphere":
                            new_dataset["exchanges"].append(exc.copy())
                            continue

                        if exc["type"] == "technosphere":
                            is_energy = False
                            exc_name_lower = exc.get("name", "").lower()
                            exc_unit = exc.get("unit", "")

                            # Check for electricity: "electricity" in name AND unit "kilowatt hour"
                            if (
                                "electricity" in exc_name_lower
                                and exc_unit == "kilowatt hour"
                            ):
                                is_energy = True
                                removed_electricity += exc.get("amount", 0)
                            # Check for heat: "heat" in name AND unit "megajoule"
                            elif "heat" in exc_name_lower and exc_unit == "megajoule":
                                is_energy = True
                                removed_heat += exc.get("amount", 0)

                            if not is_energy:
                                # Keep non-energy inputs
                                new_dataset["exchanges"].append(exc.copy())

                    # Update production exchange to match new dataset name
                    for exc in ws.production(new_dataset):
                        exc["name"] = new_dataset["name"]
                        if "input" in exc:
                            del exc["input"]

                    new_dataset["log parameters"] = {
                        "electricity removed (kWh)": removed_electricity,
                        "heat removed (MJ)": removed_heat,
                        "original dataset": original_dataset["name"],
                    }

                    # 3. Add new dataset to database
                    scenario["database"].append(new_dataset)

                    # 4. Update mapping
                    energy_stripped_mapping[cdr_tech].append(
                        {
                            "name": new_dataset["name"],
                            "reference product": new_dataset["reference product"],
                            "unit": new_dataset["unit"],
                            "location": new_dataset["location"],
                        }
                    )

                    modifications_report.append(
                        {
                            "model": scenario["model"],
                            "pathway": scenario["pathway"],
                            "year": scenario["year"],
                            "technology": cdr_tech,
                            "location": new_dataset["location"],
                            "original_name": original_dataset["name"],
                            "new_name": new_dataset["name"],
                            "electricity_removed_kwh": removed_electricity,
                            "heat_removed_mj": removed_heat,
                        }
                    )

                    total_removed += 1

            # 5. Replace original mapping with energy-stripped version
            if energy_stripped_mapping:
                scenario["mapping"]["cdr"] = energy_stripped_mapping

        if modifications_report:
            self._write_cdr_modifications_report(modifications_report)

    def _add_variables_mapping(self):
        """
        Add variables mapping in the "pathways" folder.

        """

        mappings = {}
        for scenario in self.datapackage.scenarios:
            for sector, mapping in scenario["mapping"].items():
                if sector == "final energy":
                    prefix = "FE"
                elif sector.startswith("external"):
                    prefix = "EXT"
                else:
                    prefix = "SE"

                for k, v in mapping.items():
                    datasets = []
                    for x in v:
                        data = {
                            "name": x["name"],
                            "reference product": x["reference product"],
                            "unit": x["unit"],
                        }
                        if "lhv" in x:
                            data["lhv"] = x["lhv"]
                        datasets.append(data)
                    mappings[f"{prefix} - {sector.replace('external_', '')} - {k}"] = {
                        "dataset": [
                            json.loads(s)
                            for s in {json.dumps(d, sort_keys=True) for d in datasets}
                        ]
                    }
                    self.variables_name_change[k] = f"{prefix} - {sector} - {k}"

        # CAPACITY ADDITION HANDLING
        patched_capacity_addition = None
        if "premise.final_energy" in sys.modules:
            final_energy_module = sys.modules["premise.final_energy"]
            patched_capacity_addition = getattr(
                final_energy_module, "_PATCHED_CAPACITY_ADDITION", None
            )

        if patched_capacity_addition:
            print(
                f"✅ Found globally stored capacity addition data with {len(patched_capacity_addition)} variables"
            )

            # Use first scenario's database to find datasets
            database = self.datapackage.scenarios[0]["database"]

            for yaml_key, var_config in patched_capacity_addition.items():
                if "ecoinvent_aliases" in var_config:
                    ecoinvent_aliases = var_config["ecoinvent_aliases"]
                    fltr = ecoinvent_aliases.get("fltr", {})

                    if fltr:
                        # Find datasets matching the filter
                        datasets = []

                        # Build filter for name
                        filters = []
                        if "name" in fltr:
                            name_value = fltr["name"]
                            if isinstance(name_value, list):
                                # All names must appear (AND logic)
                                for name in name_value:
                                    filters.append(ws.contains("name", name))
                            else:
                                filters.append(ws.contains("name", name_value))

                        # Build filter for reference product
                        if "reference product" in fltr:
                            ref_prod_value = fltr["reference product"]
                            if isinstance(ref_prod_value, list):
                                for ref_prod in ref_prod_value:
                                    filters.append(ws.contains("reference product", ref_prod))
                            else:
                                filters.append(ws.contains("reference product", ref_prod_value))

                        # Find matching datasets
                        if filters:
                            matched = list(ws.get_many(database, *filters))

                            # Apply mask if present
                            if "mask" in fltr:
                                mask = fltr["mask"]
                                if isinstance(mask, str):
                                    matched = [ds for ds in matched if mask not in ds["name"]]
                                elif isinstance(mask, list):
                                    matched = [
                                        ds for ds in matched
                                        if not any(m in ds["name"] for m in mask)
                                    ]

                            # Extract dataset info
                            for ds in matched:
                                datasets.append({
                                    "name": ds["name"],
                                    "reference product": ds["reference product"],
                                    "unit": ds.get("unit", "unit"),
                                })

                        # Deduplicate datasets
                        unique_datasets = [
                            dict(t)
                            for t in {
                                tuple(sorted(d.items()))
                                for d in datasets
                            }
                        ]

                        if unique_datasets:
                            # Use YAML key directly as the variable name
                            mappings[yaml_key] = {
                                "dataset": unique_datasets
                            }
                            print(f"  ✅ Added capacity mapping: {yaml_key} ({len(unique_datasets)} dataset(s))")
                        else:
                            print(f"  ⚠️ No datasets found for capacity addition: {yaml_key}")
        else:
            print("ℹ️ No globally stored capacity addition data found")

        # Handle external scenarios with production pathways
        for scenario in self.datapackage.scenarios:
            if "configurations" in scenario:
                configurations = scenario["configurations"]
                for config_key, config_value in configurations.items():
                    # Process production pathways from external scenarios
                    for variable, variable_details in config_value.get("production pathways", {}).items():
                        if variable not in mappings:
                            variable_scenario_name = variable_details.get(
                                "production volume", {}
                            ).get("variable", variable)

                            ecoinvent_alias = variable_details.get("ecoinvent alias", {})

                            # Find datasets using ecoinvent alias filters
                            filters = []
                            if "name" in ecoinvent_alias:
                                filters.append(ws.contains("name", ecoinvent_alias["name"]))
                            if "reference product" in ecoinvent_alias:
                                filters.append(
                                    ws.contains("reference product", ecoinvent_alias["reference product"])
                                )

                            if filters:
                                matched = list(ws.get_many(scenario["database"], *filters))

                                # Apply mask if present
                                mask = ecoinvent_alias.get("mask")
                                if mask:
                                    if isinstance(mask, str):
                                        matched = [ds for ds in matched if mask not in ds["name"]]
                                    elif isinstance(mask, list):
                                        matched = [
                                            ds for ds in matched
                                            if not any(m in ds["name"] for m in mask)
                                        ]

                                datasets = [
                                    {
                                        "name": ds["name"],
                                        "reference product": ds["reference product"],
                                        "unit": ds.get("unit", "unit"),
                                    }
                                    for ds in matched
                                ]

                                # Deduplicate
                                unique_datasets = [
                                    dict(t)
                                    for t in {
                                        tuple(sorted(d.items()))
                                        for d in datasets
                                    }
                                ]

                                if unique_datasets:
                                    mappings[variable] = {
                                        "scenario variable": variable_scenario_name,
                                        "dataset": unique_datasets
                                    }
                                else:
                                    print(
                                        f"⚠️ No dataset found for {variable} in {variable_scenario_name}"
                                    )

                    for yaml_key, cap_config in config_value.get("capacity_addition", {}).items():
                        if yaml_key == "settings":  # Skip settings block
                            continue

                        if yaml_key not in mappings:
                            # Determine the capacity dataset name that was created
                            if yaml_key.startswith("Sales - Transport"):
                                suffix = yaml_key.replace("Sales - Transport - ", "").strip()
                                capacity_name = f"transport capacity addition, 1 million units, {suffix}"
                            else:
                                suffix = yaml_key.replace("New Cap - ", "").strip()
                                capacity_name = f"capacity addition, 1GW, {suffix}"

                            # Find the created capacity datasets in the database
                            matched = list(ws.get_many(
                                scenario["database"],
                                ws.equals("name", capacity_name)
                            ))

                            if matched:
                                datasets = [{
                                    "name": ds["name"],
                                    "reference product": ds["reference product"],
                                    "unit": ds.get("unit", "unit"),
                                } for ds in matched]

                                # Deduplicate
                                unique_datasets = [
                                    dict(t) for t in {
                                        tuple(sorted(d.items())) for d in datasets
                                    }
                                ]

                                if unique_datasets:
                                    mappings[yaml_key] = {
                                        "dataset": unique_datasets
                                    }
                                    print(
                                        f"  ✅ Added external capacity mapping: {yaml_key} ({len(unique_datasets)} dataset(s))")
                            else:
                                print(f"  ⚠️ No external capacity datasets found for: {yaml_key}")

        # create a "mapping" folder inside "pathways"
        (Path.cwd() / "pathways_temp" / "mapping").mkdir(parents=True, exist_ok=True)

        with open(Path.cwd() / "pathways_temp" / "mapping" / "mapping.yaml", "w") as f:
            yaml.dump(mappings, f)

    def _add_scenario_data(self):
        """
        Add scenario data in the "pathways_temp" folder.
        """

        def _prefix_vars(arr, prefix: str):
            # prefix the variables coordinate
            new_vars = [
                f"{prefix} - {v}" for v in arr.coords["variables"].values.tolist()
            ]
            return arr.assign_coords(variables=("variables", new_vars))

        data_list, extra_units = [], {}

        for scenario in self.datapackage.scenarios:
            # --- base: production volumes
            pv = scenario["iam data"].production_volumes.interp(year=scenario["year"])
            old_vars = pv.coords["variables"].values.tolist()
            # translate model var -> final mapping key; fallback to readable default
            new_vars = [self.variables_name_change.get(v, v) for v in old_vars]
            pv = pv.assign_coords(variables=("variables", new_vars))
            # same for units
            units = {
                self.variables_name_change.get(k, k): v
                for k, v in pv.attrs.get("unit", {}).items()
            }

            scenario_name = f"{scenario['model']} - {scenario['pathway']}"

            # --- optional: capacity addition data
            if "premise.final_energy" in sys.modules:
                final_energy_module = sys.modules["premise.final_energy"]
                patched_capacity_addition = getattr(
                    final_energy_module, "_PATCHED_CAPACITY_ADDITION", None
                )
                patched_units = getattr(
                    final_energy_module, "_PATCHED_CAPACITY_UNITS", None
                )

                if patched_capacity_addition and patched_units:
                    # Build mapping from YAML keys to IAM variable names
                    key_to_iam_vars = {}
                    for yaml_key, var_config in patched_capacity_addition.items():
                        if "iam_aliases" in var_config:
                            iam_var = var_config["iam_aliases"].get(scenario["model"])
                            if iam_var:
                                if isinstance(iam_var, list):
                                    key_to_iam_vars[yaml_key] = iam_var
                                else:
                                    key_to_iam_vars[yaml_key] = [iam_var]

                    # Extract capacity addition variables from IAM data
                    if key_to_iam_vars and hasattr(scenario["iam data"], "data"):
                        iam_data_full = scenario["iam data"].data
                        capacity_pieces = []
                        remapped_units = {}

                        for yaml_key, iam_vars in key_to_iam_vars.items():
                            existing_vars = [
                                v for v in iam_vars
                                if v in iam_data_full.coords["variables"].values
                            ]

                            if not existing_vars:
                                continue

                            capacity_da = iam_data_full.sel(
                                variables=existing_vars
                            ).interp(year=scenario["year"])

                            # If multiple variables, sum them
                            if len(existing_vars) > 1:
                                capacity_da = capacity_da.sum(dim="variables", keep_attrs=True)
                                if "variables" not in capacity_da.dims:
                                    capacity_da = capacity_da.expand_dims("variables")

                            # Rename to YAML key
                            capacity_da = capacity_da.assign_coords(variables=[yaml_key])
                            capacity_pieces.append(capacity_da)

                            # Get unit
                            for iam_var in existing_vars:
                                if iam_var in patched_units:
                                    remapped_units[yaml_key] = patched_units[iam_var]
                                    break

                        if capacity_pieces:
                            capacity_data = xr.concat(capacity_pieces, dim="variables")
                            pv = xr.concat([pv, capacity_data], dim="variables")
                            units.update(remapped_units)
                            extra_units.update(remapped_units)
                            print(f"  ✅ Added {len(capacity_pieces)} internal capacity variables to scenario")
                            print(f"     Variables: {[yaml_key for yaml_key in key_to_iam_vars.keys()][:5]}...")

            if "configurations" in scenario:
                for config_idx, config_value in scenario["configurations"].items():
                    external_capacity = config_value.get("capacity_addition", {})

                    if external_capacity:
                        # Build mapping from YAML keys to IAM variable names
                        ext_key_to_iam_vars = {}
                        for yaml_key, cap_config in external_capacity.items():
                            if yaml_key == "settings":
                                continue

                            if "iam_aliases" in cap_config:
                                iam_var = cap_config["iam_aliases"].get(scenario["model"])
                                if iam_var:
                                    if isinstance(iam_var, list):
                                        ext_key_to_iam_vars[yaml_key] = iam_var
                                    else:
                                        ext_key_to_iam_vars[yaml_key] = [iam_var]

                        # Extract external capacity addition variables from external scenario data
                        if ext_key_to_iam_vars and config_idx in scenario.get("external data", {}):
                            external_data = scenario["external data"][config_idx]

                            # Check if this external scenario has production volume data
                            if "production volume" in external_data:
                                ext_capacity_pieces = []
                                ext_remapped_units = {}

                                for yaml_key, iam_vars in ext_key_to_iam_vars.items():
                                    # Check which IAM variables exist in external data
                                    existing_vars = [
                                        v for v in iam_vars
                                        if v in external_data["production volume"].coords["variables"].values
                                    ]

                                    if not existing_vars:
                                        continue

                                    # Extract data for these variables
                                    capacity_da = external_data["production volume"].sel(
                                        variables=existing_vars
                                    ).interp(year=scenario["year"])

                                    # If multiple variables, sum them
                                    if len(existing_vars) > 1:
                                        capacity_da = capacity_da.sum(dim="variables", keep_attrs=True)
                                        if "variables" not in capacity_da.dims:
                                            capacity_da = capacity_da.expand_dims("variables")

                                    # Rename to YAML key
                                    capacity_da = capacity_da.assign_coords(variables=[yaml_key])
                                    ext_capacity_pieces.append(capacity_da)

                                    # Get unit from external data
                                    for iam_var in existing_vars:
                                        pv_attrs = external_data["production volume"].attrs.get("unit", {})
                                        if iam_var in pv_attrs:
                                            ext_remapped_units[yaml_key] = pv_attrs[iam_var]
                                            break

                                if ext_capacity_pieces:
                                    ext_capacity_data = xr.concat(ext_capacity_pieces, dim="variables")
                                    pv = xr.concat([pv, ext_capacity_data], dim="variables")
                                    units.update(ext_remapped_units)
                                    extra_units.update(ext_remapped_units)
                                    print(
                                        f"  ✅ Added {len(ext_capacity_pieces)} external capacity variables from config {config_idx}")

            # --- optional: external data blocks
            if "external data" in scenario:
                for ext_key, external in scenario["external data"].items():
                    ext = external["production volume"].interp(year=scenario["year"])
                    # prefix includes the external block key so different externals don't collide
                    ext_prefix = f"EXT - {ext_key}"
                    ext = _prefix_vars(ext, ext_prefix)
                    ext_units = {
                        f"{ext_prefix} - {k}": v
                        for k, v in external["production volume"]
                        .attrs.get("unit", {})
                        .items()
                    }

                    pv = xr.concat([pv, ext], dim="variables")
                    units.update(ext_units)
                    extra_units.update(ext_units)
                    scenario_name += (
                        f" - {scenario['external scenarios'][ext_key]['scenario']}"
                    )

            # add scenario dimension
            pv = pv.expand_dims("scenario")
            pv = pv.assign_coords(scenario=[scenario_name])

            # keep the merged units on the array (xarray may drop attrs on concat later)
            pv.attrs["unit"] = units

            data_list.append(pv)

        # concat all scenarios
        array = xr.concat(data_list, dim="scenario")

        # ensure output dir
        outdir = Path.cwd() / "pathways_temp" / "scenario_data"
        outdir.mkdir(parents=True, exist_ok=True)

        # dataframe export
        df = array.to_dataframe().reset_index()

        # units column (lookup matches our prefixed variable names)
        # prefer array-level units, then fall back to extra_units if you keep that convention
        unit_map = dict(array.attrs.get("unit", {}))
        unit_map.update(extra_units)  # in case you want this precedence
        df["unit"] = df["variables"].map(unit_map)

        # split scenario into model/pathway
        df[["model", "pathway"]] = df["scenario"].str.split(" - ", n=1, expand=True)
        df = df.drop(columns=["scenario"])

        self.scenario_names = df["pathway"].unique().tolist()

        df = df.dropna(subset=["value"])

        # Normalize CDR signs: They have to be always positive values in pathways
        cdr_mask = df["variables"].str.contains("SE - cdr", case=False, na=False)
        if cdr_mask.any():
            df.loc[cdr_mask, "value"] = df.loc[cdr_mask, "value"].abs()

        outfile = outdir / "scenario_data.csv"
        if outfile.exists():
            outfile.unlink()
        df.to_csv(outfile, index=False)

        if "premise.final_energy" in sys.modules:
            final_energy_module = sys.modules["premise.final_energy"]
            if hasattr(final_energy_module, "_PATCHED_CAPACITY_ADDITION"):
                delattr(final_energy_module, "_PATCHED_CAPACITY_ADDITION")
            if hasattr(final_energy_module, "_PATCHED_CAPACITY_UNITS"):
                delattr(final_energy_module, "_PATCHED_CAPACITY_UNITS")
            print("🧹 Cleaned up global capacity addition state")

    def _add_classifications_file(self):
        """
        Export activity classifications to a CSV file in the datapackage.

        Each row is one activity–classification pair with columns:
        - name
        - reference product
        - unit
        - location
        - classification_system  (e.g. "CPC", "ISIC")
        - classification_code    (e.g. "xxxx: manufacture of ...")

        Databases are taken from each scenario dict under key "database",
        where "database" is a list of activity dictionaries.
        """

        outdir = Path.cwd() / "pathways_temp" / "classifications"
        outdir.mkdir(parents=True, exist_ok=True)

        outfile = outdir / "classifications.csv"

        fieldnames = [
            "name",
            "reference product",
            "classification_system",
            "classification_code",
        ]

        seen = set()

        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for scenario in self.datapackage.scenarios:
                db = scenario.get("database") or []
                for ds in db:
                    name = ds["name"]
                    ref = ds["reference product"]

                    # classifications is a list of tuples:
                    classifications = ds.get("classifications") or []

                    if not classifications:
                        print(f"No classifications for {name}")
                        if (
                            ds["name"],
                            ds["reference product"],
                        ) in self.classifications:
                            ds["classifications"] = [
                                (
                                    "ISIC rev.4 ecoinvent",
                                    self.classifications[
                                        (ds["name"], ds["reference product"])
                                    ]["ISIC rev.4 ecoinvent"],
                                ),
                                (
                                    "CPC",
                                    self.classifications[
                                        (ds["name"], ds["reference product"])
                                    ]["CPC"],
                                ),
                            ]
                            classifications = ds.get("classifications")

                    for system, code in classifications:
                        key = (name, ref, system, code)
                        if key in seen:
                            continue
                        seen.add(key)

                        writer.writerow(
                            {
                                "name": name,
                                "reference product": ref,
                                "classification_system": system,
                                "classification_code": code,
                            }
                        )

    def _build_datapackage(self, name: str, contributors: list = None):
        """
        Create and export a scenario datapackage.
        """
        # create a new datapackage
        package = Package(base_path=Path.cwd().as_posix())
        # Find all CSV files manually
        csv_files = list((Path.cwd() / "pathways_temp").glob("**/*.csv"))

        for file in csv_files:
            relpath = file.relative_to(Path.cwd()).as_posix()
            package.add_resource(
                {
                    "path": relpath,
                    "profile": "tabular-data-resource",
                    "encoding": "utf-8",
                }
            )

        package.infer("pathways_temp/**/*.yaml")
        package.infer("pathways_temp/**/*.txt")
        package.infer()

        package.descriptor["name"] = name.replace(" ", "_").lower()
        package.descriptor["title"] = name.capitalize()
        package.descriptor["description"] = (
            f"Data package generated by premise {__version__}."
        )
        package.descriptor["premise version"] = str(__version__)
        package.descriptor["scenarios"] = self.scenario_names
        package.descriptor["keywords"] = [
            "ecoinvent",
            "scenario",
            "data package",
            "premise",
            "pathways",
        ]
        package.descriptor["licenses"] = [
            {
                "id": "CC0-1.0",
                "title": "CC0 1.0",
                "url": "https://creativecommons.org/publicdomain/zero/1.0/",
            }
        ]

        if contributors is None:
            contributors = [
                {
                    "title": "undefined",
                    "name": "anonymous",
                    "email": "anonymous@anonymous.com",
                }
            ]
        else:
            contributors = [
                {
                    "title": c.get("title", "undefined"),
                    "name": c.get("name", "anonymous"),
                    "email": c.get("email", "anonymous@anonymous.com"),
                }
                for c in contributors
            ]
        package.descriptor["contributors"] = contributors
        package.commit()

        # save the json file
        package.save(str(Path.cwd() / "pathways_temp" / "datapackage.json"))

        # open the json file and ensure that all resource names are slugified
        with open(Path.cwd() / "pathways_temp" / "datapackage.json", "r") as f:
            data = yaml.full_load(f)

        for resource in data["resources"]:
            resource["name"] = resource["name"].replace(" ", "_").lower()

        # also, remove "pathways/" from the path of each resource
        for resource in data["resources"]:
            path = resource["path"]
            path = path.replace("pathways_temp", "pathways")
            path = path.replace("pathways/", "").replace("pathways\\", "")
            path = path.replace("\\", "/")
            resource["path"] = path

        # save it back as a json file
        with open(Path.cwd() / "pathways_temp" / "datapackage.json", "w") as fp:
            json.dump(data, fp)

        # zip the folder
        shutil.make_archive(name, "zip", str(Path.cwd() / "pathways_temp"))

        print(f"Data package saved at {str(Path.cwd() / f'{name}.zip')}")

    def _write_cdr_modifications_report(self, modifications: list):
        """
        Write a report of CDR modifications to a text file in the datapackage.

        :param modifications: List of dictionaries containing modification details
        """
        report_dir = Path.cwd() / "pathways_temp"
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "cdr_energy_modifications.txt"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("CDR ENERGY INPUTS MODIFICATION REPORT\n")
            f.write("=" * 80 + "\n\n")

            f.write(
                "This report documents modifications made to Carbon Dioxide Removal (CDR)\n"
            )
            f.write(
                "datasets for pathways analysis. Energy inputs (electricity and heat) have\n"
            )
            f.write(
                "been removed to avoid double-counting when using final energy variables.\n\n"
            )

            f.write(
                "Material inputs, infrastructure, and negative CO2 emissions are preserved.\n"
            )
            f.write("-" * 80 + "\n\n")

            # Group by scenario
            from collections import defaultdict

            by_scenario = defaultdict(list)
            for mod in modifications:
                key = (mod["model"], mod["pathway"], mod["year"])
                by_scenario[key].append(mod)

            for (model, pathway, year), mods in sorted(by_scenario.items()):
                f.write(f"\nScenario: {model} - {pathway} ({year})\n")
                f.write("=" * 80 + "\n\n")

                # Group by technology
                by_tech = defaultdict(list)
                for mod in mods:
                    by_tech[mod["technology"]].append(mod)

                for tech, tech_mods in sorted(by_tech.items()):
                    f.write(f"  Technology: {tech}\n")
                    f.write("  " + "-" * 76 + "\n")

                    total_elec = 0
                    total_heat = 0

                    for mod in tech_mods:
                        f.write(f"\n  Location: {mod['location']}\n")
                        f.write(f"    Original dataset: {mod['original_name']}\n")
                        f.write(f"    New dataset:      {mod['new_name']}\n")

                        if mod["electricity_removed_kwh"] > 0:
                            f.write(
                                f"    Electricity removed: {mod['electricity_removed_kwh']:,.2f} kWh\n"
                            )
                            total_elec += mod["electricity_removed_kwh"]

                        if mod["heat_removed_mj"] > 0:
                            f.write(
                                f"    Heat removed:        {mod['heat_removed_mj']:,.2f} MJ\n"
                            )
                            total_heat += mod["heat_removed_mj"]

                    # Summary for this technology
                    f.write(f"\n  Summary for {tech}:\n")
                    f.write(f"    Total datasets modified: {len(tech_mods)}\n")
                    if total_elec > 0:
                        f.write(
                            f"    Total electricity removed: {total_elec:,.2f} kWh\n"
                        )
                    if total_heat > 0:
                        f.write(f"    Total heat removed: {total_heat:,.2f} MJ\n")
                    f.write("\n")

            # Overall summary
            f.write("\n" + "=" * 80 + "\n")
            f.write("OVERALL SUMMARY\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total CDR datasets modified: {len(modifications)}\n")
            f.write(f"Scenarios processed: {len(by_scenario)}\n")

            # Count unique technologies
            unique_techs = set(mod["technology"] for mod in modifications)
            f.write(f"CDR technologies affected: {len(unique_techs)}\n")
            for tech in sorted(unique_techs):
                f.write(f"  - {tech}\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("End of report\n")
            f.write("=" * 80 + "\n")

        print(f"CDR modifications report saved: {report_path}")
