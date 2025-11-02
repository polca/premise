"""
This module contains the PathwaysDataPackage class, which is
used to create a data package for scenario analysis.
"""

import json
import shutil
from datetime import date
from pathlib import Path
from typing import List

import xarray as xr
import yaml
from datapackage import Package

from . import __version__
from .new_database import NewDatabase
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
        )

        self.scenario_names = []

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

        self.export_datapackage(
            name=name,
            contributors=contributors,
            strip_cdr_energy=strip_cdr_energy,
        )

    def export_datapackage(
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
        self.add_variables_mapping()
        self.add_scenario_data()
        self.build_datapackage(name, contributors)

    def _create_energy_stripped_cdr_datasets(self):
        """
        Create modified CDR datasets with energy inputs removed for pathways analysis.

        Removes electricity (kilowatt hour) and heat (megajoule) inputs to avoid
        double-counting when using "startswith('SE - cdr')" variables in pathways.
        Material inputs, infrastructure, and negative CO2 emissionsa are preserved.

        The mapping is then updated to point to these modified datasets.
        """
        from .transformation import ws
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
                            ws.equals("reference product", cdr_ds_info["reference product"]),
                            ws.equals("location", cdr_ds_info["location"]),
                        )
                    except:
                        # Dataset not found, keep original
                        energy_stripped_mapping[cdr_tech].append(cdr_ds_info)
                        print(
                            f"WARNING: CDR dataset not found: {cdr_ds_info['name']} "
                            f"in {cdr_ds_info['location']}"
                        )
                        modifications_report.append({
                            "model": scenario["model"],
                            "pathway": scenario["pathway"],
                            "year": scenario["year"],
                            "technology": cdr_tech,
                            "location": cdr_ds_info['location'],
                            "original_name": cdr_ds_info['name'],
                            "new_name": "NOT FOUND - SKIPPED",
                            "electricity_removed_kwh": 0,
                            "heat_removed_mj": 0,
                        })
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
                            if "electricity" in exc_name_lower and exc_unit == "kilowatt hour":
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
                    energy_stripped_mapping[cdr_tech].append({
                        "name": new_dataset["name"],
                        "reference product": new_dataset["reference product"],
                        "unit": new_dataset["unit"],
                        "location": new_dataset["location"],
                    })

                    modifications_report.append({
                        "model": scenario["model"],
                        "pathway": scenario["pathway"],
                        "year": scenario["year"],
                        "technology": cdr_tech,
                        "location": new_dataset["location"],
                        "original_name": original_dataset["name"],
                        "new_name": new_dataset["name"],
                        "electricity_removed_kwh": removed_electricity,
                        "heat_removed_mj": removed_heat,
                    })

                    total_removed += 1

            # 5. Replace original mapping with energy-stripped version
            if energy_stripped_mapping:
                scenario["mapping"]["cdr"] = energy_stripped_mapping

        if modifications_report:
            self._write_cdr_modifications_report(modifications_report)

    def add_variables_mapping(self):
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
                    mappings[f"{prefix} - {sector} - {k}"] = {
                        "dataset": [
                            json.loads(s)
                            for s in {json.dumps(d, sort_keys=True) for d in datasets}
                        ]
                    }
                    self.variables_name_change[k] = f"{prefix} - {sector} - {k}"

        # create a "mapping" folder inside "pathways"
        (Path.cwd() / "pathways_temp" / "mapping").mkdir(parents=True, exist_ok=True)

        with open(Path.cwd() / "pathways_temp" / "mapping" / "mapping.yaml", "w") as f:
            yaml.dump(mappings, f)

    def add_scenario_data(self):
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
        cdr_mask = df['variables'].str.contains('SE - cdr', case=False, na=False)
        if cdr_mask.any():
            df.loc[cdr_mask, 'value'] = df.loc[cdr_mask, 'value'].abs()

        outfile = outdir / "scenario_data.csv"
        if outfile.exists():
            outfile.unlink()
        df.to_csv(outfile, index=False)

    def build_datapackage(self, name: str, contributors: list = None):
        """
        Create and export a scenario datapackage.
        """
        # create a new datapackage
        package = Package(base_path=Path.cwd().as_posix())
        package.infer("pathways_temp/**/*.csv")
        package.infer("pathways_temp/**/*.yaml")
        package.infer("pathways_temp/**/*.txt")

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

            f.write("This report documents modifications made to Carbon Dioxide Removal (CDR)\n")
            f.write("datasets for pathways analysis. Energy inputs (electricity and heat) have\n")
            f.write("been removed to avoid double-counting when using final energy variables.\n\n")

            f.write("Material inputs, infrastructure, and negative CO2 emissions are preserved.\n")
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

                        if mod['electricity_removed_kwh'] > 0:
                            f.write(f"    Electricity removed: {mod['electricity_removed_kwh']:,.2f} kWh\n")
                            total_elec += mod['electricity_removed_kwh']

                        if mod['heat_removed_mj'] > 0:
                            f.write(f"    Heat removed:        {mod['heat_removed_mj']:,.2f} MJ\n")
                            total_heat += mod['heat_removed_mj']

                    # Summary for this technology
                    f.write(f"\n  Summary for {tech}:\n")
                    f.write(f"    Total datasets modified: {len(tech_mods)}\n")
                    if total_elec > 0:
                        f.write(f"    Total electricity removed: {total_elec:,.2f} kWh\n")
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

