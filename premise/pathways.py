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

import xarray as xr
import yaml
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
        )

        self.scenario_names = []
        self.classifications = get_classifications()

    def create_datapackage(
        self,
        name: str = f"pathways_{date.today()}",
        contributors: list = None,
        transformations: list = None,
    ):
        if transformations:
            self.datapackage.update(transformations)
        else:
            self.datapackage.update()

        self._export_datapackage(
            name=name,
            contributors=contributors,
        )

    def _export_datapackage(
        self,
        name: str,
        contributors: list = None,
    ):

        # first, delete the content of the "pathways_temp" folder
        shutil.rmtree(Path.cwd() / "pathways_temp", ignore_errors=True)

        # create matrices in current directory
        self.datapackage.write_db_to_matrices(
            filepath=str(Path.cwd() / "pathways_temp" / "inventories"),
        )
        self.variables_name_change = {}
        self._add_variables_mapping()
        self._add_scenario_data()
        self._add_classifications_file()
        self._build_datapackage(name, contributors)

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

        outfile = outdir / "scenario_data.csv"
        if outfile.exists():
            outfile.unlink()
        df.to_csv(outfile, index=False)

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
