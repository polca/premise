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
from .activity_maps import act_fltr
from .final_energy import FinalEnergy
from .new_database import NewDatabase
from .utils import dump_database, load_database


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
    ):
        if transformations:
            self.datapackage.update(transformations)
        else:
            self.datapackage.update()

        self.export_datapackage(
            name=name,
            contributors=contributors,
        )

    def export_datapackage(
        self,
        name: str,
        contributors: list = None,
    ):

        for scenario in self.datapackage.scenarios:
            load_database(scenario, self.datapackage.database)

        # first, delete the content of the "pathways_temp" folder
        shutil.rmtree(Path.cwd() / "pathways_temp", ignore_errors=True)

        # create matrices in current directory
        self.datapackage.write_db_to_matrices(
            filepath=str(Path.cwd() / "pathways_temp" / "inventories"),
        )
        self.variables_name_change = {}
        self.add_variables_mapping()
        self.add_scenario_data()
        self.build_datapackage(name, contributors)

    def find_activities(
        self, filters: [str, list], database, mask: [str, list, None] = None
    ):
        """
        Find activities in the database.

        :param filters: value(s) to filter with.
        :type filters: Union[str, lst, dict]
        :param mask: value(s) to filter with.
        :type mask: Union[str, lst, dict]
        :param database: A lice cycle inventory database
        :type database: brightway2 database object
        :return: list dictionaries with activity names, reference products and units
        """
        # remove unwanted keys, anything other than "name", "reference product" and "unit"
        if isinstance(filters, dict):
            filters = {
                k: v
                for k, v in filters.items()
                if k in ["name", "reference product", "unit"]
            }

        return [
            {
                "name": act["name"],
                "reference product": act["reference product"],
                "unit": act["unit"],
            }
            for act in act_fltr(
                database=database,
                fltr=filters,
                mask=mask or {},
            )
        ]

    def add_variables_mapping(self):
        """
        Add variables mapping in the "pathways" folder.

        """

        # create a "mapping" folder inside "pathways"
        (Path.cwd() / "pathways_temp" / "mapping").mkdir(parents=True, exist_ok=True)

        # make a list of unique variables
        vars = [
            self.datapackage.scenarios[s]["iam data"]
            .data.coords["variables"]
            .values.tolist()
            for s in range(len(self.scenarios))
        ]

        # extend to variables in external scenarios
        for scenario in self.scenarios:
            if "external scenarios" in scenario:
                for s in scenario["external data"]:
                    vars.extend(
                        [
                            scenario["external data"][s]["production volume"]
                            .coords["variables"]
                            .values.tolist()
                        ]
                    )

        # remove efficiency and emissions variables
        vars = [
            [
                v
                for v in var
                if "efficiency" not in v.lower() and "emission" not in v.lower()
            ]
            for var in vars
        ]

        # concatenate the list
        vars = list(set([item for sublist in vars for item in sublist]))

        mapping = {}

        # iterate through all YAML files contained in the "iam_variables_mapping" folder
        # the folder is located in the same folder as this module

        model_variables = []

        for file in (
            Path(__file__).resolve().parent.glob("iam_variables_mapping/*.yaml")
        ):
            # open the file
            with open(file, "r") as f:
                # load the YAML file
                data = yaml.full_load(f)

            if "final" in file.stem:
                prefix = f"FE - {file.stem}"
            else:
                prefix = f"SE - {file.stem}"

            # iterate through all variables in the YAML file
            for var, val in data.items():
                if all(x in val for x in ["iam_aliases", "ecoinvent_aliases"]):
                    for model, model_var in val["iam_aliases"].items():
                        if model_var in vars and model in [
                            s["model"] for s in self.scenarios
                        ]:

                            model_variables.append(model_var)

                            prefixed_var = f"{prefix} - {var}"
                            self.variables_name_change[var] = prefixed_var

                            mapping[prefixed_var] = {"scenario variable": model_var}
                            mapping[prefixed_var]["dataset"] = self.find_activities(
                                filters=val["ecoinvent_aliases"].get("fltr"),
                                database=self.datapackage.scenarios[0]["database"],
                                mask=val["ecoinvent_aliases"].get("mask"),
                            )
                            mapping[prefixed_var]["dataset"] = [
                                dict(t)
                                for t in {
                                    tuple(sorted(d.items()))
                                    for d in mapping[prefixed_var]["dataset"]
                                }
                            ]
                            if "lhv" in val:
                                mapping[prefixed_var]["lhv"] = val["lhv"]

        # if external scenarios, extend mapping with external data
        for scenario in self.datapackage.scenarios:
            if "configurations" in scenario:
                configurations = scenario["configurations"]
                for key, val in configurations.items():
                    for variable, variable_details in val.get(
                        "production pathways", {}
                    ).items():
                        if variable not in mapping:
                            variable_scenario_name = variable_details.get(
                                "production volume", {}
                            ).get("variable", 0)

                            prefixed_var = f"EXT - {key} - {variable}"

                            mapping[prefixed_var] = {
                                "scenario variable": variable_scenario_name
                            }
                            filters = variable_details.get("ecoinvent alias")
                            mask = variable_details.get("ecoinvent alias").get("mask")

                            mapping[prefixed_var]["dataset"] = self.find_activities(
                                filters=filters,
                                database=scenario["database"],
                                mask=mask,
                            )

                            mapping[prefixed_var]["dataset"] = [
                                dict(t)
                                for t in {
                                    tuple(sorted(d.items()))
                                    for d in mapping[prefixed_var]["dataset"]
                                }
                            ]

                            if len(mapping[prefixed_var]["dataset"]) == 0:
                                print(
                                    f"No dataset found for {prefixed_var} in {variable_scenario_name}"
                                )
                                print(f"Filters: {filters}")
                                print(f"Mask: {mask}")
                                continue

                            variables = [f"EXT - {key} - {v}" for v in list(val["production pathways"].keys())]
                            variables.remove(prefixed_var)
                            # remove datasets which names are in list of variables
                            # except for the current variable
                            if (
                                len(
                                    [
                                        d
                                        for d in mapping[prefixed_var]["dataset"]
                                        if not any(v in d["name"] for v in variables)
                                    ]
                                )
                                > 0
                            ):
                                mapping[prefixed_var]["dataset"] = [
                                    d
                                    for d in mapping[prefixed_var]["dataset"]
                                    if not any(v in d["name"] for v in variables)
                                ]

        with open(Path.cwd() / "pathways_temp" / "mapping" / "mapping.yaml", "w") as f:
            yaml.dump(mapping, f)



    def add_scenario_data(self):
        """
        Add scenario data in the "pathways_temp" folder.
        """

        def _prefix_vars(arr, prefix: str):
            # prefix the variables coordinate
            new_vars = [f"{prefix} - {v}" for v in arr.coords["variables"].values.tolist()]
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
            units = {self.variables_name_change.get(k, k): v for k, v in pv.attrs.get("unit", {}).items()}

            # --- optional: final energy use
            if hasattr(scenario["iam data"], "final_energy_use"):
                fe = scenario["iam data"].final_energy_use.interp(year=scenario["year"])
                fe = _prefix_vars(fe, "FE")
                fe_units = {f"FE - {k}": v for k, v in fe.attrs.get("unit", {}).items()}

                # concat along variables
                pv = xr.concat([pv, fe], dim="variables")
                # merge unit dicts
                units.update(fe_units)

            scenario_name = f"{scenario['model']} - {scenario['pathway']}"

            # --- optional: external data blocks
            if "external data" in scenario:
                for ext_key, external in scenario["external data"].items():
                    ext = external["production volume"].interp(year=scenario["year"])
                    # prefix includes the external block key so different externals don't collide
                    ext_prefix = f"EXT - {ext_key}"
                    ext = _prefix_vars(ext, ext_prefix)
                    ext_units = {f"{ext_prefix} - {k}": v
                                 for k, v in external["production volume"].attrs.get("unit", {}).items()}

                    pv = xr.concat([pv, ext], dim="variables")
                    units.update(ext_units)
                    extra_units.update(ext_units)
                    scenario_name += f" - {scenario['external scenarios'][ext_key]['scenario']}"

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

    def build_datapackage(self, name: str, contributors: list = None):
        """
        Create and export a scenario datapackage.
        """
        # create a new datapackage
        package = Package(base_path=Path.cwd().as_posix())
        package.infer("pathways_temp/**/*.csv")
        package.infer("pathways_temp/**/*.yaml")

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
