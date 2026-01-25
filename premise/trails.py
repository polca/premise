"""
This module contains the PathwaysDataPackage class, which is
used to create a data package for scenario analysis.
"""

import json
import csv
import shutil
import math
from datetime import date
from pathlib import Path
from typing import List, Dict, Tuple, Set

import yaml
import pandas as pd
from datapackage import Package
import bw2data

from . import __version__
from .new_database import NewDatabase
from .inventory_imports import get_classifications
from .filesystem_constants import DATA_DIR
from .utils import load_database, dump_database

FILEPATH_TEMPORAL_PARAMETERS = (
    DATA_DIR / "trails" / "classifications_temporal_params_copy.xlsx"
)


class KeyLoader(yaml.SafeLoader):
    pass


def key_constructor(loader, node):
    # node is a YAML sequence; construct as tuple (hashable)
    seq = loader.construct_sequence(node)
    return tuple(seq)


KeyLoader.add_constructor("!key", key_constructor)


def _mean_age_from_params(dist_type, loc, scale, mn, mx, lifetime):
    """
    Compute mean age (mu) from existing temporal distribution parameters.
    Falls back to lifetime/2 if not computable.
    """
    try:
        t = int(dist_type) if dist_type is not None else None
    except Exception:
        t = None

    # Type 2: lognormal on AGE
    if t == 2:
        if loc is None or scale is None or scale <= 0:
            return lifetime / 2.0 if lifetime else None
        try:
            return float(math.exp(float(loc) + 0.5 * float(scale) ** 2))
        except Exception:
            return lifetime / 2.0 if lifetime else None

    # Vintage-based types: compute E[vintage] then mu = -E[vintage]
    if t == 4:
        if mn is None or mx is None:
            return lifetime / 2.0 if lifetime else None
        Ev = (float(mn) + float(mx)) / 2.0
        return -Ev

    if t == 5:
        if mn is None or mx is None or loc is None:
            return lifetime / 2.0 if lifetime else None
        Ev = (float(mn) + float(mx) + float(loc)) / 3.0
        return -Ev

    if t == 3:
        # Approximate truncated normal mean with loc
        if loc is None:
            return lifetime / 2.0 if lifetime else None
        return -float(loc)

    # Unknown / missing type: fallback
    return lifetime / 2.0 if lifetime else None


class TrailsDataPackage:
    def __init__(
        self,
        scenario: dict,
        years: List[int] = range(2005, 2105, 10),
        source_version: str = "3.12",
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
        assert "model" in scenario, "Missing `model` key in `scenario`."
        assert "pathway" in scenario, "Missing `pathway` key in `scenario`."
        assert "year" not in scenario, "Key `year` not needed in `scenario`."

        self.years = years

        # build self.scenarios, a list of dictionaries (scenario)
        # each dictionary has a `year` field, from `years`

        self.scenarios = []
        for year in years:
            sc = scenario.copy()
            sc["year"] = year
            self.scenarios.append(sc)

        self.source_db = source_db
        self.source_version = source_version
        self.key = key

        # check biosphere database name
        if biosphere_name not in bw2data.databases:
            raise ValueError(
                f"Wrong biosphere name: {biosphere_name}. "
                f"Should be one of {bw2data.databases}"
            )

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

        (
            self.stock_asset_params,
            self.service_operation_lifetimes,
            self.end_of_life_suppliers,
            self.biomass_growth_params,
        ) = (
            self._load_temporal_specs_from_excel(FILEPATH_TEMPORAL_PARAMETERS)
        )

    def create_datapackage(
        self,
        name: str = f"trails_{date.today()}",
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

    def _load_temporal_specs_from_excel(
        self, path: Path
    ) -> tuple[
        Dict[Tuple[str, str], dict],
        Dict[Tuple[str, str], dict],
        Set[Tuple[str, str]],
        Dict[Tuple[str, str], dict],
    ]:
        """
        Returns:
          stock_assets: dict[(name, ref)] -> exchange-level temporal params for stock_asset suppliers
          service_ops:  dict[(name, ref)] -> lifetime/mean age/dist params for service_operation datasets
          end_of_life:  set[(name, ref)] of end_of_life supplier datasets
          biomass_growth: dict[(name, ref)] -> temporal params for CO2 uptake in biomass growth datasets
        """
        import pandas as pd

        df = pd.read_excel(path)

        required = {"name", "reference product", "temporal_tag"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Temporal params Excel file missing columns: {sorted(missing)}"
            )

        def _clean(x):
            if x is None:
                return None
            try:
                if pd.isna(x):
                    return None
            except Exception:
                pass
            if isinstance(x, str):
                s = x.strip()
                return s if s else None
            return x

        def _num(x):
            x = _clean(x)
            if x is None:
                return None
            try:
                return float(x)
            except Exception:
                return None

        stock_assets: Dict[Tuple[str, str], dict] = {}
        service_ops: Dict[Tuple[str, str], dict] = {}
        end_of_life: Set[Tuple[str, str]] = set()
        biomass_growth: Dict[Tuple[str, str], dict] = {}

        for _, row in df.iterrows():
            tag = _clean(row.get("temporal_tag"))
            name = _clean(row.get("name"))
            ref = _clean(row.get("reference product"))
            if not name or not ref or not tag:
                continue

            if tag == "stock_asset":
                dist_type = _clean(row.get("age distribution type"))
                if dist_type is not None:
                    try:
                        dist_type = int(float(dist_type))
                    except Exception:
                        dist_type = None

                if dist_type is None:
                    continue  # cannot apply without a type

                stock_assets[(name, ref)] = {
                    "temporal_distribution": dist_type,
                    "temporal_loc": _num(row.get("loc")),
                    "temporal_scale": _num(row.get("scale")),
                    "temporal_min": _num(row.get("minimum")),
                    "temporal_max": _num(row.get("maximum")),
                }

            elif tag == "service_operation":

                L = _num(row.get("lifetime"))

                if L is None or L <= 0:
                    continue

                dist_type = _clean(row.get("age distribution type"))
                if dist_type is not None:

                    try:
                        dist_type = int(float(dist_type))
                    except Exception:
                        dist_type = None

                loc = _num(row.get("loc"))
                scale = _num(row.get("scale"))
                mn = _num(row.get("minimum"))
                mx = _num(row.get("maximum"))
                mu = _mean_age_from_params(dist_type, loc, scale, mn, mx, float(L))
                # Clip mu to [0, L] to avoid pathological kernels
                mu = min(max(0.0, float(mu)), float(L))
                service_ops[(name, ref)] = {
                    "lifetime": float(L),
                    "mean_age": float(mu),
                    "dist_type": dist_type,
                    "loc": loc,
                    "scale": scale,
                    "minimum": mn,
                    "maximum": mx,
                }

            elif tag == "end_of_life":
                end_of_life.add((name, ref))

            elif tag == "biomass_growth":
                dist_type = _clean(row.get("age distribution type"))
                if dist_type is not None:
                    try:
                        dist_type = int(float(dist_type))
                    except Exception:
                        dist_type = None

                biomass_growth[(name, ref)] = {
                    "temporal_distribution": dist_type,
                    "temporal_loc": _num(row.get("loc")),
                    "temporal_scale": _num(row.get("scale")),
                    "temporal_min": _num(row.get("minimum")),
                    "temporal_max": _num(row.get("maximum")),
                    "lifetime": _num(row.get("lifetime")),
                }

        return stock_assets, service_ops, end_of_life, biomass_growth

    def _export_datapackage(
        self,
        name: str,
        contributors: list = None,
    ):

        # first, delete the content of the "trails_temp" folder
        shutil.rmtree(Path.cwd() / "trails_temp", ignore_errors=True)

        self.add_temporal_distributions()

        # create matrices in current directory
        self.datapackage.write_db_to_matrices(
            filepath=str(Path.cwd() / "trails_temp" / "inventories"),
        )
        self.variables_name_change = {}
        self._add_classifications_file()
        self._build_datapackage(name, contributors)

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

        outdir = Path.cwd() / "trails_temp" / "classifications"
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
        csv_files = list((Path.cwd() / "trails_temp").glob("**/*.csv"))

        for file in csv_files:
            relpath = file.relative_to(Path.cwd()).as_posix()
            package.add_resource(
                {
                    "path": relpath,
                    "profile": "tabular-data-resource",
                    "encoding": "utf-8",
                    "dialect": {"delimiter": ";"},
                }
            )

        package.infer("trails_temp/**/*.yaml")
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
            "trails",
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
        package.save(str(Path.cwd() / "trails_temp" / "datapackage.json"))

        # open the json file and ensure that all resource names are slugified
        with open(Path.cwd() / "trails_temp" / "datapackage.json", "r") as f:
            data = yaml.full_load(f)

        for resource in data["resources"]:
            resource["name"] = resource["name"].replace(" ", "_").lower()

        # also, remove "trails/" from the path of each resource
        for resource in data["resources"]:
            path = resource["path"]
            path = path.replace("trails_temp", "trails")
            path = path.replace("trails/", "").replace("trails\\", "")
            path = path.replace("\\", "/")
            resource["path"] = path

        # save it back as a json file
        with open(Path.cwd() / "trails_temp" / "datapackage.json", "w") as fp:
            json.dump(data, fp)

        # reorder matrices and indices
        self.reorder_matrices()

        # zip the folder
        shutil.make_archive(name, "zip", str(Path.cwd() / "trails_temp"))

        print(f"Trails data package saved at {str(Path.cwd() / f'{name}.zip')}")

    def reorder_matrices(self) -> None:
        """
        Harmonize matrix index spaces across all year-folders in trails_temp/inventories.

        Rules implemented:
          - Existing rows in A_matrix.csv and B_matrix.csv are preserved verbatim
            EXCEPT for the integer index columns which are remapped to a global ordering.
          - A_matrix_index.csv and B_matrix_index.csv are rewritten to the global ordering.
          - For activities missing in a given year, append one diagonal placeholder row to A:
              value = 1
              flip  = 0
            (Other columns in this new row use deterministic/neutral defaults.)
        """

        print("Reordering matrices to global index spaces...")
        base = Path.cwd() / "trails_temp" / "inventories"
        slice_dirs = self._find_slice_dirs(base)

        if not slice_dirs:
            raise FileNotFoundError(
                f"No inventory folders found under {base} containing the required files."
            )

        # 1) Build global indices (union across all slices)
        global_A_keys, global_A_index = self._build_global_A_index(slice_dirs)
        global_B_keys, global_B_index = self._build_global_B_index(slice_dirs)

        # 2) Apply to each slice
        for d in slice_dirs:
            a_idx_path = d / "A_matrix_index.csv"
            b_idx_path = d / "B_matrix_index.csv"
            a_mat_path = d / "A_matrix.csv"
            b_mat_path = d / "B_matrix.csv"

            # Build local->global maps
            local_A = self._read_A_index(a_idx_path)  # key -> local int
            local_B = self._read_B_index(b_idx_path)  # key -> local int

            map_A = {local_i: global_A_index[k] for k, local_i in local_A.items()}
            map_B = {local_i: global_B_index[k] for k, local_i in local_B.items()}

            # Rewrite index CSVs to global ordering
            self._write_A_index_global(a_idx_path, global_A_keys)
            self._write_B_index_global(b_idx_path, global_B_keys)

            # Rewrite matrices (only index columns changed)
            present_global_acts = self._rewrite_A_matrix_indices_only(a_mat_path, map_A)
            self._rewrite_B_matrix_indices_only(b_mat_path, map_A, map_B)

            # Append placeholder diagonal rows for missing global activities
            self._append_placeholders_to_A(
                a_mat_path=a_mat_path,
                global_n=len(global_A_keys),
                present_global_acts=present_global_acts,
            )

    # -----------------------------
    # Discovery
    # -----------------------------

    def _find_slice_dirs(self, base: Path) -> List[Path]:
        required = {
            "A_matrix.csv",
            "A_matrix_index.csv",
            "B_matrix.csv",
            "B_matrix_index.csv",
        }
        out: List[Path] = []
        if not base.exists():
            return out

        # Find directories that contain all required files
        for d in base.rglob("*"):
            if not d.is_dir():
                continue
            names = {p.name for p in d.iterdir() if p.is_file()}
            if required.issubset(names):
                out.append(d)

        # Stable traversal (reproducible)
        out.sort(key=lambda p: p.as_posix())
        return out

    # -----------------------------
    # Index parsing / writing
    # -----------------------------

    AKey = Tuple[str, str, str, str]  # (name, ref product, unit, location)
    BKey = Tuple[str, str, str, str]  # (name, compartment, subcompartment, unit)

    def _read_A_index(self, path: Path) -> Dict[AKey, int]:
        out: Dict[TrailsDataPackage.AKey, int] = {}
        with path.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f, delimiter=";")
            for row in r:
                key = (
                    (row.get("name") or "").strip(),
                    (row.get("reference product") or "").strip(),
                    (row.get("unit") or "").strip(),
                    (row.get("location") or "").strip(),
                )
                idx = int(row["index"])
                out[key] = idx
        return out

    def _read_B_index(self, path: Path) -> Dict[BKey, int]:
        out: Dict[TrailsDataPackage.BKey, int] = {}
        with path.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f, delimiter=";")
            for row in r:
                key = (
                    (row.get("name") or "").strip(),
                    (row.get("compartment") or "").strip(),
                    (row.get("subcompartment") or "").strip(),
                    (row.get("unit") or "").strip(),
                )
                idx = int(row["index"])
                out[key] = idx
        return out

    def _build_global_A_index(
        self, slice_dirs: List[Path]
    ) -> Tuple[List[AKey], Dict[AKey, int]]:
        # Baseline ordering from first slice, then append novel keys sorted
        first = self._read_A_index(slice_dirs[0] / "A_matrix_index.csv")
        ordered: List[TrailsDataPackage.AKey] = sorted(
            first.keys(), key=lambda k: first[k]
        )

        seen: Set[TrailsDataPackage.AKey] = set(ordered)
        new_keys: Set[TrailsDataPackage.AKey] = set()

        for d in slice_dirs[1:]:
            idx = self._read_A_index(d / "A_matrix_index.csv")
            for k in idx.keys():
                if k not in seen:
                    new_keys.add(k)

        for k in sorted(new_keys):
            ordered.append(k)

        global_map = {k: i for i, k in enumerate(ordered)}
        return ordered, global_map

    def _build_global_B_index(
        self, slice_dirs: List[Path]
    ) -> Tuple[List[BKey], Dict[BKey, int]]:
        first = self._read_B_index(slice_dirs[0] / "B_matrix_index.csv")
        ordered: List[TrailsDataPackage.BKey] = sorted(
            first.keys(), key=lambda k: first[k]
        )

        seen: Set[TrailsDataPackage.BKey] = set(ordered)
        new_keys: Set[TrailsDataPackage.BKey] = set()

        for d in slice_dirs[1:]:
            idx = self._read_B_index(d / "B_matrix_index.csv")
            for k in idx.keys():
                if k not in seen:
                    new_keys.add(k)

        for k in sorted(new_keys):
            ordered.append(k)

        global_map = {k: i for i, k in enumerate(ordered)}
        return ordered, global_map

    def _write_A_index_global(self, path: Path, global_keys: List[AKey]) -> None:
        tmp = path.with_suffix(".csv.tmp")
        fieldnames = ["name", "reference product", "unit", "location", "index"]
        with tmp.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            w.writeheader()
            for i, (name, ref, unit, loc) in enumerate(global_keys):
                w.writerow(
                    {
                        "name": name,
                        "reference product": ref,
                        "unit": unit,
                        "location": loc,
                        "index": i,
                    }
                )
        tmp.replace(path)

    def _write_B_index_global(self, path: Path, global_keys: List[BKey]) -> None:
        tmp = path.with_suffix(".csv.tmp")
        fieldnames = ["name", "compartment", "subcompartment", "unit", "index"]
        with tmp.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            w.writeheader()
            for i, (name, comp, subcomp, unit) in enumerate(global_keys):
                w.writerow(
                    {
                        "name": name,
                        "compartment": comp,
                        "subcompartment": subcomp,
                        "unit": unit,
                        "index": i,
                    }
                )
        tmp.replace(path)

    # -----------------------------
    # Matrix rewriting (indices only)
    # -----------------------------

    def _rewrite_A_matrix_indices_only(
        self, path: Path, map_A: Dict[int, int]
    ) -> Set[int]:
        """
        Rewrite A_matrix.csv, changing only:
          - index of activity
          - index of product

        Returns the set of global activity indices present as row indices after rewrite.
        """
        tmp = path.with_suffix(".csv.tmp")
        present: Set[int] = set()

        with (
            path.open("r", encoding="utf-8", newline="") as fin,
            tmp.open("w", encoding="utf-8", newline="") as fout,
        ):
            r = csv.DictReader(fin, delimiter=";")
            fieldnames = r.fieldnames or []
            if (
                "index of activity" not in fieldnames
                or "index of product" not in fieldnames
            ):
                raise ValueError(f"{path} missing required A matrix columns.")

            w = csv.DictWriter(fout, fieldnames=fieldnames, delimiter=";")
            w.writeheader()

            for row in r:
                old_i = int(row["index of activity"])
                old_j = int(row["index of product"])
                new_i = map_A[old_i]
                new_j = map_A[old_j]
                row["index of activity"] = str(new_i)
                row["index of product"] = str(new_j)
                w.writerow(row)
                present.add(new_i)

        tmp.replace(path)
        return present

    def _rewrite_B_matrix_indices_only(
        self, path: Path, map_A: Dict[int, int], map_B: Dict[int, int]
    ) -> None:
        """
        Rewrite B_matrix.csv, changing only:
          - index of activity (using map_A)
          - index of biosphere flow (using map_B)
        """
        tmp = path.with_suffix(".csv.tmp")

        with (
            path.open("r", encoding="utf-8", newline="") as fin,
            tmp.open("w", encoding="utf-8", newline="") as fout,
        ):
            r = csv.DictReader(fin, delimiter=";")
            fieldnames = r.fieldnames or []
            if (
                "index of activity" not in fieldnames
                or "index of biosphere flow" not in fieldnames
            ):
                raise ValueError(f"{path} missing required B matrix columns.")

            w = csv.DictWriter(fout, fieldnames=fieldnames, delimiter=";")
            w.writeheader()

            for row in r:
                old_i = int(row["index of activity"])
                old_k = int(row["index of biosphere flow"])
                row["index of activity"] = str(map_A[old_i])
                row["index of biosphere flow"] = str(map_B[old_k])
                w.writerow(row)

        tmp.replace(path)

    # -----------------------------
    # Placeholders
    # -----------------------------

    def _append_placeholders_to_A(
        self,
        a_mat_path: Path,
        global_n: int,
        present_global_acts: Set[int],
    ) -> None:
        """
        Append diagonal placeholder rows to A_matrix.csv for missing activities.

        Requirement from user:
          - value = 1
          - flip  = 0

        We keep the file schema and set other fields to neutral defaults for new rows.
        """
        missing = [i for i in range(global_n) if i not in present_global_acts]
        if not missing:
            return

        # We must use the same columns as the file already has.
        with a_mat_path.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f, delimiter=";")
            fieldnames = r.fieldnames or []
            # Sanity: required columns must exist
            for req in ("index of activity", "index of product", "value", "flip"):
                if req not in fieldnames:
                    raise ValueError(
                        f"{a_mat_path} missing required column '{req}' needed for placeholders."
                    )

        # Append rows
        with a_mat_path.open("a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")

            for g in missing:
                row = {k: "" for k in fieldnames}

                row["index of activity"] = str(g)
                row["index of product"] = str(g)
                row["value"] = "1"
                row["flip"] = "0"

                # Neutral defaults for typical uncertainty schema (only for new placeholder rows)
                if "uncertainty type" in row:
                    row["uncertainty type"] = "0"
                if "loc" in row:
                    row["loc"] = "1"
                if "negative" in row:
                    row["negative"] = "0"

                w.writerow(row)

    def add_temporal_distributions(self):
        """
        1) For technosphere exchanges that draw from a stock_asset supplier, inject the supplier-specific temporal params.
        2) If the *dataset itself* is tagged service_operation, then inject a default uniform temporal distribution
           (type 4) over [-lifetime, -1] onto all technosphere + biosphere exchanges that do not already have one.
        3) For technosphere exchanges that draw from an end_of_life supplier, shift the calling dataset's
           age distribution by its average age (or lifetime fallback) and apply it to the exchange.
        """
        stock_assets = getattr(self, "stock_asset_params", {})  # (name, ref) -> params
        service_ops = getattr(
            self, "service_operation_lifetimes", {}
        )  # (name, ref) -> service operation params
        end_of_life = getattr(self, "end_of_life_suppliers", set())
        biomass_growth = getattr(self, "biomass_growth_params", {})

        def _num(x):
            if x is None:
                return None
            try:
                return float(x)
            except Exception:
                return None


        for s, scenario in enumerate(self.datapackage.scenarios):
            scenario = load_database(scenario, self.datapackage.database)
            db = scenario["database"]
            product_lookup: Dict[Tuple[str, str], Set[str]] = {}
            for ds in db:
                key = (
                    (ds.get("name") or "").strip(),
                    (ds.get("location") or "").strip(),
                )
                ref = (ds.get("reference product") or "").strip()
                if not key[0] or not key[1] or not ref:
                    continue
                product_lookup.setdefault(key, set()).add(ref)

            def _exchange_product(exc):
                ref = (exc.get("product") or exc.get("reference product") or "").strip()
                if ref:
                    return ref
                name = (exc.get("name") or "").strip()
                loc = (exc.get("location") or "").strip()
                candidates = product_lookup.get((name, loc)) or set()
                if len(candidates) == 1:
                    ref = next(iter(candidates))
                    exc["product"] = ref
                    return ref
                return ""

            for ds in db:
                ds_name = (ds.get("name") or "").strip()
                ds_ref = (ds.get("reference product") or "").strip()

                # ---- (0) biomass_growth: apply temporal params to CO2 uptake in biosphere
                bg = biomass_growth.get((ds_name, ds_ref))
                if bg is not None:
                    dist_type = bg.get("temporal_distribution")
                    for e in ds.get("exchanges", []):
                        if e.get("type") != "biosphere":
                            continue
                        if (e.get("name") or "").strip() != "Carbon dioxide, in air":
                            continue
                        if e.get("temporal_distribution") is not None:
                            continue

                        if dist_type is not None:
                            e["temporal_distribution"] = dist_type
                            loc = bg.get("temporal_loc")
                            e["temporal_scale"] = bg.get("temporal_scale")
                            mn = bg.get("temporal_min")
                            mx = bg.get("temporal_max")
                            # CO2 uptake occurs in the past: ensure negative-time support
                            if mn is not None and mx is not None and mn >= 0 and mx >= 0:
                                mn, mx = -mx, -mn
                                if loc is not None:
                                    loc = -loc
                            e["temporal_loc"] = loc
                            e["temporal_min"] = mn
                            e["temporal_max"] = mx
                        else:
                            L = bg.get("lifetime")
                            if L is None or L <= 0:
                                continue
                            e["temporal_distribution"] = 4  # uniform kernel
                            e["temporal_loc"] = None
                            e["temporal_scale"] = None
                            e["temporal_min"] = -float(L)
                            e["temporal_max"] = 0.0

                # ---- (A) service_operation: dataset-level default on all exchanges (unless already present)
                spec = service_ops.get((ds_name, ds_ref))
                if spec is not None:
                    L = float(spec["lifetime"])
                    mu = float(spec["mean_age"])

                    # Option C kernel support
                    mn = -mu
                    mx = L - mu

                    for e in ds.get("exchanges", []):
                        if e.get("type") not in ("technosphere", "biosphere"):
                            continue
                        if e.get("temporal_distribution") is not None:
                            continue

                        e["temporal_distribution"] = 4  # uniform kernel
                        e["temporal_loc"] = None
                        e["temporal_scale"] = None
                        e["temporal_min"] = mn
                        e["temporal_max"] = mx

                # ---- (B) stock_asset suppliers: exchange-level override for technosphere (even if service_operation ran)
                # This is done after (A) so that stock_asset-specific params can overwrite the default uniform, if desired.
                for e in ds.get("exchanges", []):
                    if e.get("type") != "technosphere":
                        continue

                    sup_name = (e.get("name") or "").strip()
                    sup_ref = _exchange_product(e)
                    key = (sup_name, sup_ref)

                    params = stock_assets.get(key)
                    if not params:
                        continue

                    e["temporal_distribution"] = params["temporal_distribution"]
                    e["temporal_loc"] = params.get("temporal_loc")
                    e["temporal_scale"] = params.get("temporal_scale")
                    e["temporal_min"] = params.get("temporal_min")
                    e["temporal_max"] = params.get("temporal_max")

                # ---- (C) end_of_life suppliers: shift calling dataset distribution by its average age
                ds_mean_age = None
                ds_lifetime = None
                ds_dist_type = None
                ds_loc = None
                ds_scale = None
                ds_min = None
                ds_max = None

                if spec is not None:
                    ds_mean_age = _num(spec.get("mean_age"))
                    ds_lifetime = _num(spec.get("lifetime"))
                    ds_dist_type = spec.get("dist_type")
                    ds_loc = _num(spec.get("loc"))
                    ds_scale = _num(spec.get("scale"))
                    ds_min = _num(spec.get("minimum"))
                    ds_max = _num(spec.get("maximum"))

                # Fallback to dataset-provided fields, if any
                if ds_mean_age is None:
                    ds_mean_age = _num(
                        ds.get("mean_age")
                        or ds.get("average age")
                        or ds.get("average_age")
                    )
                if ds_lifetime is None:
                    ds_lifetime = _num(ds.get("lifetime"))

                # If not a service_operation dataset, use stock_asset dist params when available
                if ds_dist_type is None:
                    ds_stock = stock_assets.get((ds_name, ds_ref))
                    if ds_stock:
                        ds_dist_type = ds_stock.get("temporal_distribution")
                        ds_loc = _num(ds_stock.get("temporal_loc"))
                        ds_scale = _num(ds_stock.get("temporal_scale"))
                        ds_min = _num(ds_stock.get("temporal_min"))
                        ds_max = _num(ds_stock.get("temporal_max"))

                if ds_mean_age is None and ds_dist_type is not None:
                    mu = _mean_age_from_params(
                        ds_dist_type,
                        ds_loc,
                        ds_scale,
                        ds_min,
                        ds_max,
                        ds_lifetime,
                    )
                    if mu is not None:
                        ds_mean_age = float(mu)

                if ds_mean_age is None and ds_lifetime is not None:
                    ds_mean_age = ds_lifetime / 2.0

                for e in ds.get("exchanges", []):
                    if e.get("type") != "technosphere":
                        continue

                    sup_name = (e.get("name") or "").strip()
                    sup_ref = _exchange_product(e)
                    if (sup_name, sup_ref) not in end_of_life:
                        continue

                    if ds_mean_age is None and ds_lifetime is None:
                        continue

                    # Use the calling dataset's distribution params if available; otherwise fallback to uniform.
                    if ds_dist_type is not None:
                        shift = 0.0
                        if (
                            ds_lifetime is not None
                            and ds_mean_age is not None
                            and ds_loc is not None
                        ):
                            shift = (ds_lifetime - ds_mean_age) - ds_loc
                        elif ds_mean_age is not None and ds_loc is not None:
                            shift = ds_mean_age - ds_loc
                        e["temporal_distribution"] = ds_dist_type
                        e["temporal_loc"] = None if ds_loc is None else ds_loc + shift
                        e["temporal_scale"] = ds_scale
                        e["temporal_min"] = None if ds_min is None else ds_min + shift
                        e["temporal_max"] = None if ds_max is None else ds_max + shift
                    else:
                        e["temporal_distribution"] = 4  # uniform kernel
                        e["temporal_loc"] = None
                        e["temporal_scale"] = None
                        if ds_lifetime is not None:
                            e["temporal_min"] = 0.0
                            e["temporal_max"] = float(ds_lifetime)
                        else:
                            e["temporal_min"] = float(ds_mean_age)
                            e["temporal_max"] = float(ds_mean_age)

            self.datapackage.scenarios[s] = dump_database(scenario)
