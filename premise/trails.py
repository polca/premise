"""
This module contains the PathwaysDataPackage class, which is
used to create a data package for scenario analysis.
"""

import json
import csv
import shutil
import re
import ast
from datetime import date
from pathlib import Path
from typing import List, Dict, Tuple, Set

import yaml
from datapackage import Package
import bw2data

from . import __version__
from .new_database import NewDatabase
from .inventory_imports import get_classifications
from .filesystem_constants import DATA_DIR
from .utils import load_database, dump_database

FILEPATH_TEMPORAL_PARAMETERS = DATA_DIR / "trails" / "temporal_distributions.csv"


class KeyLoader(yaml.SafeLoader):
    pass


def key_constructor(loader, node):
    # node is a YAML sequence; construct as tuple (hashable)
    seq = loader.construct_sequence(node)
    return tuple(seq)


KeyLoader.add_constructor("!key", key_constructor)


class TrailsDataPackage:
    def __init__(
        self,
        scenario: dict,
        years: List[int] = list(range(2005, 2100, 10)) + [2100],
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
            self.end_of_life_suppliers,
            self.biomass_growth_params,
            self.maintenance_suppliers,
        ) = self._load_temporal_specs_from_csv(FILEPATH_TEMPORAL_PARAMETERS)

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

    def _load_temporal_specs_from_csv(self, path: Path) -> tuple[
        Dict[Tuple[str, str], dict],
        Set[Tuple[str, str]],
        Dict[Tuple[str, str], dict],
        Set[Tuple[str, str]],
    ]:
        """
        Returns:
          stock_assets: dict[(name, ref)] -> exchange-level temporal params for stock_asset suppliers
          end_of_life:  set[(name, ref)] of end_of_life supplier datasets
          biomass_growth: dict[(name, ref)] -> temporal params for CO2 uptake in biomass growth datasets
          maintenance: set[(name, ref)] of maintenance supplier datasets
        """
        with open(path, "r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))

        required = {"name", "reference product", "temporal_tag"}
        missing = required - set(rows[0].keys() if rows else [])
        if missing:
            raise ValueError(
                f"Temporal params CSV file missing columns: {sorted(missing)}"
            )

        def _clean(x):
            if x is None:
                return None
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

        def _num_list(x):
            x = _clean(x)
            if x is None:
                return None
            if isinstance(x, (list, tuple)):
                try:
                    return [float(v) for v in x]
                except Exception:
                    return None

            s = str(x).strip()
            vals = None
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = ast.literal_eval(s)
                    if isinstance(parsed, (list, tuple)):
                        vals = [float(v) for v in parsed]
                except Exception:
                    vals = None
            if vals is None:
                parts = re.split(r"[|;,]", s)
                if len(parts) == 1:
                    parts = s.split()
                vals = []
                for p in parts:
                    p = p.strip()
                    if not p:
                        continue
                    try:
                        vals.append(float(p))
                    except Exception:
                        return None
            return vals if vals else None

        stock_assets: Dict[Tuple[str, str], dict] = {}
        end_of_life: Set[Tuple[str, str]] = set()
        biomass_growth: Dict[Tuple[str, str], dict] = {}
        maintenance: Set[Tuple[str, str]] = set()

        for row in rows:
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
                    "temporal_offsets": _num_list(row.get("offsets")),
                    "temporal_weights": _num_list(row.get("weights")),
                    "temporal_min": _num(row.get("minimum")),
                    "temporal_max": _num(row.get("maximum")),
                    "lifetime": _num(row.get("lifetime")),
                    "mean_age": _num(
                        row.get("mean_age")
                        or row.get("average_age")
                        or row.get("average age")
                    ),
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
                    "temporal_offsets": _num_list(row.get("offsets")),
                    "temporal_weights": _num_list(row.get("weights")),
                    "temporal_min": _num(row.get("minimum")),
                    "temporal_max": _num(row.get("maximum")),
                    "lifetime": _num(row.get("lifetime")),
                }

            elif tag == "maintenance":
                maintenance.add((name, ref))

        return stock_assets, end_of_life, biomass_growth, maintenance

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
        missing_classifications_seen = set()
        inferred_classifications = {}

        def _norm(x: str) -> str:
            return " ".join((x or "").strip().lower().split())

        def _canon_name(x: str) -> str:
            s = _norm(x)
            patterns = [
                r",\s*with carbon capture and storage\b",
                r",\s*with carbon capture and reuse\b",
                r",\s*economic allocation\b",
                r",\s*energy allocation\b",
                r",\s*system expansion\b",
                r",\s*at fuelling station\b",
                r",\s*no biogenic carbon impacts\b",
            ]
            for pat in patterns:
                s = re.sub(pat, "", s)
            return " ".join(s.split())

        # Build lookup indices from existing classifications
        by_name = {}
        by_product = {}
        by_canon_name = {}
        for (n, p), cls in self.classifications.items():
            isic = cls.get("ISIC rev.4 ecoinvent")
            cpc = cls.get("CPC")
            if not isic or not cpc:
                continue
            by_name.setdefault(_norm(n), set()).add((isic, cpc))
            by_product.setdefault(_norm(p), set()).add((isic, cpc))
            by_canon_name.setdefault(_canon_name(n), set()).add((isic, cpc))

        # Majority class for heat outputs in "burned in passenger car" datasets
        burned_heat_counts = {}
        for (n, p), cls in self.classifications.items():
            if (
                "burned in passenger car" in _norm(n)
                and _norm(p) == "heat"
                and cls.get("ISIC rev.4 ecoinvent")
                and cls.get("CPC")
            ):
                key = (cls["ISIC rev.4 ecoinvent"], cls["CPC"])
                burned_heat_counts[key] = burned_heat_counts.get(key, 0) + 1
        burned_heat_default = None
        if burned_heat_counts:
            burned_heat_default = max(burned_heat_counts.items(), key=lambda x: x[1])[0]

        def _infer_classification(name: str, ref: str):
            n = _norm(name)
            p = _norm(ref)
            cn = _canon_name(name)

            # Exact-name unique class
            name_classes = by_name.get(n, set())
            if len(name_classes) == 1:
                isic, cpc = next(iter(name_classes))
                return isic, cpc

            # Exact-product unique class
            prod_classes = by_product.get(p, set())
            if len(prod_classes) == 1:
                isic, cpc = next(iter(prod_classes))
                return isic, cpc

            # Canonical-name unique class
            canon_classes = by_canon_name.get(cn, set())
            if len(canon_classes) == 1:
                isic, cpc = next(iter(canon_classes))
                return isic, cpc

            # Heuristic: petrol markets
            if n.startswith("market for petrol"):
                base = self.classifications.get(("market for petrol", "petrol"))
                if base and base.get("ISIC rev.4 ecoinvent") and base.get("CPC"):
                    return base["ISIC rev.4 ecoinvent"], base["CPC"]

            # Heuristic: passenger car combustion heat datasets
            if "burned in passenger car" in n and p == "heat" and burned_heat_default:
                return burned_heat_default

            # Heuristics: wood processing / sawmilling families
            if n.startswith("market for sawlog and veneer log"):
                return (
                    "0220:Logging",
                    "3110: Wood, sawn or chipped lengthwise, sliced or peeled, of a thickness exceeding 6 mm; railway or tramway sleepers […]",
                )

            if (
                n.startswith("sawnwood production")
                or n.startswith("planing, ")
                or n.startswith("market for sawnwood")
                or n.startswith("beam, ")
                or n.startswith("board, ")
                or n.startswith("lath, ")
            ):
                if "shavings" in p or "wood chips" in p:
                    return (
                        "1610:Sawmilling and planing of wood",
                        "31230: Wood in chips or particles",
                    )
                return (
                    "1610:Sawmilling and planing of wood",
                    "3110: Wood, sawn or chipped lengthwise, sliced or peeled, of a thickness exceeding 6 mm; railway or tramway sleepers […]",
                )

            # Heuristics: ethanol / esterification families
            if "ethanol production" in n and "ethanol" in p:
                return (
                    "2011:Manufacture of basic chemicals",
                    "35491: Biodiesel",
                )
            if "ethanol production" in n and p == "electricity, high voltage":
                return (
                    "3510:Electric power generation, transmission and distribution",
                    "17100: Electrical energy",
                )
            if n == "esterification of soybean oil":
                if p == "fatty acid methyl ester":
                    return (
                        "2011:Manufacture of basic chemicals",
                        "35491: Biodiesel",
                    )
                if p == "glycerine":
                    return (
                        "2011:Manufacture of basic chemicals",
                        "34620: Organic compounds with nitrogen function",
                    )

            return None

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
                        key = (name, ref)
                        if key in self.classifications:
                            ds["classifications"] = [
                                (
                                    "ISIC rev.4 ecoinvent",
                                    self.classifications[key]["ISIC rev.4 ecoinvent"],
                                ),
                                (
                                    "CPC",
                                    self.classifications[key]["CPC"],
                                ),
                            ]
                            classifications = ds.get("classifications")
                        else:
                            inferred = _infer_classification(name, ref)
                            if inferred is not None:
                                isic, cpc = inferred
                                ds["classifications"] = [
                                    ("ISIC rev.4 ecoinvent", isic),
                                    ("CPC", cpc),
                                ]
                                self.classifications[key] = {
                                    "ISIC rev.4 ecoinvent": isic,
                                    "CPC": cpc,
                                }
                                inferred_classifications[key] = (isic, cpc)
                                classifications = ds.get("classifications")
                            elif key not in missing_classifications_seen:
                                print(f"No classifications for {name} ({ref})")
                                missing_classifications_seen.add(key)

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

        # Persist high-confidence inferred classifications for future runs.
        if inferred_classifications:
            cls_path = DATA_DIR / "utils" / "import" / "classifications.csv"
            file_keys = set()
            with open(cls_path, "r", newline="", encoding="utf-8-sig") as f:
                r = csv.DictReader(f)
                for row in r:
                    file_keys.add(
                        (row.get("name", "").strip(), row.get("product", "").strip())
                    )
            with open(cls_path, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["name", "product", "ISIC rev.4 ecoinvent", "CPC"],
                )
                for (name, ref), (isic, cpc) in inferred_classifications.items():
                    key = (name.strip(), ref.strip())
                    if key in file_keys:
                        continue
                    w.writerow(
                        {
                            "name": name,
                            "product": ref,
                            "ISIC rev.4 ecoinvent": isic,
                            "CPC": cpc,
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
        Apply temporal distributions in a single pass over exchanges:
        - biomass_growth: apply dataset-level params to CO2 uptake exchange
        - stock_asset: apply supplier-level params directly
        - maintenance: uniform distribution over [0, lifetime] using calling dataset lifetime from CSV
        - end_of_life: one-pulse (type 6) at dataset lifetime from CSV
        """
        stock_assets = getattr(self, "stock_asset_params", {})  # (name, ref) -> params
        end_of_life = getattr(self, "end_of_life_suppliers", set())
        biomass_growth = getattr(self, "biomass_growth_params", {})
        maintenance = getattr(self, "maintenance_suppliers", set())

        for s, scenario in enumerate(self.datapackage.scenarios):
            scenario = load_database(scenario, self.datapackage.database)
            db = scenario["database"]
            validation_errors = []

            for ds in db:
                ds_name = (ds.get("name") or "").strip()
                ds_ref = (ds.get("reference product") or "").strip()
                ds_stock = stock_assets.get((ds_name, ds_ref), {})
                ds_lifetime = ds_stock.get("lifetime")

                bg = biomass_growth.get((ds_name, ds_ref))
                for e in ds.get("exchanges", []):
                    exc_type = e.get("type")

                    if (
                        exc_type == "biosphere"
                        and (e.get("name") or "").strip() == "Carbon dioxide, in air"
                        and bg is not None
                        and bg.get("temporal_distribution") is not None
                    ):
                        e["temporal_distribution"] = bg.get("temporal_distribution")
                        e["temporal_loc"] = bg.get("temporal_loc")
                        e["temporal_scale"] = bg.get("temporal_scale")
                        e["temporal_min"] = bg.get("temporal_min")
                        e["temporal_max"] = bg.get("temporal_max")
                        e["temporal_offsets"] = bg.get("temporal_offsets")
                        e["temporal_weights"] = bg.get("temporal_weights")
                        continue

                    if exc_type != "technosphere":
                        continue

                    sup_name = (e.get("name") or "").strip()
                    sup_ref = (e.get("product") or e.get("reference product") or "").strip()
                    if not sup_ref:
                        validation_errors.append(
                            "Missing supplier product on technosphere exchange "
                            f"in dataset ({ds_name}, {ds_ref}) for exchange '{sup_name}'."
                        )
                        continue
                    key = (sup_name, sup_ref)
                    params = stock_assets.get(key)
                    is_maintenance = key in maintenance
                    is_end_of_life = key in end_of_life

                    matched = int(params is not None) + int(is_maintenance) + int(
                        is_end_of_life
                    )
                    if matched > 1:
                        tags = []
                        if params is not None:
                            tags.append("stock_asset")
                        if is_maintenance:
                            tags.append("maintenance")
                        if is_end_of_life:
                            tags.append("end_of_life")
                        validation_errors.append(
                            "Ambiguous temporal tags for supplier "
                            f"{key}: matched {tags} in dataset "
                            f"({ds_name}, {ds_ref})."
                        )
                        continue
                    if matched == 0:
                        continue

                    if params is not None:
                        e["temporal_distribution"] = params["temporal_distribution"]
                        e["temporal_loc"] = params.get("temporal_loc")
                        e["temporal_scale"] = params.get("temporal_scale")
                        e["temporal_min"] = params.get("temporal_min")
                        e["temporal_max"] = params.get("temporal_max")
                        e["temporal_offsets"] = params.get("temporal_offsets")
                        e["temporal_weights"] = params.get("temporal_weights")
                        continue

                    if (is_maintenance or is_end_of_life) and ds_lifetime is None:
                        validation_errors.append(
                            "Missing dataset lifetime in temporal CSV for "
                            f"({ds_name}, {ds_ref}) required by supplier {key} "
                            f"(tag: {'maintenance' if is_maintenance else 'end_of_life'})."
                        )
                        continue

                    if is_maintenance:
                        e["temporal_distribution"] = 4
                        e["temporal_loc"] = None
                        e["temporal_scale"] = None
                        e["temporal_min"] = 0.0
                        e["temporal_max"] = float(ds_lifetime)
                        e["temporal_offsets"] = None
                        e["temporal_weights"] = None
                        continue

                    if is_end_of_life:
                        pulse_time = float(ds_lifetime)
                        e["temporal_distribution"] = 6
                        e["temporal_loc"] = None
                        e["temporal_scale"] = None
                        e["temporal_min"] = None
                        e["temporal_max"] = None
                        e["temporal_offsets"] = [pulse_time]
                        e["temporal_weights"] = [1.0]

            if validation_errors:
                sample = "\n".join(f"- {msg}" for msg in validation_errors[:25])
                remaining = len(validation_errors) - 25
                if remaining > 0:
                    sample += f"\n- ... and {remaining} more"
                raise ValueError(
                    "Temporal distribution validation failed:\n"
                    f"{sample}"
                )

            self.datapackage.scenarios[s] = dump_database(scenario)
