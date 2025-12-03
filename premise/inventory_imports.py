"""
Contains class and methods to imports inventories from ecoinvent, premise,
and those provided by the user.
"""

import csv
import itertools
import logging
import uuid
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Union
import json
from collections import deque

import bw2io
import numpy as np
import pandas as pd
import requests
import yaml
from bw2io import CSVImporter, ExcelImporter, Migration
from prettytable import PrettyTable
from wurst import searching as ws

from .clean_datasets import remove_categories, remove_uncertainty
from .data_collection import get_delimiter
from .filesystem_constants import DATA_DIR, DIR_CACHED_DB, INVENTORY_DIR
from .geomap import Geomap

FILEPATH_CONSEQUENTIAL_BLACKLIST = DATA_DIR / "consequential" / "blacklist.yaml"
CORRESPONDENCE_BIO_FLOWS = (
    DATA_DIR / "utils" / "export" / "correspondence_biosphere_flows.yaml"
)
FILEPATH_CLASSIFICATIONS = DATA_DIR / "utils" / "import" / "classifications.csv"

TEMP_CSV_FILE = DIR_CACHED_DB / "temp.csv"
TEMP_EXCEL_FILE = DIR_CACHED_DB / "temp.xlsx"

MIGRATIONS_DIR = DATA_DIR / "utils" / "import" / "migrations"



logging.basicConfig(
    level=logging.DEBUG,
    filename="unlinked.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_classifications():
    """
    Retrieve the classification of the datasets to import.
    """

    df = pd.read_csv(
        FILEPATH_CLASSIFICATIONS, sep=get_delimiter(filepath=FILEPATH_CLASSIFICATIONS)
    )

    # Build the nested dictionary
    classification_dict = {
        (row["name"], row["product"]): {
            "ISIC rev.4 ecoinvent": row["ISIC rev.4 ecoinvent"],
            "CPC": row["CPC"],
        }
        for _, row in df.iterrows()
    }

    return classification_dict


def get_correspondence_bio_flows():
    """
    Mapping between ei39 and ei<39 biosphere flows.
    """

    with open(CORRESPONDENCE_BIO_FLOWS, "r", encoding="utf-8") as stream:
        flows = yaml.safe_load(stream)
        return flows


def get_biosphere_code(version) -> dict:
    """
    Retrieve a dictionary with biosphere flow names and uuid codes.
    :returns: dictionary with biosphere flow names as keys and uuid codes as values

    """
    if version == "3.9":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_39.csv"
    elif version == "3.10":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_310.csv"
    elif version == "3.11":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_311.csv"
    elif version == "3.12":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_312.csv"
    elif version == "3.7":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_37.csv"
    else:
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"

    if not Path(fp).is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    with open(fp, encoding="utf-8") as file:
        input_dict = csv.reader(
            file,
            delimiter=get_delimiter(filepath=fp),
        )

        return {(row[0], row[1], row[2], row[3]): row[4] for row in input_dict}


def get_consequential_blacklist():
    with open(FILEPATH_CONSEQUENTIAL_BLACKLIST, "r", encoding="utf-8") as stream:
        flows = yaml.safe_load(stream)
        return flows


def normalize_version_for_migration(v: str) -> str:
    return normalize_version(v)

def normalize_version(v: str) -> str:
    """
    Normalize ecoinvent version strings so that:
    - '3.7.1' becomes '3.7'
    - '3.9.1' becomes '3.9'
    - '3.10.1' becomes '3.10'
    - '3.9' stays '3.9'
    - '3.9.0' becomes '3.9'
    """
    parts = v.split(".")
    if len(parts) == 2:
        return v  # already normalized
    if len(parts) >= 3:
        return ".".join(parts[:2])  # keep only major.minor
    return v



def discover_biosphere_migrations(debug=False):
    folder = MIGRATIONS_DIR / "biosphere"
    migrations = {}

    if debug:
        print(f"[migration] Looking for biosphere JSONs in: {folder}")

    for fp in sorted(folder.glob("*.json")):
        data = json.load(fp.open())

        raw_src = data["source_id"].split("-")[1]     # e.g. "3.5-biosphere"
        raw_dst = data["target_id"].split("-")[1]

        src = normalize_version(raw_src.replace("-biosphere", ""))
        dst = normalize_version(raw_dst.replace("-biosphere", ""))

        migrations[(src, dst)] = data

    return migrations



def discover_available_migrations(debug: bool = False) -> Dict[tuple, dict]:
    """
    Read all technosphere migration JSONs (cutoff) and return a mapping:
    {(source_version, target_version): json_data}

    If debug is True, print discovered steps.
    """
    folder = MIGRATIONS_DIR / "cutoff"
    migrations: Dict[tuple, dict] = {}

    if debug:
        print(f"[migration] Looking for JSONs in: {folder}")

    for fp in sorted(folder.glob("*.json")):
        with fp.open() as f:
            data = json.load(f)

        # example source_id: "ecoinvent-3.5-cutoff"
        try:
            raw_src = data["source_id"].split("-")[1]
            raw_dst = data["target_id"].split("-")[1]

            src = normalize_version(raw_src)
            dst = normalize_version(raw_dst)
        except Exception as e:
            raise ValueError(f"Malformed source_id/target_id in {fp}: {e}")

        migrations[(src, dst)] = data

    if debug:
        if not migrations:
            print("[migration] No migration JSONs found.")

    return migrations



def build_version_graph(available: Dict[tuple, dict]) -> dict:
    """Bidirectional graph: version -> list of (neighbor, direction)."""
    graph = {}
    versions = set()

    for (src, dst) in available.keys():
        versions.add(src)
        versions.add(dst)

    for v in versions:
        graph[v] = []

    for (src, dst) in available.keys():
        graph[src].append((dst, "forward"))
        graph[dst].append((src, "backward"))

    return graph


def resolve_migration_route(version_in: str, version_out: str, available: Dict[tuple, dict]):
    """
    Find a sequence of steps (src, dst, direction) from version_in to version_out,
    where direction is 'forward' or 'backward'.
    """
    if version_in == version_out:
        return []

    graph = build_version_graph(available)

    if version_in not in graph or version_out not in graph:
        msg = [
            f"Versions {version_in} or {version_out} not in migration graph.",
            "Known versions:",
            ", ".join(sorted(graph.keys())),
        ]
        raise ValueError("\n".join(msg))

    visited = {version_in}
    queue = deque([(version_in, [])])

    while queue:
        current, path = queue.popleft()
        for neighbor, direction in graph[current]:
            if neighbor in visited:
                continue
            step = (current, neighbor, direction)
            new_path = path + [step]
            if neighbor == version_out:
                return new_path
            visited.add(neighbor)
            queue.append((neighbor, new_path))

    # No path found – show what edges we actually have
    edges_str = "\n".join(
        f"  {s} -> {d}" for (s, d) in sorted(
            available.keys(),
            key=lambda v: (float(v[0].replace('.', '')), float(v[1].replace('.', '')))
        )
    )
    raise ValueError(
        f"No migration route found from {version_in} to {version_out}.\n"
        "Available migration pairs are:\n"
        f"{edges_str}"
    )


def matches_source(exc: dict, source: dict) -> bool:
    """
    Return True if exchange matches the given source specification.

    Only keys present in `source` are checked, except for keys we
    deliberately ignore: 'unit', 'allocation', 'comment'.
    """
    IGNORE_KEYS = {"unit", "allocation", "comment"}

    for key, value in source.items():
        if key in IGNORE_KEYS:
            continue
        if exc.get(key) != value:
            return False
    return True





def apply_disaggregation(db: list, disaggregate_rules: list):
    if not disaggregate_rules:
        return

    for ds in db:
        new_exchanges = []

        for exc in ds["exchanges"]:
            if exc.get("type") != "technosphere":
                new_exchanges.append(exc)
                continue

            rule = next(
                (r for r in disaggregate_rules if matches_source(exc, r["source"])),
                None,
            )

            if rule is None:
                new_exchanges.append(exc)
                continue

            original_amount = exc["amount"]

            for tgt in rule["targets"]:
                new_exc = exc.copy()

                for field in ("name", "reference product", "location"):
                    if field in tgt:
                        new_exc[field] = tgt[field]

                if "reference product" in tgt:
                    new_exc["product"] = tgt["reference product"]

                alloc = tgt.get("allocation", 1.0)
                new_exc["amount"] = original_amount * alloc

                new_exc.pop("input", None)

                new_exchanges.append(new_exc)

        ds["exchanges"] = new_exchanges



def apply_aggregation(db: list, disaggregate_rules: list):
    """
    Backward aggregation: inverse of apply_disaggregation.
    Many target exchanges are summed into a single source exchange.
    """
    if not disaggregate_rules:
        return

    for ds in db:
        exchanges = ds["exchanges"]
        new_exchanges = []
        used_indices = set()

        for idx, exc in enumerate(exchanges):
            if idx in used_indices:
                continue

            if exc.get("type") != "technosphere":
                new_exchanges.append(exc)
                continue

            applied_rule = False

            for rule in disaggregate_rules:
                targets = rule["targets"]

                if not any(matches_source(exc, t) for t in targets):
                    continue

                matching_indices = []
                total_amount = 0.0
                first_match = None

                for j, ex2 in enumerate(exchanges):
                    if j in used_indices:
                        continue
                    if ex2.get("type") != "technosphere":
                        continue
                    if any(matches_source(ex2, t) for t in targets):
                        matching_indices.append(j)
                        used_indices.add(j)
                        total_amount += ex2["amount"]
                        if first_match is None:
                            first_match = ex2

                if not matching_indices:
                    continue

                new_exc = first_match.copy()
                src = rule["source"]

                for field in ("name", "reference product", "location"):
                    if field in src:
                        new_exc[field] = src[field]

                if "reference product" in src:
                    new_exc["product"] = src["reference product"]

                new_exc["amount"] = total_amount
                new_exc.pop("input", None)

                new_exchanges.append(new_exc)
                applied_rule = True
                break

            if not applied_rule:
                new_exchanges.append(exc)

        ds["exchanges"] = new_exchanges


def apply_backward_replace(db: list, replace_rules: list):
    """
    Backward replace:
    for each rule, find exchanges matching the TARGET and change them back to SOURCE.
    """
    if not replace_rules:
        return

    for ds in db:
        for exc in ds["exchanges"]:
            if exc.get("type") not in ("technosphere", "biosphere"):
                continue

            for rule in replace_rules:
                src = rule["source"]
                tgt = rule["target"]

                if matches_source(exc, tgt):
                    for field in ("name", "reference product", "location", "uuid", "formula"):
                        if field in src:
                            exc[field] = src[field]

                    if "reference product" in src:
                        exc["product"] = src["reference product"]

                    exc.pop("input", None)
                    break

def apply_biosphere_migration(db, biosphere_rules):
    if not biosphere_rules:
        return

    # --- DELETE rules ---
    for ds in db:
        new_ex = []
        for exc in ds["exchanges"]:
            if exc.get("type") != "biosphere":
                new_ex.append(exc)
                continue

            should_delete = False
            for rule in biosphere_rules.get("delete", []):
                src = rule["source"]
                # UUID is ignored
                if exc.get("name") == src.get("name"):
                    should_delete = True
                    break

            if not should_delete:
                new_ex.append(exc)

        ds["exchanges"] = new_ex

    # --- REPLACE rules ---
    for ds in db:
        for exc in ds["exchanges"]:
            if exc.get("type") != "biosphere":
                continue

            for rule in biosphere_rules.get("replace", []):
                src = rule["source"]

                # UUID ignored
                if exc.get("name") != src.get("name"):
                    continue
                if "unit" in src and exc.get("unit") != src["unit"]:
                    continue

                # Apply all target attributes EXCEPT uuid
                for key, val in rule["target"].items():
                    if key == "uuid":
                        continue
                    exc[key] = val



def register_forward_migration_mapping(src_ver: str, dst_ver: str, data: dict) -> str:
    """
    Build and register a bw2io.Migration from JSON 'replace' and 'delete' sections.
    Returns the migration name.
    """
    mig_name = f"migration_{src_ver.replace('.', '')}_{dst_ver.replace('.', '')}"

    mapping = {
        "fields": ["name", "reference product", "location"],
        "data": [],
    }

    for item in data.get("replace", []):
        src = item["source"]
        tgt = item["target"]

        # make a copy without unit
        tgt_clean = {k: v for k, v in tgt.items() if k != "unit"}

        mapping["data"].append(
            (
                (
                    src.get("name"),
                    src.get("reference product"),
                    src.get("location"),
                ),
                tgt_clean,
            )
        )

    for item in data.get("delete", []):
        s = item["source"]
        mapping["data"].append(
            (
                (
                    s.get("name"),
                    s.get("reference product"),
                    s.get("location"),
                ),
            {},
            )
        )

    Migration(mig_name).write(
        mapping,
        description=f"Change technosphere names due to change from {src_ver} to {dst_ver}",
    )
    return mig_name


def apply_migration_step(importer, src_ver: str, dst_ver: str, direction: str, available: Dict[tuple, dict]):
    """
    Apply one migration step, either forward or backward.

    importer: self.import_db (ExcelImporter/CSVImporter)
    src_ver, dst_ver: strings like "3.6", "3.7"
    direction: "forward" or "backward"
    available: {(src, dst): json_data}
    """
    if direction == "forward":
        f_src, f_dst = src_ver, dst_ver
    elif direction == "backward":
        f_src, f_dst = dst_ver, src_ver
    else:
        raise ValueError(f"Unknown direction {direction}")

    data = available[(f_src, f_dst)]

    if direction == "forward":
        print(f"Applying forward migration {f_src} -> {f_dst}")
        mig_name = register_forward_migration_mapping(f_src, f_dst, data)
        importer.migrate(mig_name)
        apply_disaggregation(importer.data, data.get("disaggregate", []))
    else:
        print(f"Applying backward migration {f_src} <- {f_dst}")
        apply_backward_replace(importer.data, data.get("replace", []))
        apply_aggregation(importer.data, data.get("disaggregate", []))
        # 'delete' rules are not reversible and are ignored here.

    biosphere_available = discover_biosphere_migrations()
    if (f_src, f_dst) in biosphere_available:
        apply_biosphere_migration(importer.data, biosphere_available[(f_src, f_dst)])

def migrate_import_db(importer, version_in: str, version_out: str):
    """
    Apply all necessary steps (forward and/or backward) to migrate
    the imported inventory technosphere exchanges from version_in to version_out,
    using JSON migration files.
    """
    src = normalize_version_for_migration(version_in)
    dst = normalize_version_for_migration(version_out)

    if src == dst:
        return

    # set debug=True at least while you're testing
    available = discover_available_migrations(debug=False)

    route = resolve_migration_route(src, dst, available)

    print("Migration route:", " → ".join([route[0][0]] + [step[1] for step in route]))

    for step_src, step_dst, direction in route:
        apply_migration_step(importer, step_src, step_dst, direction, available)


def check_for_duplicate_datasets(data: List[dict]) -> List[dict]:
    """Check whether there are duplicate datasets in the inventory to import."""
    datasets = [(ds["name"], ds["reference product"], ds["location"]) for ds in data]
    duplicates = [
        item
        for item, count in itertools.groupby(sorted(datasets))
        if len(list(count)) > 1
    ]
    if duplicates:
        print("Duplicate datasets found (they need to be removed):")
        # print them using prettytable
        table = PrettyTable()
        table.field_names = ["Name", "Reference product", "Location"]
        for duplicate in duplicates:
            table.add_row(duplicate)
        print(table)

        # remove duplicates
        duplicates_added = []
        for ds in data:
            if (ds["name"], ds["reference product"], ds["location"]) in duplicates:
                if (
                    ds["name"],
                    ds["reference product"],
                    ds["location"],
                ) not in duplicates_added:
                    duplicates_added.append(
                        (ds["name"], ds["reference product"], ds["location"])
                    )
                else:
                    data.remove(ds)

    return data


def check_for_datasets_compliance_with_consequential_database(
    datasets: List[dict], blacklist: List[dict]
):
    """
    Check whether the datasets to import are compliant with the consequential database.

    :param datasets: list of datasets to import
    :param blacklist: list of datasets that are not in the consequential database
    :return: list of datasets that are compliant with the consequential database

    """
    # if system model is `consequential`` there is a
    # number of datasets we do not want to import

    tuples_of_blacklisted_datasets = [
        (i["name"], i["reference product"], i["unit"]) for i in blacklist
    ]

    datasets = [
        d
        for d in datasets
        if (d["name"], d["reference product"], d["unit"])
        not in tuples_of_blacklisted_datasets
    ]

    # also, we want to change exchanges that do not
    # exist in the consequential LCA database
    # and change them for the consequential equivalent

    for ds in datasets:
        for exchange in ds["exchanges"]:
            if exchange["type"] == "technosphere":
                exc_id = (
                    exchange["name"],
                    exchange.get("reference product"),
                    exchange["unit"],
                )

                if exc_id in tuples_of_blacklisted_datasets:
                    for d in blacklist:
                        if exc_id == (d["name"], d.get("reference product"), d["unit"]):
                            if "replacement" in d:
                                exchange["name"] = d["replacement"]["name"]
                                exchange["reference product"] = d["replacement"][
                                    "reference product"
                                ]
                                exchange["product"] = d["replacement"][
                                    "reference product"
                                ]
                                exchange["location"] = d["replacement"]["location"]

    return datasets


def check_amount_format(database: list) -> list:
    """
    Check that the `amount` field is of type `float`.
    :param database: database to check
    :return: database with corrected amount field
    """

    for dataset in database:
        for exc in dataset["exchanges"]:
            if not isinstance(exc["amount"], float):
                exc["amount"] = float(exc["amount"])

            if isinstance(exc["amount"], (np.float64, np.ndarray)):
                exc["amount"] = float(exc["amount"])

        for k, v in dataset.items():
            if isinstance(v, dict):
                for i, j in v.items():
                    if isinstance(j, (np.float64, np.ndarray)):
                        v[i] = float(v[i])

        for e in dataset["exchanges"]:
            for k, v in e.items():
                if isinstance(v, (np.float64, np.ndarray)):
                    e[k] = float(e[k])

    return database


def check_uncertainty_data(data, filename):
    MANDATORY_UNCERTAINTY_FIELDS = {
        2: {"loc", "scale"},  # lognormal
        3: {"loc", "scale"},  # normal
        4: {"minimum", "maximum"},  # uniform
        5: {"loc", "minimum", "maximum"},  # triangular
        6: {"loc", "minimum", "maximum"},
        7: {"minimum", "maximum"},
        8: {"loc", "scale", "shape"},
        9: {"loc", "scale", "shape"},
        10: {"loc", "scale", "shape"},
        11: {"loc", "scale", "shape"},
        12: {"loc", "scale", "shape"},
    }

    rows = []

    for dataset in data:
        for exc in dataset["exchanges"]:
            if exc["type"] in ["technosphere", "biosphere"]:
                if "uncertainty type" not in exc:
                    exc["uncertainty type"] = 0

                if exc["uncertainty type"] not in {0, 1}:
                    missing_parameters = [
                        f
                        for f in MANDATORY_UNCERTAINTY_FIELDS[exc["uncertainty type"]]
                        if exc.get(f) is None
                    ]
                    if missing_parameters:
                        rows.append(
                            [
                                dataset["name"][:30],
                                exc["name"][:30],
                                exc["uncertainty type"],
                                missing_parameters,
                            ]
                        )

                if exc["uncertainty type"] == 2:
                    if exc["amount"] < 0:
                        if exc.get("negative") is not True:
                            rows.append(
                                [
                                    dataset["name"][:30],
                                    exc["name"][:30],
                                    exc["uncertainty type"],
                                    "'negative' should be TRUE",
                                ]
                            )

                # if distribution is triangular, make sure that `minimum`
                # and `maximum` are not equal and are comprising the `loc`
                if exc["uncertainty type"] == 5:
                    if exc["minimum"] == exc["maximum"]:
                        rows.append(
                            [
                                dataset["name"][:30],
                                exc["name"][:30],
                                exc["uncertainty type"],
                                "minimum and maximum are equal",
                            ]
                        )

                    if (
                        not exc.get("minimum", 0)
                        <= exc.get("loc", 0)
                        <= exc.get("maximum", 0)
                    ):
                        rows.append(
                            [
                                dataset["name"][:30],
                                exc["name"][:30],
                                exc["uncertainty type"],
                                "loc not within minimum and maximum",
                            ]
                        )

    if len(rows) > 0:
        print(
            f"the following exchanges from {filename} are missing uncertainty information:"
        )
        table = PrettyTable()
        table.field_names = ["Name", "Exchange", "Uncertainty type", "Missing param."]
        table.add_rows(rows)
        print(table)


class BaseInventoryImport:
    """
    Base class for inventories that are to be merged with the wurst database.

    :ivar database: the target database for the import (the ecoinvent database),
    unpacked to a list of dicts
    :ivar version_in: the ecoinvent database version of the inventory to import
    :ivar version_out: the ecoinvent database version the imported inventories
    should comply with
    :ivar path: the filepath of the inventories to import

    """

    def __init__(
        self,
        database: List[dict],
        version_in: str,
        version_out: str,
        path: Union[str, Path],
        system_model: str,
        keep_uncertainty_data: bool = False,
    ) -> None:
        """Create a :class:`BaseInventoryImport` instance."""
        self.database = database
        # self.db_code = [x["code"] for x in self.database]
        self.db_names = [
            (x["name"].lower(), x["reference product"].lower(), x["location"])
            for x in self.database
        ]
        self.version_in = version_in
        self.version_out = version_out
        self.biosphere_dict = get_biosphere_code(self.version_out)
        self.correspondence_bio_flows = get_correspondence_bio_flows()
        self.system_model = system_model
        self.consequential_blacklist = get_consequential_blacklist()
        self.list_unlinked = []
        self.keep_uncertainty_data = keep_uncertainty_data
        self.path = path
        self.classifications = get_classifications()

        print(f"Importing {path}")
        if "http" in str(path):
            r = requests.head(path)
            if r.status_code != 200:
                raise ValueError("The file at {} could not be found.".format(path))
        else:
            if not Path(path).exists():
                raise FileNotFoundError(
                    f"The inventory file {path} could not be found."
                )

        self.path = Path(path) if isinstance(path, str) else path
        self.import_db = self.load_inventory()


    def load_inventory(self) -> None:
        """Load an inventory from a specified path.
        Sets the :attr:`import_db` attribute.
        :returns: Nothing.
        """
        return None

    def prepare_inventory(self) -> None:
        """Prepare the inventory for the merger with Ecoinvent.
        Modifies :attr:`import_db` in-place.
        :returns: Nothing
        """

    def check_for_already_existing_datasets(self) -> None:
        """
        Check whether the inventories to be imported are not
        already in the source database.
        """

        # print if we find datasets that already exist

        already_exist = []
        for ds in self.import_db.data:
            key = (
                ds["name"].lower(),
                ds["reference product"].lower(),
                ds["location"],
            )
            if key in self.db_names:
                already_exist.append(ds)

        if len(already_exist) > 0:
            print(
                "The following datasets to import already exist "
                "in the source database. "
                "They will not be imported"
            )
            table = PrettyTable(["Name", "Reference product", "Location", "File"])

            if isinstance(self.path, str):
                name = self.path
            else:
                name = self.path.name

            for dataset in already_exist:
                table.add_row(
                    [
                        dataset["name"][:30],
                        dataset["reference product"][:30],
                        dataset["location"],
                        name[:30],
                    ]
                )

            print(table)

        self.import_db.data = [
            ds for ds in self.import_db.data if ds not in already_exist
        ]

    def merge_inventory(self) -> List[dict]:
        """Prepare :attr:`import_db` and merge the inventory to the ecoinvent :attr:`database`.
        Calls :meth:`prepare_inventory`. Changes the :attr:`database` attribute.
        :returns: Nothing
        """

        self.prepare_inventory()
        return self.import_db

    def search_missing_exchanges(self, label: str, value: str) -> List[dict]:
        """
        Return a list of activities for which
        a given exchange cannot be found
        :param label: the label of the field to look for
        :param value: the value of the field to look for
        :return:
        """

        results = []
        for act in self.import_db.data:
            if (
                len([a for a in act["exchanges"] if label in a and a[label] == value])
                == 0
            ):
                results.append(act)

        return results

    def search_missing_field(self, field: str, scope: str = "activity") -> List[dict]:
        """Find exchanges and activities that do not contain a specific field
        in :attr:`imort_db`
        :param str field: label of the field to search for.
        :param scope: "activity" or "all". whether to search in the activity
        or the activity and its exchanges
        :returns: a list of dictionaries, activities and exchanges
        :rtype: list
        """
        results = []
        for act in self.import_db.data:
            if field not in act:
                results.append(act)

            if scope == "all":
                for ex in act["exchanges"]:
                    if ex["type"] == "technosphere" and field not in ex:
                        results.append(ex)
        return results

    def check_units(self) -> None:
        """
        Check that the units of the exchanges are compliant
        with the ecoinvent database.
        :returns: Nothing
        """

        ALLOWED_UNITS = [
            "kilogram",
            "cubic meter",
            "cubic meter-year",
            "kilowatt hour",
            "kilometer",
            "ton kilometer",
            "ton-kilometer",
            "megajoule",
            "unit",
            "square meter",
            "kilowatt hour",
            "square meter-year",
            "meter",
            "vehicle-kilometer",
            "person-kilometer",
            "person kilometer",
            "passenger-kilometer",
            "meter-year",
            "kilo Becquerel",
            "kilogram day",
            "kg*day",
            "hectare",
            "kilometer-year",
            "litre",
            "guest night",
            "Sm3",
            "standard cubic meter",
            "hour",
        ]

        for dataset in self.import_db.data:
            for exchange in dataset["exchanges"]:
                if exchange["unit"] not in ALLOWED_UNITS:
                    raise ValueError(
                        f"The unit {exchange['unit']} is not allowed in the ecoinvent database."
                        f"Please check the exchange {exchange} in the dataset {dataset['name']}."
                    )

    def fill_data_gaps(self, exc):
        """
        Some datatsets have the exchange amount set to zero because of ecoinvent license restrictions
        We need to replace the zero value with the correct amount, which we find in the ecoinvent database.
        """

        name, ref_prod, loc = (
            exc["replacement name"],
            exc["replacement product"],
            exc["replacement location"],
        )

        try:

            for ds in self.database:
                if (
                    ds["name"] == name
                    and ds["reference product"] == ref_prod
                    and ds["location"] == loc
                ):
                    sum_amount = 0
                    if exc["type"] == "technosphere":

                        for e in ws.technosphere(
                            ds,
                            ws.equals("name", exc["name"]),
                            ws.equals("product", exc["product"]),
                            # ws.equals("location", exc["location"]),
                        ):
                            sum_amount += e["amount"]

                    elif exc["type"] == "biosphere":
                        for e in ws.biosphere(
                            ds,
                            ws.equals("name", exc["name"]),
                            ws.equals("categories", exc["categories"]),
                            ws.equals("unit", exc["unit"]),
                        ):
                            sum_amount += e["amount"]

                    else:
                        raise ValueError(
                            f"Exchange type {exc['type']} not supported for filling data gaps."
                        )
                    if sum_amount == 0:
                        # trying with "market group for" or "market for"
                        if "market for" in exc["name"]:
                            n = exc["name"].replace("market for", "market group for")
                        elif "market group for" in exc["name"]:
                            n = exc["name"].replace("market group for", "market for")

                        else:
                            print(
                                f"Could not find a valid amount for exchange {exc['name']} in dataset {ds['name']} with reference product {ref_prod} and location {loc}"
                            )
                            return

                        for e in ws.technosphere(
                            ds,
                            ws.equals("name", n),
                            ws.equals("product", exc["product"]),
                            # ws.equals("location", exc["location"]),
                        ):
                            sum_amount += e["amount"]

                        if sum_amount == 0:
                            print(
                                f"Could not find a valid amount for exchange {exc['name']} | {exc['product']} in dataset {ds['name']} with reference product {ref_prod} and location {loc}"
                            )
                            return

                    exc["amount"] = sum_amount
                    exc.pop("replacement name", None)
                    exc.pop("replacement product", None)
                    exc.pop("replacement location", None)

        except ws.NoResults:
            print(
                f"Could not find a valid amount for exchange {exc['name']} in dataset {name} with reference product {ref_prod} and location {loc}"
            )
            raise

    def add_product_field_to_exchanges(self) -> None:
        """Add the `product` key to the production and
        technosphere exchanges in :attr:`import_db`.
        Also add `code` field if missing.
        For production exchanges, use the value of the `reference_product` field.
        For technosphere exchanges, search the activities in :attr:`import_db` and
        use the reference product. If none is found, search the Ecoinvent :attr:`database`.
        Modifies the :attr:`import_db` attribute in place.
        :raises IndexError: if no corresponding activity (and reference product) can be found.
        """
        # Add a `product` field to the production exchange
        for dataset in self.import_db.data:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "production":
                    if "product" not in exchange:
                        exchange["product"] = dataset["reference product"]

                    if exchange["name"] != dataset["name"]:
                        exchange["name"] = dataset["name"]

        # Add a `product` field to technosphere exchanges
        for dataset in self.import_db.data:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "technosphere":
                    # Check if the field 'product' is present
                    if not "product" in exchange:
                        try:
                            exchange["product"] = self.correct_product_field(
                                (
                                    exchange["name"],
                                    exchange["location"],
                                    exchange["unit"],
                                    exchange.get("reference product", None),
                                )
                            )
                        except KeyError:
                            print(
                                f"Could not find a product for {exchange} in {dataset['name']}"
                            )
                            raise IndexError()

                    # If a 'reference product' field is present, we make sure
                    # it matches with the new 'product' field
                    # if "reference product" in y:
                    if "reference product" in exchange:
                        try:
                            assert exchange["product"] == exchange["reference product"]
                        except AssertionError:
                            exchange["product"] = self.correct_product_field(
                                (
                                    exchange["name"],
                                    exchange["location"],
                                    exchange["unit"],
                                    exchange.get("reference product", None),
                                )
                            )

                if exchange["type"] in (
                    "technosphere",
                    "biosphere",
                ):
                    # check if amount is missing and need to be filled
                    if "replacement name" in exchange:
                        self.fill_data_gaps(exchange)

        # Add a `code` field if missing
        for dataset in self.import_db.data:
            if "code" not in dataset:
                dataset["code"] = str(uuid.uuid4().hex)

    @lru_cache
    def correct_product_field(self, exc: tuple) -> [str, None]:
        """
        Find the correct name for the `product` field of the exchange
        :param exc: a dataset exchange
        :return: name of the product field of the exchange

        """
        # Look first in the imported inventories
        candidate = next(
            ws.get_many(
                self.import_db.data,
                ws.equals("name", exc[0]),
                ws.equals("location", exc[1]),
                ws.equals("unit", exc[2]),
            ),
            None,
        )

        # If not, look in the ecoinvent inventories
        if candidate is None:
            if exc[-1] is not None:
                candidate = next(
                    ws.get_many(
                        self.database,
                        ws.equals("name", exc[0]),
                        ws.equals("location", exc[1]),
                        ws.equals("unit", exc[2]),
                        ws.equals("reference product", exc[-1]),
                    ),
                    None,
                )
            else:
                candidate = next(
                    ws.get_many(
                        self.database,
                        ws.equals("name", exc[0]),
                        ws.equals("location", exc[1]),
                        ws.equals("unit", exc[2]),
                    ),
                    None,
                )

        if candidate is not None:
            return candidate["reference product"]

        self.list_unlinked.append(
            (
                exc[0],
                exc[-1],
                exc[1],
                None,
                exc[2],
                "technosphere",
                self.path.name,
            )
        )

        return None

    def correct_keys(self):
        """Lower-case all keys in-place."""

        new_db = []
        for ds in self.import_db.data:
            new_ds = {k.lower(): v for k, v in ds.items()}
            new_ds["exchanges"] = [
                {k.lower(): v for k, v in exc.items()}
                for exc in ds.get("exchanges", [])
            ]
            new_db.append(new_ds)

        self.import_db.data = new_db

    def add_biosphere_links(self) -> None:
        """Add links for biosphere exchanges to :attr:`import_db`
        Modifies the :attr:`import_db` attribute in place.

        """
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "biosphere":
                    if isinstance(y["categories"], str):
                        y["categories"] = tuple(y["categories"].split("::"))

                    if len(y["categories"]) > 1:
                        key = (
                            y["name"],
                            y["categories"][0],
                            y["categories"][1],
                            y["unit"],
                        )
                    else:
                        key = (
                            y["name"],
                            y["categories"][0].strip(),
                            "unspecified",
                            y["unit"],
                        )

                    if key not in self.biosphere_dict:
                        if self.correspondence_bio_flows.get(key[1], {}).get(key[0]):
                            new_key = list(key)
                            new_key[0] = self.correspondence_bio_flows[key[1]][key[0]]
                            key = tuple(new_key)

                            if key not in self.biosphere_dict:
                                new_key = list(key)

                                try:
                                    new_key[0] = self.correspondence_bio_flows[key[1]][
                                        key[0]
                                    ]
                                    key = tuple(new_key)

                                    if key not in self.biosphere_dict:
                                        print(
                                            f"Could not find a biosphere flow for {key} in {self.path.name}. Exchange deleted."
                                        )
                                        y["delete"] = True

                                except KeyError:
                                    print(
                                        f"Could not find a biosphere flow for {key} in {self.path.name}. Exchange deleted."
                                    )
                                    y["delete"] = True
                            y["name"] = new_key[0]

                    # **New fallback step: Try without subcomparment before deleting**
                    if key not in self.biosphere_dict and y.get("delete") is False:
                        fallback_key = (key[0], key[1], "unspecified", key[3])
                        if fallback_key in self.biosphere_dict:
                            key = fallback_key
                            y["categories"] = (key[1], "unspecified")
                        else:
                            print(
                                f"Could not find a biosphere flow for {key} or {fallback_key} in {self.path.name}. Exchange deleted."
                            )
                            y["delete"] = True

                    try:
                        y["input"] = (
                            "biosphere3",
                            self.biosphere_dict[key],
                        )
                    except KeyError:
                        print(
                            f"Could not find a biosphere flow for {key} in {self.path.name}. You need to fix this."
                        )
                        # remove the exchange if it is not linked
                        y["delete"] = True

            x["exchanges"] = [y for y in x["exchanges"] if "delete" not in y]

    def lower_case_technosphere_exchanges(self) -> None:
        blakclist = [
            "NOx",
            "SOx",
            "N-",
            "EUR",
            "Mannheim",
            "Sohio",
        ]

        for ds in self.import_db.data:
            # lower case name and reference product
            # only if they are not in the blacklist
            # and if the first word is not an acronym
            if (
                not any(x in ds["name"] for x in blakclist)
                and not ds["name"].split(" ")[0].isupper()
            ):
                ds["name"] = ds["name"][0].lower() + ds["name"][1:]
            if (
                not any(x in ds["reference product"] for x in blakclist)
                and not ds["reference product"].split(" ")[0].isupper()
            ):
                ds["reference product"] = (
                    ds["reference product"][0].lower() + ds["reference product"][1:]
                )

            for exc in ds["exchanges"]:
                if exc["type"] in ["technosphere", "production"]:
                    if (
                        not any(x in exc["name"] for x in blakclist)
                        and not exc["name"].split(" ")[0].isupper()
                    ):
                        exc["name"] = exc["name"][0].lower() + exc["name"][1:]

                    if (
                        not any(
                            x in exc.get("reference product", "") for x in blakclist
                        )
                        and not exc.get("reference product", "").split(" ")[0].isupper()
                    ):
                        if exc.get("reference product") is not None:
                            exc["reference product"] = (
                                exc["reference product"][0].lower()
                                + exc["reference product"][1:]
                            )

                    if (
                        not any(x in exc.get("product", "") for x in blakclist)
                        and not exc.get("product", "").split(" ")[0].isupper()
                    ):
                        if exc.get("product") is not None:
                            exc["product"] = (
                                exc["product"][0].lower() + exc["product"][1:]
                            )

    def remove_ds_and_modifiy_exchanges(self, name: str, ex_data: dict) -> None:
        """
        Remove an activity dataset from :attr:`import_db` and replace the corresponding
        technosphere exchanges by what is given as second argument.
        :param str name: name of activity to be removed
        :param dict ex_data: data to replace the corresponding exchanges
        :returns: Nothing
        """

        self.import_db.data = [
            act for act in self.import_db.data if not act["name"] == name
        ]

        for act in self.import_db.data:
            for ex in act["exchanges"]:
                if ex["type"] == "technosphere" and ex["name"] == name:
                    ex.update(ex_data)
                    # make sure there is no existing link
                    if "input" in ex:
                        del ex["input"]

            # Delete any field that does not have information
            for key in act:
                if act[key] is None:
                    act.pop(key)

    def add_classifications(self):

        for ds in self.import_db.data:
            if (ds["name"], ds["reference product"]) in self.classifications:

                ds["classifications"] = [
                    (
                        "ISIC rev.4 ecoinvent",
                        self.classifications[(ds["name"], ds["reference product"])][
                            "ISIC rev.4 ecoinvent"
                        ],
                    ),
                    (
                        "CPC",
                        self.classifications[(ds["name"], ds["reference product"])][
                            "CPC"
                        ],
                    ),
                ]
            else:
                print(
                    f"WARNING: missing classification for {ds['name']} | {ds['reference product']}"
                )

    def display_unlinked_exchanges(self):
        """
        Display the list of unlinked exchanges
        using prettytable
        """
        print("List of unlinked exchanges:")

        table = PrettyTable()
        table.field_names = [
            "Name",
            "Reference product",
            "Location",
            "Categories",
            "Unit",
            "Type",
            "File",
        ]
        table.add_rows(list(set(self.list_unlinked)))
        print(table)

        for unlinked in self.list_unlinked:
            logging.warning(f'{"|".join(map(str, list(unlinked)))} | {self.version_in}')


class DefaultInventory(BaseInventoryImport):
    """
    Importing class. Inherits from :class:`BaseInventoryImport`.

    """

    def __init__(
        self,
        database,
        version_in,
        version_out,
        path,
        system_model,
        keep_uncertainty_data,
    ):
        super().__init__(
            database, version_in, version_out, path, system_model, keep_uncertainty_data
        )

    def load_inventory(self) -> bw2io.ExcelImporter:
        return ExcelImporter(self.path)

    def prepare_inventory(self) -> None:
        # --- NEW generic migration logic (forward & backward) ---
        migrate_import_db(self.import_db, self.version_in, self.version_out)
        # --------------------------------------------------------

        if self.system_model == "consequential":
            self.import_db.data = (
                check_for_datasets_compliance_with_consequential_database(
                    self.import_db.data, self.consequential_blacklist
                )
            )

        self.import_db.data = remove_categories(self.import_db.data)

        self.lower_case_technosphere_exchanges()
        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        self.check_units()
        self.correct_keys()
        self.add_classifications()

        # Remove uncertainty data
        if not self.keep_uncertainty_data:
            print("Remove uncertainty data.")
            self.import_db.data = remove_uncertainty(self.import_db.data)
        else:
            check_uncertainty_data(self.import_db.data, filename=Path(self.path).stem)

        # Check for duplicates
        self.check_for_already_existing_datasets()
        self.import_db.data = check_for_duplicate_datasets(self.import_db.data)

        if self.list_unlinked:
            self.display_unlinked_exchanges()


class VariousVehicles(BaseInventoryImport):
    """
    Imports various future vehicles' inventories (two-wheelers, buses, trams, etc.).

    :ivar database: wurst database
    :ivar version_in: original ecoinvent version of the inventories
    :ivar version_out: ecoinvent version the inventories should comply with
    :ivar path: filepath of the inventories
    :ivar year: year of the database
    :ivar regions:
    :ivar model: IAM model
    :ivar scenario: IAM scenario
    :ivar vehicle_type: "two-wheeler", "car, "truck" or "bus"
    :ivar relink: whether suppliers within a dataset need to be relinked
    :ivar has_fleet: whether the `vehicle_type` has associated fleet information
    """

    def __init__(
        self,
        database: List[dict],
        version_in: str,
        version_out: str,
        path: Union[str, Path],
        year: int,
        regions: List[str],
        model: str,
        scenario: str,
        vehicle_type: str,
        relink: bool = False,
        has_fleet: bool = False,
        system_model: str = "cutoff",
    ) -> None:
        super().__init__(database, version_in, version_out, path, system_model)
        self.year = year
        self.regions = regions
        self.model = model
        self.scenario = scenario
        self.vehicle_type = vehicle_type
        self.relink = relink
        self.has_fleet = has_fleet
        self.geo = Geomap(model=model)

    def load_inventory(self):
        return ExcelImporter(self.path)

    def prepare_inventory(self):
        # --- NEW generic migration logic (forward & backward) ---
        migrate_import_db(self.import_db, self.version_in, self.version_out)
        # --------------------------------------------------------

        self.lower_case_technosphere_exchanges()
        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_already_existing_datasets()
        self.check_units()
        self.add_classifications()

        if self.list_unlinked:
            self.display_unlinked_exchanges()

    def merge_inventory(self):
        self.database.extend(self.import_db.data)

        return self.database


class AdditionalInventory(BaseInventoryImport):
    """
    Import additional inventories, if any.
    """

    def __init__(self, database, version_in, version_out, path, system_model):
        super().__init__(database, version_in, version_out, path, system_model)

    def download_file(self, url, local_path) -> None:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Save Excel file directly as binary
            if Path(local_path).suffix == ".xlsx":
                with open(local_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)

            # Otherwise, assume it's a CSV (text)
            else:
                with open(local_path, "w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(
                        file,
                        quoting=csv.QUOTE_MINIMAL,
                        delimiter=",",
                        quotechar="'",
                        escapechar="\\",
                    )
                    for line in response.iter_lines():
                        decoded_line = line.decode("utf-8", errors="replace")
                        writer.writerow(decoded_line.split(","))

        except requests.RequestException as e:
            raise ConnectionError(f"Error downloading the file: {e}") from e

    def load_inventory(self):
        path_str = str(self.path)

        if "http" in path_str:
            if ":/" in path_str and "://" not in path_str:
                path_str = path_str.replace(":/", "://")
            self.download_file(
                path_str,
                TEMP_CSV_FILE if path_str.endswith(".csv") else TEMP_EXCEL_FILE,
            )
            temp_file_path = (
                TEMP_CSV_FILE if path_str.endswith(".csv") else TEMP_EXCEL_FILE
            )
        else:
            temp_file_path = self.path

        if temp_file_path.suffix == ".xlsx":
            return ExcelImporter(temp_file_path)
        if temp_file_path.suffix == ".csv":
            try:
                return CSVImporter(temp_file_path)
            except:
                raise ValueError(f"The file from {self.path} is not a valid CSV file.")

        raise ValueError(
            "Incorrect filetype for inventories. Should be either .xlsx or .csv"
        )

    def prepare_inventory(self):
        # --- NEW generic migration logic (forward & backward) ---
        migrate_import_db(self.import_db, self.version_in, self.version_out)
        # --------------------------------------------------------

        if self.system_model == "consequential":
            self.import_db.data = (
                check_for_datasets_compliance_with_consequential_database(
                    self.import_db.data, self.consequential_blacklist
                )
            )

        self.import_db.data = remove_categories(self.import_db.data)
        self.lower_case_technosphere_exchanges()
        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Check for duplicates
        self.check_for_already_existing_datasets()
        self.import_db.data = check_for_duplicate_datasets(self.import_db.data)
        # check numbers format
        self.import_db.data = check_amount_format(self.import_db.data)

        if self.list_unlinked:
            self.display_unlinked_exchanges()

    def merge_inventory(self) -> List[dict]:
        """Prepare :attr:`import_db` and merge the inventory to the ecoinvent :attr:`database`.
        Calls :meth:`prepare_inventory`. Changes the :attr:`database` attribute.
        :returns: Nothing
        """

        self.prepare_inventory()
        return self.import_db.data
