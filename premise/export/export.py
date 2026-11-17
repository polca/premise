"""
export.py contains all the functions to format, prepare and export databases.
"""

import csv
import json
import multiprocessing as mp
import os
import uuid
from collections import defaultdict
from functools import lru_cache
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import sparse
import yaml
from datapackage import Package
from pandas import DataFrame
from scipy import sparse as nsp

from .. import __version__
from ..iam.data_collection import get_delimiter
from ..core.filesystem_constants import DATA_DIR
from ..inventory.inventory_imports import (
    get_correspondence_bio_flows,
    normalize_version,
)
from ..core.utils import reset_all_codes
from ..validation.validation import (
    VALIDATION_RULESET_VERSION,
    BaseDatasetValidator,
    ValidationReport,
)

FILEPATH_SIMAPRO_UNITS = DATA_DIR / "utils" / "export" / "simapro_units.yml"
FILEPATH_SIMAPRO_COMPARTMENTS = (
    DATA_DIR / "utils" / "export" / "simapro_compartments.yml"
)
CORRESPONDENCE_BIO_FLOWS = (
    DATA_DIR / "utils" / "export" / "correspondence_biosphere_flows.yaml"
)

# current working directory
DIR_DATAPACKAGE = Path.cwd() / "export" / "datapackage"


def _has_semantic_certificate(scenario: dict) -> bool:
    payload = scenario.get("_validation_report")
    if not isinstance(payload, dict):
        return False
    try:
        report = ValidationReport.from_dict(payload)
    except (KeyError, TypeError, ValueError):
        return False
    return report.ruleset_version == VALIDATION_RULESET_VERSION and report.valid


def get_simapro_units() -> Dict[str, str]:
    """
    Load a dictionary that maps brightway2 unit to Simapro units.
    :return: a dictionary that maps brightway2 unit to Simapro units
    """

    with open(FILEPATH_SIMAPRO_UNITS, "r", encoding="utf-8") as stream:
        simapro_units = yaml.safe_load(stream)

    return simapro_units


def get_simapro_compartments() -> Dict[str, str]:
    """
    Load a dictionary that maps brightway2 unit to Simapro compartments.
    :return: a dictionary that maps brightway2 unit to Simapro compartments.
    """

    with open(FILEPATH_SIMAPRO_COMPARTMENTS, "r", encoding="utf-8") as stream:
        simapro_comps = yaml.safe_load(stream)

    return simapro_comps


def load_simapro_categories():
    """Load a dictionary with categories to use for Simapro export"""

    # Load the matching dictionary
    filename = "simapro_classification.csv"
    filepath = DATA_DIR / "utils" / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of Simapro categories could not be found."
        )
    with open(filepath, encoding="latin1") as f:
        csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
    _, *data = csv_list

    dict_cat = {}
    for row in data:
        _, cat_code, category_1, category_2, category_3 = row
        dict_cat[str(cat_code)] = {
            "category 1": category_1,
            "category 2": category_2,
            "category 3": category_3,
        }

    return dict_cat


def get_simapro_category_of_exchange():
    """Load a dictionary with categories to use for Simapro export based on ei 3.7"""

    # Load the matching dictionary
    filename = "simapro_categories.csv"
    filepath = DATA_DIR / "utils" / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of Simapro categories could not be found."
        )
    with open(
        filepath,
        encoding="latin-1",
    ) as file:
        csv_reader = csv.DictReader(file)

        dict_cat = {}
        # first row are headers
        for row in csv_reader:
            dict_cat[(row["name"].lower(), row["product"].lower())] = dict(row)

    return dict_cat


def resolve_simapro_category(
    name: str,
    product: str,
    categories: dict,
    default_main: str = "material",
    default_sub: str = r"Others\Transformation",
) -> tuple[str, str]:
    """
    Return a valid SimaPro category pair for an exchange or dataset.

    Some rows in ``simapro_categories.csv`` exist but have empty category fields.
    For export purposes, these must be treated like missing mappings, otherwise
    the resulting CSV contains an empty ``Category type`` line that SimaPro
    refuses to import.
    """

    entry = categories.get((name.lower(), product.lower()))
    if entry is None:
        return default_main, default_sub

    main_category = (entry.get("category") or "").strip()
    sub_category = (entry.get("sub_category") or "").strip()

    if not main_category or not sub_category:
        return default_main, default_sub

    return main_category, sub_category


def get_simapro_biosphere_dictionnary():
    """
    Load a dictionary with biosphere flows to use for Simapro export.
    """

    # Load the matching dictionary between ecoinvent and Simapro biosphere flows
    filename = "simapro-biosphere.json"
    filepath = DATA_DIR / "utils" / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of biosphere flow match between ecoinvent "
            "and Simapro could not be found."
        )
    with open(filepath, encoding="utf-8") as json_file:
        data = json.load(json_file)
    dict_bio = {}
    for row in data:
        dict_bio[row[2]] = row[1]

    return dict_bio


def create_index_of_A_matrix(database):
    """Map stripped activity identities to their positions; last duplicates win."""
    index = {}
    for i in range(len(database)):
        dataset = database[i]
        index[
            (
                dataset["name"].strip(),
                dataset["reference product"].strip(),
                dataset["unit"].strip(),
                dataset["location"].strip(),
            )
        ] = i
    return index


def rev_index(inds: dict) -> dict:
    """Reverse a mapping, keeping the last key when values repeat."""
    return {v: k for k, v in inds.items()}


def create_codes_index_of_biosphere_flows_matrix(version):
    """Map biosphere UUIDs to rows; the last catalog entry wins for duplicates."""
    data = biosphere_flows_dictionary(version)

    return {v: k for k, v in enumerate(data.values())}


def create_index_of_biosphere_flows_matrix(version):
    """Map (flow name, compartment, subcompartment, unit) to its matrix row."""
    data = biosphere_flows_dictionary(version)

    return {v: k for k, v in enumerate(data.keys())}


@lru_cache
def biosphere_flows_dictionary(version):
    """
    Create a dictionary with biosphere flows
    (name, category, sub-category, unit) -> code
    """
    version = normalize_version(str(version))
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

    csv_dict = {}

    with open(fp, encoding="utf-8") as file:
        input_dict = csv.reader(
            file,
            delimiter=get_delimiter(filepath=fp),
        )
        for row in input_dict:
            csv_dict[(row[0], row[1], row[2], row[3])] = row[-1]

    return csv_dict


def get_list_unique_acts(scenarios: List[dict]) -> list:
    """Return distinct exchange identities across scenario inventories.

    Identities are (name, product, categories, location, unit, type); amounts
    and input codes do not affect identity. Optional fields default to None.
    The returned list follows set iteration order, not a stable sort order.

    :param scenarios: scenario dictionaries containing inventory databases
    :return: list of unique exchange identities
    """

    identities = {
        (
            exchange["name"],
            exchange.get("product"),
            exchange.get("categories"),
            exchange.get("location"),
            exchange["unit"],
            exchange["type"],
        )
        for scenario in scenarios
        for activity in scenario["database"]
        for exchange in activity["exchanges"]
    }
    return list(identities)


bio_flows_correspondence = get_correspondence_bio_flows()
exc_codes = {}


@lru_cache
def fetch_exchange_code(name, ref, loc, unit):
    if (name, ref, loc, unit) not in exc_codes:
        code = str(uuid.uuid4().hex)
        exc_codes[(name, ref, loc, unit)] = code
    else:
        code = exc_codes[(name, ref, loc, unit)]

    return code


def get_act_dict_structure(ind, acts_ind, db_name) -> dict:
    name, ref, _, loc, unit, _ = acts_ind[ind]
    code = fetch_exchange_code(name, ref, loc, unit)

    return {
        "name": name,
        "reference product": ref,
        "unit": unit,
        "location": loc,
        "database": db_name,
        "code": code,
        "parameters": [],
        "exchanges": [],
    }


def correct_biosphere_flow(name, cat, unit, version):
    """Resolve an exact biosphere identity before trying compartment aliases.

    Unmapped identities raise ValueError. A configured alias pointing to a
    missing catalog entry retains KeyError.
    """

    bio_dict = biosphere_flows_dictionary(version)

    if len(cat) > 1:
        main_cat = cat[0]
        sub_cat = cat[1]
    else:
        main_cat = cat[0]
        sub_cat = "unspecified"

    key = (name, main_cat, sub_cat, unit)
    if key in bio_dict:
        return bio_dict[key]

    canonical_name = bio_flows_correspondence.get(main_cat, {}).get(name)
    if canonical_name:
        return bio_dict[(canonical_name, main_cat, sub_cat, unit)]

    raise ValueError(f"{name, cat, unit, version} not found in biosphere dictionary.")


def get_exchange(ind, acts_ind, db_name, version, amount=1.0):
    """Reconstruct one exchange, replacing only zero production amounts by one."""
    name, ref, cat, loc, unit, flow_type = acts_ind[ind]
    return {
        "name": name,
        "product": ref,
        "unit": unit,
        "location": loc,
        "categories": cat,
        "type": flow_type,
        "amount": amount if flow_type != "production" or amount != 0 else 1.0,
        "input": (
            ("biosphere3", correct_biosphere_flow(name, cat, unit, version))
            if flow_type == "biosphere"
            else (db_name, fetch_exchange_code(name, ref, loc, unit))
        ),
    }


def write_formatted_data(name, data, filepath):
    """
    Write a bw2io-style inventory CSV in activity and exchange order.

    :param name: name of the database
    :param data: data to write
    :param filepath: path to the file
    :return: the supplied output path
    """

    result = [["Database", name], []]

    for act in data:
        result.append(["Activity", act["name"]])
        result.append(["reference product", act["reference product"]])
        result.append(["unit", act["unit"]])
        result.append(["location", act["location"]])
        result.append(["comment", act.get("comment", "")])
        result.append(["source", act.get("source", "")])
        result.append(["parameters", act.get("parameters", [])])
        result.append([""])

        result.append(["Exchanges"])
        if act.get("exchanges"):
            result.append(
                [
                    "name",
                    "amount",
                    "unit",
                    "location",
                    "categories",
                    "type",
                    "product",
                ]
            )
            for exc in act["exchanges"]:
                result.append(
                    [
                        exc["name"],
                        exc["amount"],
                        exc["unit"],
                        exc.get("location"),
                        (
                            "::".join(list(exc.get("categories", [])))
                            if exc["type"] == "biosphere"
                            else None
                        ),
                        exc["type"],
                        exc.get("product"),
                    ]
                )
        result.append([])

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for line in result:
            writer.writerow(line)

    return filepath


def build_datapackage(
    df, inventories, list_scenarios, ei_version, name, system_model="cutoff"
):
    """
    Create and export a scenario datapackage.
    """

    with TemporaryDirectory(prefix="premise-datapackage-") as directory:
        staging = Path(directory)
        # check that directory exists, otherwise create it
        df.to_csv(staging / "scenario_data.csv", index=False, encoding="utf-8-sig")
        write_formatted_data(
            name=name, data=inventories, filepath=staging / "inventories.csv"
        )
        package = Package(base_path=str(staging))
        for filename in ("scenario_data.csv", "inventories.csv"):
            package.add_resource({"path": filename})
        package.infer()
        package.descriptor["name"] = name
        package.descriptor["title"] = name.capitalize()
        package.descriptor["description"] = (
            f"Data package generated by premise {__version__}."
        )
        package.descriptor["premise version"] = str(__version__)
        package.descriptor["dependencies"] = [
            {
                "name": "ecoinvent",
                "system model": "cut-off" if system_model == "cutoff" else system_model,
                "version": ei_version,
                "type": "source",
            },
            {
                "name": "biosphere3",
            },
        ]

        if len(list_scenarios[0].split(" - ")) == 3:
            package.descriptor["scenarios"] = [
                {
                    "name": s,
                    "description": f"Prospective db, "
                    f"based on {s.split(' - ')[0].upper()}, "
                    f"pathway {s.split(' - ')[1].upper()}, "
                    f"for the year {s.split(' - ')[-1]}.",
                }
                for s in list_scenarios[1:]
            ]
        else:
            package.descriptor["scenarios"] = [
                {
                    "name": s,
                    "description": f"Prospective db, "
                    f"based on {s.split(' - ')[0].upper()}, "
                    f"pathway {s.split(' - ')[1].upper()}, "
                    f"for the year {s.split(' - ')[2]}, and "
                    f"external scenario {' '.join(s.split(' - ')[3:])}.",
                }
                for s in list_scenarios[1:]
            ]

        package.descriptor["keywords"] = [
            "ecoinvent",
            "scenario",
            "data package",
            "premise",
        ]
        package.descriptor["licenses"] = [
            {
                "id": "CC0-1.0",
                "title": "CC0 1.0",
                "url": "https://creativecommons.org/publicdomain/zero/1.0/",
            }
        ]
        package.commit()

        # check that directory exists, otherwise create it
        Path(DIR_DATAPACKAGE).mkdir(parents=True, exist_ok=True)

        # save the datapackage
        package.save(DIR_DATAPACKAGE / f"{name}.zip")

        print(f"Data package saved at {DIR_DATAPACKAGE / f'{name}.zip'}")


def generate_scenario_factor_file(
    origin_db: list,
    scenarios: list,
    db_name: str,
    biosphere_name: str,
    version: str,
    scenario_list: list = None,
):
    """
    Generate scenario factors and additional inventories in reconstruction order.

    :param origin_db: the original database
    :param scenarios: a list of databases
    :param db_name: the name of the database
    :param biosphere_name: the name of the biosphere database
    :param version: the version of ecoinvent
    :param scenario_list: a list of external scenarios
    """

    print("Building scenario factor file...")

    # create the dataframe
    df, new_db, list_unique_acts = generate_scenario_difference_file(
        origin_db=origin_db,
        scenarios=scenarios,
        db_name=db_name,
        biosphere_name=biosphere_name,
        version=version,
        scenario_list=scenario_list,
    )

    original = df["original"]
    original = original.replace(0, 1)
    df.loc[:, "original":] = df.loc[:, "original":].div(original, axis=0)

    # remove the column `original`
    df = df.drop(columns=["original"])

    # Select by identity without changing the reconstruction order below.
    original_activity_keys = set(get_list_unique_acts([{"database": origin_db}]))
    new_activity_keys = set(list_unique_acts) - original_activity_keys

    print(f"Number of new activities: {len(new_activity_keys)}")

    # fetch the additional activities from new_db
    extra_acts = [
        dataset
        for dataset in new_db
        if (
            dataset["name"],
            dataset.get("reference product"),
            None,
            dataset.get("location"),
            dataset["unit"],
            "production",
        )
        in new_activity_keys
    ]

    return df, extra_acts


def generate_new_activities(args):
    k, v, acts_ind, db_name, version, dict_meta, m = args
    act = get_act_dict_structure(k, acts_ind, db_name)
    meta_id = tuple(list(acts_ind[k])[:-1])
    act.update(dict_meta[meta_id])

    act["exchanges"].extend(
        get_exchange(i, acts_ind, db_name, version, amount=m[i, k, 0]) for i in v
    )
    return act


def generate_scenario_difference_file(
    db_name,
    origin_db,
    scenarios,
    version,
    scenario_list,
    biosphere_name,
) -> tuple[DataFrame, list[dict], set[Any]]:
    """
    Generate a scenario difference file for a given list of databases
    :param db_name: name of the new database
    :param origin_db: the original database
    :param scenarios: list of databases
    """

    bio_dict = biosphere_flows_dictionary(version)

    # Turn list into set for O(1) membership tests
    list_acts = set(get_list_unique_acts([{"database": origin_db}] + scenarios))

    acts_ind = dict(enumerate(list_acts))
    acts_ind_rev = {v: k for k, v in acts_ind.items()}

    list_scenarios = ["original"] + scenario_list

    list_dbs = [origin_db] + [a["database"] for a in scenarios]

    matrices = {
        a: nsp.lil_matrix((len(list_acts), len(list_acts)))
        for a, _ in enumerate(list_scenarios)
    }

    # Use defaultdict to avoid key errors
    dict_meta = defaultdict(dict)

    for db in list_dbs:
        for a in db:
            key = (a["name"], a["reference product"], None, a["location"], a["unit"])
            dict_meta[key] = {
                b: c
                for b, c in a.items()
                if b
                not in {
                    "exchanges",
                    "code",
                    "name",
                    "reference product",
                    "location",
                    "unit",
                    "database",
                }
            }

    for i, db in enumerate(list_dbs):
        for ds in db:
            if not ds["exchanges"]:
                continue
            matrix = matrices[i]
            c = (
                ds["name"],
                ds.get("reference product"),
                ds.get("categories"),
                ds.get("location"),
                ds["unit"],
                "production",
            )
            column = acts_ind_rev[c]
            for exc in ds["exchanges"]:
                s = (
                    exc["name"],
                    exc.get("product"),
                    exc.get("categories"),
                    exc.get("location"),
                    exc["unit"],
                    exc["type"],
                )
                matrix[acts_ind_rev[s], column] += exc["amount"]

    m = sparse.stack([sparse.COO(x) for x in matrices.values()], axis=-1)
    dataframe_rows = []

    inds_d = defaultdict(list)
    for consumer, supplier in sparse.argwhere(m.sum(-1).T != 0):
        inds_d[consumer].append(supplier)

    with Pool(processes=mp.cpu_count()) as pool:
        new_db = pool.map(
            generate_new_activities,
            [
                (k, v, acts_ind, db_name, version, dict_meta, m)
                for k, v in inds_d.items()
            ],
        )

    inds_std = sparse.argwhere((m[..., 1:] == m[..., 0, None]).all(axis=-1).T == False)
    inds_std = _include_production_rows_for_changing_self_consumption(
        indices=inds_std,
        acts_ind=acts_ind,
    )

    for i in inds_std:
        c_name, c_ref, c_cat, c_loc, c_unit, _ = acts_ind[i[0]]
        s_name, s_ref, s_cat, s_loc, s_unit, s_type = acts_ind[i[1]]

        database_name = db_name
        exc_key_supplier = None

        if s_type == "biosphere":
            database_name = "biosphere3"

            key_exc = (
                s_name,
                s_cat[0],
                s_cat[1] if len(s_cat) > 1 else "unspecified",
                s_unit,
            )

            if key_exc in bio_dict:
                exc_key_supplier = (
                    database_name,
                    bio_dict[key_exc],
                )
            else:
                exc_key_supplier = (
                    database_name,
                    bio_dict[
                        bio_flows_correspondence.get(s_cat[0], {}).get(s_name, s_name),
                        s_cat[0],
                        s_cat[1] if len(s_cat) > 1 else "unspecified",
                        s_unit,
                    ],
                )

        row = [
            s_name,
            s_ref,
            s_loc,
            s_cat,
            database_name,
            exc_key_supplier,
            s_unit,
            c_name,
            c_ref,
            c_loc,
            c_cat,
            c_unit,
            db_name,
            None,
            s_type,
        ]

        row.extend(m[i[1], i[0], :])

        dataframe_rows.append(row)

    columns = [
        "from activity name",
        "from reference product",
        "from location",
        "from categories",
        "from database",
        "from key",
        "from unit",
        "to activity name",
        "to reference product",
        "to location",
        "to categories",
        "to unit",
        "to database",
        "to key",
        "flow type",
    ] + list_scenarios

    df = pd.DataFrame(dataframe_rows, columns=columns)

    df["to categories"] = None
    df = df.replace({"None": None, np.nan: None})
    df.loc[
        df["flow type"] == "biosphere", ["from reference product", "from location"]
    ] = None
    df.loc[df["flow type"].isin(["technosphere", "production"]), "from categories"] = (
        None
    )

    df.loc[df["flow type"] == "biosphere", "from database"] = biosphere_name

    # update the tuples in `from key` to make sure the first element
    # is the biosphere database name
    df.loc[df["flow type"] == "biosphere", "from key"] = df.loc[
        df["flow type"] == "biosphere", "from key"
    ].map(lambda x: (biosphere_name, x[1]))

    new_db, df = find_technosphere_keys(new_db, df)

    # return the dataframe and the new db
    return df, new_db, list_acts


def find_technosphere_keys(db, df):
    """Refresh inventory codes and fill activity keys without changing row labels.

    Production/technosphere keys are rebuilt; existing biosphere source keys
    are retained. Missing suppliers remain unresolved. Assignment follows row
    positions so filtered, duplicate and MultiIndex labels do not misalign keys.
    """
    indexer = df["flow type"].isin(["technosphere", "production"])
    df.loc[indexer, "from key"] = None
    df.loc[indexer | df["flow type"].eq("biosphere"), "to key"] = None
    db = reset_all_codes(db)

    activity_keys = {
        (a["name"], a["reference product"], a["location"]): (a["database"], a["code"])
        for a in db
    }

    for side in ("from", "to"):
        key = f"{side} key"
        indexer = df[key].isna()
        if not indexer.any():
            continue
        columns = [
            f"{side} activity name",
            f"{side} reference product",
            f"{side} location",
        ]
        if indexer.all():
            rows = df
            indexer = slice(None)
        else:
            rows = df.loc[indexer, columns]
        df.loc[indexer, key] = (
            pd.Series(list(zip(*(rows[column] for column in columns))))
            .map(activity_keys)
            .to_numpy()
        )

    return db, df


def _resolve_superstructure_flow_type(flow_types: pd.Series) -> str:
    unique_flow_types = {flow_type for flow_type in flow_types if flow_type is not None}

    if len(unique_flow_types) == 1:
        return unique_flow_types.pop()

    if _is_production_consumption_flow_type_set(unique_flow_types):
        # A self-loop technosphere exchange nets against the diagonal
        # production exchange and should stay represented as a production row.
        return "production"

    raise ValueError(
        f"Cannot aggregate superstructure rows with incompatible flow types: {sorted(unique_flow_types)}."
    )


def _is_production_consumption_flow_type_set(flow_types) -> bool:
    """Return whether flow types describe production plus one negative input."""

    flow_types = set(flow_types)
    return (
        len(flow_types) == 2
        and "production" in flow_types
        and bool(flow_types & {"technosphere", "generic consumption"})
    )


def _include_production_rows_for_changing_self_consumption(
    indices, acts_ind: dict
) -> list[tuple[int, int]]:
    """Include unchanged production rows needed to net changing diagonal inputs."""

    coordinates = [tuple(map(int, index)) for index in indices]
    coordinate_set = set(coordinates)
    negative_edge_types = {"technosphere", "generic consumption"}

    for consumer_index, supplier_index in coordinates:
        consumer = acts_ind[consumer_index]
        supplier = acts_ind[supplier_index]
        same_activity = all(
            supplier[position] == consumer[position] for position in (0, 1, 3, 4)
        )
        production_coordinate = (consumer_index, consumer_index)
        if (
            supplier[-1] in negative_edge_types
            and same_activity
            and production_coordinate not in coordinate_set
        ):
            coordinates.append(production_coordinate)
            coordinate_set.add(production_coordinate)

    return coordinates


def _net_superstructure_scenario_values(
    df: pd.DataFrame, scenario_columns: list[str], group_columns: list[str]
) -> pd.DataFrame:
    """
    Aggregate scenario values for rows that map to the same matrix coordinate.

    Mixed ``production`` + ``technosphere`` self-loops must net the
    technosphere amount against the production amount so the resulting row stays
    on the production diagonal with the correct residual value.
    """

    flow_type_sets = (
        df.groupby(group_columns, sort=False, dropna=False)["flow type"]
        .agg(lambda values: frozenset(v for v in values if v is not None))
        .reset_index(name="flow type set")
    )

    invalid_flow_type_sets = flow_type_sets[
        ~flow_type_sets["flow type set"].map(
            lambda flow_type_set: len(flow_type_set) == 1
            or _is_production_consumption_flow_type_set(flow_type_set)
        )
    ]
    if not invalid_flow_type_sets.empty:
        invalid_sets = sorted(
            {
                tuple(sorted(flow_type_set))
                for flow_type_set in invalid_flow_type_sets["flow type set"]
            }
        )
        raise ValueError(
            "Cannot aggregate superstructure rows with incompatible flow types: "
            f"{invalid_sets}."
        )

    signed_df = df.merge(flow_type_sets, on=group_columns, how="left")
    mixed_self_loops = signed_df["flow type set"].map(
        _is_production_consumption_flow_type_set
    )
    signed_df.loc[
        mixed_self_loops
        & signed_df["flow type"].isin({"technosphere", "generic consumption"}),
        scenario_columns,
    ] = signed_df.loc[
        mixed_self_loops
        & signed_df["flow type"].isin({"technosphere", "generic consumption"}),
        scenario_columns,
    ].mul(
        -1
    )

    return (
        signed_df.groupby(group_columns, sort=False, dropna=False)[scenario_columns]
        .sum()
        .reset_index()
    )


def _aggregate_duplicate_superstructure_rows(
    df: pd.DataFrame, scenario_columns: list[str]
) -> tuple[pd.DataFrame, int, int]:
    """
    Collapse rows that map to the same matrix coordinate.

    Exact duplicate rows are dropped first. Remaining collisions on
    ``("from key", "to key")`` are aggregated by netting scenario values while
    keeping the first descriptive metadata row for readability.
    """

    before = len(df)
    df = df.drop_duplicates()
    exact_duplicates = before - len(df)

    if df.empty:
        return df, exact_duplicates, 0

    # Production exchanges must stay on the diagonal with value 1 before any
    # netting with same-coordinate technosphere exchanges.
    for scenario in scenario_columns:
        df.loc[(df["flow type"] == "production") & (df[scenario] == 0), scenario] = 1

    group_columns = ["from key", "to key"]
    duplicate_collisions = len(df) - len(df.drop_duplicates(subset=group_columns))

    if duplicate_collisions == 0:
        return df, exact_duplicates, 0

    metadata = df.groupby(group_columns, sort=False, dropna=False).first().reset_index()
    totals = _net_superstructure_scenario_values(
        df=df,
        scenario_columns=scenario_columns,
        group_columns=group_columns,
    )
    flow_types = (
        df.groupby(group_columns, sort=False, dropna=False)["flow type"]
        .agg(_resolve_superstructure_flow_type)
        .reset_index()
    )

    metadata = metadata.drop(columns=scenario_columns + ["flow type"])
    aggregated = metadata.merge(flow_types, on=group_columns).merge(
        totals, on=group_columns
    )

    return aggregated, exact_duplicates, duplicate_collisions


def generate_superstructure_db(
    origin_db,
    scenarios,
    db_name,
    biosphere_name,
    filepath,
    version,
    scenario_list,
    file_format="excel",
    preserve_original_column: bool = False,
) -> List[dict]:
    """
    Build a superstructure database from a list of databases
    :param origin_db: the original database
    :param scenarios: a list of modified databases
    :param db_name: the name of the new database
    :param filepath: the filepath of the new database
    :param version: the version of the new database
    :param file_format: the format of the scenario difference file. Can be "excel", "csv" or "feather".
    :param preserve_original_column: whether to keep the original column in the scenario difference file
    :param scenario_list: a list of external scenarios

    :return: a superstructure database
    """

    new_db, df = _build_superstructure_db(
        origin_db=origin_db,
        scenarios=scenarios,
        db_name=db_name,
        biosphere_name=biosphere_name,
        scenario_list=scenario_list,
        version=version,
        preserve_original_column=preserve_original_column,
    )

    if filepath is not None:
        filepath = Path(filepath)
    else:
        filepath = Path.cwd() / "export" / "scenario diff files"

    if not os.path.exists(filepath):
        os.makedirs(filepath)

    # if df is longer than the row limit of Excel,
    # the export to Excel is not an option
    if len(df) > 1048576:
        file_format = "csv"
        print(
            "The scenario difference file is too long to be exported to Excel. Exporting to CSV instead."
        )

    if file_format == "excel":
        filepath_sdf = filepath / f"scenario_diff_{db_name}.xlsx"
        df.to_excel(filepath_sdf, index=False)
    elif file_format == "csv":
        filepath_sdf = filepath / f"scenario_diff_{db_name}.csv"
        df.to_csv(filepath_sdf, index=False, sep=";", encoding="utf-8-sig")
    elif file_format == "feather":
        filepath_sdf = filepath / f"scenario_diff_{db_name}.feather"
        df.to_feather(filepath_sdf)
    else:
        raise ValueError(f"Unknown format {file_format}")

    print(f"Scenario difference file exported to {filepath}!")

    return new_db


def _build_superstructure_db(
    origin_db,
    scenarios,
    db_name,
    biosphere_name,
    version,
    scenario_list,
    preserve_original_column: bool = True,
) -> tuple[List[dict], pd.DataFrame]:
    """Build a union database and finalized scenario dataframe without writing files."""

    print("Building superstructure database...")

    df, new_db, _ = generate_scenario_difference_file(
        origin_db=origin_db,
        scenarios=scenarios,
        db_name=db_name,
        biosphere_name=biosphere_name,
        version=version,
        scenario_list=scenario_list,
    )

    df = df.drop(columns=["to unit"])
    df = df.rename(columns={"from unit": "unit"})
    if "unit" in df.columns:
        df = df.drop(columns=["unit"])

    if preserve_original_column:
        scenario_columns = ["original", *scenario_list]
    else:
        df = df.drop(columns=["original"])
        scenario_columns = scenario_list

    df, exact_duplicates, duplicate_collisions = (
        _aggregate_duplicate_superstructure_rows(
            df=df,
            scenario_columns=scenario_columns,
        )
    )

    if exact_duplicates:
        print(f"Dropped {exact_duplicates} exact duplicate(s).")
    if duplicate_collisions:
        print(
            f"Collapsed {duplicate_collisions} overlapping row(s) by netting scenario values."
        )

    return new_db, df


def check_geographical_linking(scenario, original_database):
    """Prefer an indexed local supplier for eligible new regional activities.

    Original activity identities, global locations, markets, import/mix/transport
    names and self-supply exchanges retain their links. Name exclusions remain
    case-sensitive. Only exchange locations change; the supplier index is read-only.
    """
    index = scenario.get("index") or {}
    original_datasets = {
        (a["name"], a["reference product"], a["location"]) for a in original_database
    }
    supplier_locations = {}
    excluded_names = ("import", "mix", "transport")

    for ds in scenario["database"]:
        name, product, location = ds["name"], ds["reference product"], ds["location"]
        if (
            (name, product, location) in original_datasets
            or location in {"GLO", "RoW", "World"}
            or "market" in name
            or any(word in name for word in excluded_names)
        ):
            continue
        for exc in ds["exchanges"]:
            if (
                exc["type"] != "technosphere"
                or any(word in exc["name"] for word in excluded_names)
                or exc["location"] == location
            ):
                continue
            key = (exc["name"], exc["product"])
            if key == (name, product):
                continue
            locations = supplier_locations.get(key)
            if locations is None:
                locations = {provider["location"] for provider in index.get(key, ())}
                supplier_locations[key] = locations
            if location in locations:
                exc["location"] = location

    return scenario


def prepare_db_for_export(
    scenario, name, original_database, version, biosphere_name=None
):
    """
    Prepare a database for export.
    """

    # ensuring that all geographically appropriate exchanges are present
    scenario = check_geographical_linking(scenario, original_database)

    # validate the database
    validator = BaseDatasetValidator(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        original_database=original_database,
        database=scenario["database"],
        db_name=name,
        biosphere_name=biosphere_name,
        version=version,
        extra_regions=scenario.get("additional valid regions"),
    )
    make_normalizer = getattr(validator, "make_normalizer", None)
    if make_normalizer is not None:
        validator.database = make_normalizer().normalize_database()
    if _has_semantic_certificate(scenario):
        report = validator.run_export_schema_checks()
    else:
        report = validator.run_all_checks()

    if isinstance(report, ValidationReport) and report.phase_results:
        scenario["_export_validation_phase"] = report.phase_results[0].to_dict()

    return validator.database


def prepare_db_for_fast_export(scenario, name, version, biosphere_name=None):
    """
    Prepare a database for Brightway export using only the minimal
    formatting and validation steps required by the fast writer.
    """

    validator = BaseDatasetValidator(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        original_database=[],
        database=scenario["database"],
        db_name=name,
        biosphere_name=biosphere_name,
        version=version,
        extra_regions=scenario.get("additional valid regions"),
    )
    run_session = getattr(validator, "run_fast_export_session", None)
    if run_session is not None:
        validator.database, report = run_session()
    else:
        # Compatibility path for third-party validator subclasses which have
        # not adopted the combined export session protocol.
        make_normalizer = getattr(validator, "make_normalizer", None)
        if make_normalizer is not None:
            normalizer = make_normalizer()
            prepare_fast_fields = getattr(
                normalizer, "prepare_fast_export_fields", None
            )
            if prepare_fast_fields is not None:
                validator.database = prepare_fast_fields()
        if _has_semantic_certificate(scenario) and hasattr(
            validator, "run_export_schema_checks"
        ):
            report = validator.run_export_schema_checks()
        else:
            report = validator.run_fast_export_checks()

    if isinstance(report, ValidationReport) and report.phase_results:
        scenario["_export_validation_phase"] = report.phase_results[0].to_dict()

    return validator.database


def _prepare_database(scenario, db_name, original_database, biosphere_name, version):

    scenario["database"] = prepare_db_for_export(
        scenario,
        name=db_name,
        original_database=original_database,
        biosphere_name=biosphere_name,
        version=version,
    )

    return scenario


class Export:
    """
    Export production/technosphere (A) and biosphere (B) coordinate CSVs.

    Lookup CSVs identify the numbered activities, products and biosphere flows.
    Each file export builds fresh indexes and shares them across coordinate and
    lookup files. Standalone coordinate methods also use current inventory data.
    Coordinate generation resolves each activity's number when needed and reuses
    it for the remaining exchanges in that activity.

    :ivar db: transformed database
    :vartype db: list
    :ivar scenario: name of an IAM pathway
    :vartype scenario: str
    :ivar year: year of scenario
    :vartype year: int

    """

    def __init__(
        self,
        scenario: dict = None,
        filepath: Path = None,
        version: str = None,
        system_model: str = None,
    ):
        self.db = scenario["database"]
        self.model = scenario["model"]
        self.scenario = scenario["pathway"]
        self.inventory_definition = {
            key: scenario[key]
            for key in ("model", "pathway", "year", "clic inventories", "clic recipe")
            if key in scenario
        }
        self.year = scenario["year"]
        self.filepath = filepath
        self.version = version
        self.system_model = system_model

    def create_A_matrix_coordinates(self) -> list:
        """Return production and technosphere rows with uncertainty fields."""
        return self._create_A_matrix_coordinates(create_index_of_A_matrix(self.db))

    def _create_A_matrix_coordinates(self, index_A) -> list:
        list_exchanges = []

        try:
            for ds in self.db:
                activity_index = None
                for exc in ds["exchanges"]:
                    exchange_type = exc["type"]
                    if exchange_type not in ("production", "technosphere"):
                        continue
                    if activity_index is None:
                        activity_index = index_A[
                            (
                                ds["name"],
                                ds["reference product"],
                                ds["unit"],
                                ds["location"],
                            )
                        ]

                    if exchange_type == "production":
                        row = [
                            activity_index,
                            index_A[
                                (
                                    exc["name"],
                                    exc["product"],
                                    exc["unit"],
                                    exc["location"],
                                )
                            ],
                            exc["amount"],
                            0,
                            exc["amount"],
                            None,
                            None,
                            None,
                            None,
                            0,
                            0,
                        ]
                    else:
                        row = [
                            activity_index,
                            index_A[
                                (
                                    exc["name"],
                                    exc["product"],
                                    exc["unit"],
                                    exc["location"],
                                )
                            ],
                            exc["amount"],
                            exc.get("uncertainty type", 0),
                            (
                                exc["loc"]
                                if exc.get("uncertainty type", 0) not in [0, 1]
                                else exc["amount"]
                            ),
                            exc.get("scale"),
                            exc.get("shape"),
                            exc.get("minimum"),
                            exc.get("maximum"),
                            1 if exc["amount"] < 0 else 0,
                            1,
                        ]
                    list_exchanges.append(row)

        except KeyError:
            print(f"KeyError for {exc} in {ds['name']}")

        return list_exchanges

    def create_B_matrix_coordinates(self):
        """Return biosphere rows using the current version's UUID numbering."""
        return self._create_B_matrix_coordinates(
            create_index_of_A_matrix(self.db),
            create_codes_index_of_biosphere_flows_matrix(self.version),
        )

    def _create_B_matrix_coordinates(self, index_A, biosphere_codes):
        list_rows = []

        for ds in self.db:
            activity_index = None
            for exc in ds["exchanges"]:
                if exc["type"] == "biosphere":
                    try:
                        ind_B = biosphere_codes[exc["input"][1]]
                        if activity_index is None:
                            activity_index = index_A[
                                (
                                    ds["name"],
                                    ds["reference product"],
                                    ds["unit"],
                                    ds["location"],
                                )
                            ]

                        row = [
                            activity_index,
                            ind_B,
                            exc["amount"],
                            exc.get("uncertainty type", 0),
                            (
                                exc["loc"]
                                if exc.get("uncertainty type", 0) not in [0, 1]
                                else exc["amount"]
                            ),
                            exc.get("scale"),
                            exc.get("shape"),
                            exc.get("minimum"),
                            exc.get("maximum"),
                            1 if exc["amount"] < 0 else 0,
                            1,
                        ]
                    except KeyError:
                        print(
                            "Cannot find the biosphere flow",
                            exc["name"],
                            exc["categories"],
                            "in ",
                            ds["name"],
                            ds["reference product"],
                            ds["location"],
                        )
                        row = ()
                    list_rows.append(row)
        return list_rows

    def export_db_to_matrices(self):
        """Write the four matrix CSVs with one consistent set of fresh indexes."""

        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)

        if self.inventory_definition.get("clic inventories"):
            from ..reporting.clic import write_inventory_sidecars

            write_inventory_sidecars(
                self.filepath / "inventory", [self.inventory_definition]
            )

        index_A = create_index_of_A_matrix(self.db)
        index_B = create_index_of_biosphere_flows_matrix(self.version)
        biosphere_codes = create_codes_index_of_biosphere_flows_matrix(self.version)

        # Export A matrix
        rows = self._create_A_matrix_coordinates(index_A)

        with open(self.filepath / "A_matrix.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            writer.writerow(
                [
                    "index of activity",
                    "index of product",
                    "value",
                    "uncertainty type",
                    "loc",
                    "scale",
                    "shape",
                    "minimum",
                    "maximum",
                    "negative",
                    "flip",
                ]
            )
            writer.writerows(rows)

        # Export A index
        with open(self.filepath / "A_matrix_index.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            writer.writerow(["name", "reference product", "unit", "location", "index"])
            for d in index_A:
                data = list(d) + [index_A[d]]
                writer.writerow(data)

        # Export B matrix
        with open(self.filepath / "B_matrix.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            writer.writerow(
                [
                    "index of activity",
                    "index of biosphere flow",
                    "value",
                    "uncertainty type",
                    "loc",
                    "scale",
                    "shape",
                    "minimum",
                    "maximum",
                    "negative",
                    "flip",
                ]
            )
            rows = self._create_B_matrix_coordinates(index_A, biosphere_codes)
            writer.writerows(rows)

        # Export B index
        with open(self.filepath / "B_matrix_index.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            writer.writerow(["name", "compartment", "subcompartment", "unit", "index"])
            for d in index_B:
                data = list(d) + [index_B[d]]
                writer.writerow(data)

        print(f"Matrices saved in {self.filepath}.")

    @staticmethod
    def create_rev_index_of_B_matrix(version):
        """Map UUIDs to flow descriptions, preserving last-entry duplicate handling."""
        return rev_index(biosphere_flows_dictionary(version))

    def rev_index(self, inds):
        """Compatibility entry point for reversing a mapping."""
        return rev_index(inds)
