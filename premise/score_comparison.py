import pandas as pd
import bw2data, bw2calc
from tqdm import tqdm
import sys
import re

from .new_database import NewDatabase


def _parse_version_tuple(version) -> tuple:
    """Return a comparable 3-int version tuple from str/list/tuple versions."""
    if isinstance(version, (tuple, list)):
        parts = [int(x) for x in version[:3]]
    else:
        parts = [int(x) for x in re.findall(r"\d+", str(version))[:3]]

    while len(parts) < 3:
        parts.append(0)

    return tuple(parts)


def _is_brightway25_stack() -> bool:
    """True when using BW2.5 libraries (bw2calc>=2 and bw2data>=4)."""
    return _parse_version_tuple(getattr(bw2calc, "__version__", "0")) >= (
        2,
        0,
        0,
    ) and _parse_version_tuple(getattr(bw2data, "__version__", "0")) >= (4, 0, 0)


def _get_bw25_demand_key(ds, lca):
    """Return a product-node integer id usable as BW2.5 LCI demand key."""
    if ds.id in lca.dicts.product:
        return ds.id

    for exc in ds.production():
        candidate = None
        if getattr(exc, "input", None) is not None:
            candidate = getattr(exc.input, "id", None)
        if candidate is None:
            candidate = exc.get("input")
        if isinstance(candidate, int) and candidate in lca.dicts.product:
            return candidate

    raise KeyError(
        f"Could not find BW2.5 product demand key for dataset '{ds}'. "
        "Neither ds.id nor production exchange input ids are in lca.dicts.product."
    )


def comparative_analysis(
    ndb: NewDatabase = None,
    indicators: list = None,
    databases: list = None,
    limit: int = 1000,
    direct_only=False,
) -> pd.DataFrame:
    """
    A function that does an LCA of all common datasets in databases
    contained in scenarios, and compare them with the original
    ecoinvent database. It returns a pandas dataframe with:
    * dataset name
    * dataset reference product
    * dataset location
    * HS code (when available)
    * CPC code (when available)
    and the score for each indicator for each scenario database, plus
    the score of the corresponding dataset from the original database.

    :param ndb: instance of NewDatabase
    :param indicators: list of indicators to calculate
    :param databases: list of databases to calculate if not from scenarios
    :param limit: limit the number of datasets to process
    """

    if indicators is None:
        indicators = [
            m
            for m in bw2data.methods
            if "ef v3.1" in str(m).lower()
            and "lt" not in str(m).lower()
            and "en15804" in str(m).lower()
        ]

    if len(indicators) == 0:
        raise ValueError("No indicators to calculate.")

    if databases:
        databases = [bw2data.Database(db) for db in databases]
    else:
        original_db = bw2data.Database(ndb.source)
        new_databases = [
            bw2data.Database(s["database name"])
            for s in ndb.scenarios
            if s["database name"]
        ]
        databases = [original_db] + new_databases

    scores = {}

    common_datasets = [
        (
            ds["name"],
            ds["reference product"],
            ds["location"],
        )
        for ds in databases[0]
    ]

    if limit is None:
        limit = len(common_datasets)

    if len(common_datasets) > limit:
        common_datasets = common_datasets[:limit]

    use_bw25_indexing = _is_brightway25_stack()

    for db in databases:

        lca = bw2calc.LCA({db.random(): 1}, method=indicators[0])
        lca.lci(factorize=True)
        lca.lcia()

        method_matrices = [lca.characterization_matrix.copy()]

        for indicator in indicators[1:]:
            lca.switch_method(indicator)
            method_matrices.append(lca.characterization_matrix.copy())

        datasets = list(db)
        for x, ds in tqdm(
            enumerate(datasets), total=len(datasets), desc=f"Processing {db.name}"
        ):
            key = (
                ds["name"],
                ds["reference product"],
                ds["location"],
            )

            if key not in common_datasets:
                continue

            # iterate through the "classifications" list
            # which contains tuples, and fetch the second item
            cpc = [
                item[1].split(":")[-1]
                for item in ds.get("classifications", [])
                if item[0] == "CPC"
            ]
            if cpc:
                cpc = cpc[0]

            isic = [
                item[1].split(":")[-1]
                for item in ds.get("classifications", [])
                if item[0] == "ISIC rev.4 ecoinvent"
            ]
            if isic:
                isic = isic[0]
            else:
                isic = None

            if cpc:
                key += (cpc,)
            else:
                key += (None,)

            if isic:
                key += (isic,)
            else:
                key += (None,)

            if key not in scores:
                scores[key] = {}

            amount = 1
            for e in ds.production():
                amount = e["amount"]

            if use_bw25_indexing:
                lca.lci(demand={_get_bw25_demand_key(ds, lca): amount})
            else:
                lca.redo_lci({ds: amount})
            for j, characterization_matrix in enumerate(method_matrices):

                if indicators[j] not in scores[key]:
                    scores[key][indicators[j]] = {}

                if direct_only:
                    if use_bw25_indexing:
                        index = lca.dicts.activity[ds.id]
                    else:
                        index = lca.activity_dict[ds.key]
                    scores[key][indicators[j]][db.name] = (
                        characterization_matrix * lca.inventory
                    )[:, index].sum()
                else:
                    scores[key][indicators[j]][db.name] = (
                        characterization_matrix * lca.inventory
                    ).sum()
            sys.stdout.flush()

    # Convert nested dictionary to DataFrame
    records = []
    for key, result in scores.items():

        name, ref_prod, loc, cpc, isic = key

        base = {
            "name": name,
            "reference product": ref_prod,
            "location": loc,
            "ISIC": isic,
            "CPC": cpc,
        }

        for method, db_scores in result.items():
            row = base.copy()
            row["indicator"] = method
            row.update(db_scores)
            records.append(row)

    return pd.DataFrame(records)


def interconnection_analysis(
    database: bw2data.Database,
):
    """
    A function that list all datasets in the database
    and counts the numbers of datasets each
    gives inputs to.
    """

    counts = {}

    for ds in database:
        key = (ds["name"], ds["reference product"], ds["location"])
        if key not in counts:
            counts[key] = 0

    for ds in database:
        for exc in ds.technosphere():
            key = (exc["name"], exc["product"], exc["location"])
            if key in counts:
                counts[key] += 1

    # Convert counts to DataFrame
    records = []
    for key, count in counts.items():
        name, ref_prod, loc = key
        records.append(
            {
                "name": name,
                "reference product": ref_prod,
                "location": loc,
                "count": count,
            }
        )
    return pd.DataFrame(records).sort_values(by="count", ascending=False)
