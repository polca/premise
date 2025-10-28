import pandas as pd
import bw2data, bw2calc
from tqdm import tqdm
import sys

from .new_database import NewDatabase


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

            index = lca.activity_dict[ds.key]

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

            lca.redo_lci({ds: amount})
            for j, characterization_matrix in enumerate(method_matrices):

                if indicators[j] not in scores[key]:
                    scores[key][indicators[j]] = {}

                if direct_only:
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
