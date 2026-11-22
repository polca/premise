"""Mark where TRAILS should keep the construction year already assigned."""

import csv
from collections import defaultdict
from fnmatch import fnmatchcase

import yaml


def _matches(name, patterns):
    return any(fnmatchcase(name.casefold(), pattern) for pattern in patterns)


def _pass_through(caller, supplier, rules):
    """Recognize the same asset at a market or a reviewed construction input."""
    name, product, unit, _ = caller
    target, target_product, target_unit, _ = supplier
    # A retirement supplier can share the retired asset's product label.
    if target.casefold().startswith(
        ("treatment ", "dismantling", "decommission", "maintenance", "replacement")
    ):
        return False
    if name.casefold().startswith(("market ", "market group ")) and (product, unit) == (
        target_product,
        target_unit,
    ):
        return True
    return any(
        _matches(name, rule["callers"]) and _matches(target, rule["suppliers"])
        for rule in rules
    )


def _zero_stock_pairs(activities, sources, scenario, zero_fleets):
    """Locate construction links backed by an explicitly empty IAM fleet."""
    if not zero_fleets:
        return set()
    callers = defaultdict(list)
    suppliers = {activity: index for index, activity in activities.items()}
    for index, (name, product, _, location) in activities.items():
        callers[name, product, location].append(index)
    pairs = set()
    for (model_pathway, caller, supplier), fleets in sources.items():
        if model_pathway != scenario[:2] or len(fleets) != 1:
            continue
        if not fleets.issubset(zero_fleets):
            continue
        buyers = callers.get(caller, ())
        supplier_index = suppliers.get(supplier)
        if (
            len(buyers) == 1
            and supplier_index is not None
            and buyers[0] != supplier_index
        ):
            pairs.add((buyers[0], supplier_index))
    return pairs


def mark_construction_routes(root, rules_path, zero_stock=()):
    """Annotate exported A matrices without changing amounts or baseline timing.

    Roles are carried by the exchange, not by the supplier. A shared market can
    therefore still use its original timing for a demand that has no build year.
    """
    report = root / "temporal_selector_matches.csv"
    if not report.is_file():
        return
    dated = defaultdict(set)
    sources = defaultdict(set)
    with report.open(newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            if not row.get("iam_vintage_asset"):
                continue
            scenario = tuple(row["scenario"].split(" | "))
            caller = (
                row["calling_dataset_name"],
                row["calling_dataset_reference_product"],
                row["calling_dataset_location"],
            )
            supplier = (
                row["name"],
                row["reference_product"],
                row["unit"],
                row["location"],
            )
            dated[scenario].add((caller, supplier))
            if region := row.get("iam_vintage_region"):
                sources[scenario[:2], caller, supplier].add(
                    (row["iam_vintage_asset"], region)
                )
    if not dated:
        return

    with rules_path.open(encoding="utf-8") as stream:
        rules = yaml.safe_load(stream).get("construction_pass_through", [])

    zeros_by_year = defaultdict(set)
    for asset, region, year in zero_stock:
        zeros_by_year[str(year)].add((asset, region))

    for path in sorted((root / "inventories").glob("*/*/*/A_matrix.csv")):
        scenario = path.parts[-4:-1]
        with path.with_name("A_matrix_index.csv").open(
            newline="", encoding="utf-8"
        ) as index:
            activities = {
                row["index"]: (
                    row["name"],
                    row["reference product"],
                    row["unit"],
                    row["location"],
                )
                for row in csv.DictReader(index, delimiter=";")
            }
        dated_pairs = dated.get(scenario, set())
        zero_pairs = _zero_stock_pairs(
            activities, sources, scenario, zeros_by_year.get(scenario[-1])
        )
        missing_zero_pairs = zero_pairs.copy()
        # Most activities are neither a market nor a reviewed asset wrapper.
        forwarding = {
            i
            for i, activity in activities.items()
            if activity[0].casefold().startswith("market ")
            or any(_matches(activity[0], rule["callers"]) for rule in rules)
        }
        tmp = path.with_suffix(".route.tmp")
        with (
            path.open(newline="", encoding="utf-8") as src,
            tmp.open("w", newline="", encoding="utf-8") as dest,
        ):
            reader = csv.DictReader(src, delimiter=";")
            columns = list(reader.fieldnames)
            if "construction_timing" not in columns:
                columns.append("construction_timing")
            writer = csv.DictWriter(dest, fieldnames=columns, delimiter=";")
            writer.writeheader()
            for row in reader:
                i, j = row["index of activity"], row["index of product"]
                role = ""
                if row["flip"] == "1" and i != j:
                    caller, supplier = activities[i], activities[j]
                    name, product, _, location = caller
                    missing_zero_pairs.discard((i, j))
                    if (
                        row.get("temporal_distribution") == "6"
                        and ((name, product, location), supplier) in dated_pairs
                    ):
                        role = "dated"
                    elif (i, j) in zero_pairs:
                        role = "zero_fleet"
                    elif i in forwarding and _pass_through(caller, supplier, rules):
                        role = "pass_through"
                row["construction_timing"] = role
                writer.writerow(row)
            # Global activity indices also include activities absent in this snapshot.
            neutral_flags = {
                name: 0 for name in ("uncertainty type", "negative") if name in columns
            }
            for i, j in sorted(missing_zero_pairs):
                writer.writerow(
                    {
                        **neutral_flags,
                        "index of activity": i,
                        "index of product": j,
                        "value": 0,
                        "flip": 1,
                        "construction_timing": "zero_fleet",
                    }
                )
        tmp.replace(path)
