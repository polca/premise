"""Count effective construction timing, including legacy fallbacks.

Read licensed ecoinvent inventories and optional premise workbooks without
modifying them. Counts are exchange occurrences, not impact-weighted coverage
or a census of all infrastructure. Only metadata and aggregate counts are saved.
"""

from __future__ import annotations

import argparse
import copy
import json
from collections import Counter

import bw2data as bd

from dev.audit_trails_temporal_selectors import workbook_relationships
from premise.core.filesystem_constants import DATA_DIR
from premise.temporal_legacy import compile_legacy_rows
from premise.temporal_specs import TemporalRegistry
from premise.trails import TrailsDataPackage


def gap_relationship_category(caller, supplier):
    """Independent candidate scan for the reviewed asset gaps."""
    name = caller.get("name", "").casefold()
    product = supplier.get("reference product", supplier.get("product", "")).casefold()
    supplier_name = supplier.get("name", "").casefold()
    unit = supplier.get("unit", "").casefold()
    if (
        caller.get("reference product") == "electricity, low voltage"
        and caller.get("unit") == "kilowatt hour"
        and product == "distribution network, electricity, low voltage"
        and unit == "kilometer"
    ):
        return "low_voltage_network"
    if (
        name.startswith("electricity supply, from stationary battery")
        and supplier_name.startswith("market for battery capacity, stationary")
        and unit == "kilowatt hour"
    ):
        return "stationary_battery_portfolio"
    if (
        name.startswith(
            (
                "transport, passenger bus,",
                "transport, passenger car,",
                "transport, freight, lorry,",
            )
        )
        and supplier_name == "ev charger, level 3, plugin, 200 kw"
        and unit == "unit"
    ):
        return "direct_service_plugin_charger"
    if (
        name.startswith("transport, passenger bus,")
        and supplier_name == "ev charger, level 3, with pantograph, 450 kw"
        and unit == "unit"
    ):
        return "direct_service_pantograph_charger"
    if (
        name.startswith("lorry production,")
        and supplier_name == "ev charger, level 3, plugin, 200 kw"
        and unit == "unit"
    ):
        return "allocated_new_truck_charger"
    if (
        product in {"photovoltaic cell factory", "metal coating facility"}
        and unit == "unit"
        and not name.startswith(("market", "maintenance", "treatment"))
        and "construction" not in name
    ):
        return "factory_building"
    return None


def count_basis(records, relationships, registry, stocks, version):
    context = {"ecoinvent_version": version}
    counts = Counter()
    legacy_pairs = Counter()
    profile_counts = Counter()
    caller_cache = {}
    gap_counts = Counter()
    gap_misses = []
    gap_extras = []
    gap_profiles = {
        "direct_service_plugin_charger": "bnetza-de-dc-plugin-charger-150-250kw-vintage-2026",
        "direct_service_pantograph_charger": "bernmobil-ch-pantograph-site-vintage-2026",
        "allocated_new_truck_charger": "reviewed-truck-allocated-charger-construction-event",
        "low_voltage_network": "sigeif-fr-low-voltage-network-vintage-2023",
        "stationary_battery_portfolio": "eia-us-stationary-battery-energy-vintage-2025",
        "factory_building": "desnz-gb-factory-building-vintage-2023",
    }
    for caller, exchanges in relationships:
        caller_key = (
            caller["name"],
            caller.get("reference product"),
            caller.get("location"),
            caller.get("unit"),
            repr(caller.get("classifications")),
        )
        if caller_key not in caller_cache:
            local = copy.copy(registry)
            local.assignments = registry.matching_caller_assignments(
                caller, target="supplier", context=context
            )
            caller_cache[caller_key] = local
        local = caller_cache[caller_key]
        for exchange in exchanges:
            if exchange.get("type", "technosphere") != "technosphere":
                continue
            supplier = records.get(tuple(exchange.get("input", ())), {})
            name = exchange.get("name") or supplier.get("name", "")
            product = (
                exchange.get("product")
                or exchange.get("reference product")
                or supplier.get("reference product", "")
            )
            params = stocks.get((name, product))
            record = {
                **supplier,
                **exchange,
                "name": name,
                "reference product": product,
                "location": supplier.get("location", exchange.get("location", "")),
            }
            category = gap_relationship_category(caller, record)
            if category:
                gap_counts[category] += 1
            if params is None and not local.assignments and category is None:
                continue
            resolution = (
                local.resolve(
                    record, target="supplier", context=context, caller_record=caller
                )
                if local.assignments
                else None
            )
            if resolution is not None:
                profile = registry.profiles[resolution.profile_id]
                basis = (
                    "reviewed_fleet_age"
                    if profile["reference_event"] == "current_service"
                    else "reviewed_construction_event"
                )
                counts[basis] += 1
                profile_counts[resolution.profile_id] += 1
            elif params is not None:
                counts[
                    (
                        "legacy_parametric"
                        if params["temporal_distribution"] != 6
                        else "legacy_discrete"
                    )
                ] += 1
                legacy_pairs[(caller["name"], name, product)] += 1
            if category and (
                resolution is None or resolution.profile_id != gap_profiles[category]
            ):
                gap_misses.append(
                    {"caller": caller["name"], "supplier": name, "category": category}
                )
            elif (
                category is None
                and resolution is not None
                and resolution.profile_id in gap_profiles.values()
            ):
                gap_extras.append(
                    {
                        "caller": caller["name"],
                        "supplier": name,
                        "profile": resolution.profile_id,
                    }
                )
    return {
        "counts": dict(counts),
        "reviewed_profile_matches": dict(profile_counts),
        "gap_candidates_by_category": dict(gap_counts),
        "gap_misses": gap_misses,
        "gap_extras": gap_extras,
        "remaining_legacy_relationships": [
            {"caller": c, "supplier": s, "product": p, "count": n}
            for (c, s, p), n in legacy_pairs.most_common()
        ],
    }


def main():
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--versions", nargs="+", default=["3.12"])
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    directory = DATA_DIR / "trails" / "temporal"
    registry = TemporalRegistry.from_directory(directory)
    loader = TrailsDataPackage.__new__(TrailsDataPackage)
    stocks = loader._load_temporal_specs(compile_legacy_rows(directory))[0]
    workbooks = workbook_relationships(
        [
            DATA_DIR / "additional_inventories" / name
            for name in (
                "lci-buses.xlsx",
                "lci-trucks.xlsx",
                "lci-pass_cars.xlsx",
                "lci-battery-capacity.xlsx",
            )
        ]
    )
    reports = []
    for version in args.versions:
        project = f"ecoinvent-{version}-cutoff"
        bd.projects.set_current(project)
        if project not in bd.databases:
            raise ValueError(f"Missing database {project}.")
        records = bd.Database(project).load()
        base = count_basis(
            records,
            ((r, r["exchanges"]) for r in records.values()),
            registry,
            stocks,
            version,
        )
        additional = count_basis(
            {},
            ((c, e) for _, c, e in workbooks),
            registry,
            stocks,
            version,
        )
        report = {
            "version": version,
            "project": project,
            "database": project,
            "base_ecoinvent": base,
            "additional_inventories": additional,
            "additional_inventory_files": sorted({name for name, _, _ in workbooks}),
        }
        reports.append(report)
        print(
            version,
            "base",
            base["counts"],
            "additional inventories",
            additional["counts"],
            flush=True,
        )
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(reports, indent=2) + "\n")
    if any(
        report[scope][key]
        for report in reports
        for scope in ("base_ecoinvent", "additional_inventories")
        for key in ("gap_misses", "gap_extras")
    ):
        raise SystemExit(
            "Uncovered or unintended gap-profile relationships; inspect JSON report."
        )


if __name__ == "__main__":
    main()
