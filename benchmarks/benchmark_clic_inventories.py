"""Compare workbook preparation with a selected CLIC + retained-workbook graph.

Uses an existing, trusted local source cache; never writes a Brightway database.
Only timing and count summaries are exported. Licensed records stay in memory
and in the user-selected private cache directory.
"""

from __future__ import annotations

import argparse
import copy
import json
import pickle
import time
from collections import defaultdict
from pathlib import Path
from unittest.mock import patch

from premise.database import new_database as database_module
from premise.database.new_database import NewDatabase
from premise.inventory import inventory_imports
from premise.inventory.clic import ClicInventoryProvider
from premise.inventory.clic_hybrid import (
    import_retained_inventories,
    plan_hybrid_inventories,
)
from premise.inventory.clic_requirements import registry


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-cache", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--version", default="3.12")
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument("--sectors", nargs="+")
    parser.add_argument("--allow-documented-proxies", action="store_true")
    args = parser.parse_args()
    source = pickle.loads(args.source_cache.read_bytes())
    owner = NewDatabase.__new__(NewDatabase)
    owner.version, owner.system_model = args.version, args.system_model
    owner.keep_imports_uncertainty = False
    owner._database = copy.deepcopy(source)
    timings = defaultdict(float)

    def timed(name, function):
        def wrapped(*a, **kw):
            start = time.perf_counter()
            try:
                return function(*a, **kw)
            finally:
                timings[name] += time.perf_counter() - start

        return wrapped

    methods = (
        "lower_case_technosphere_exchanges",
        "add_biosphere_links",
        "fill_dataset_data_gaps",
        "add_product_field_to_exchanges",
        "check_units",
        "correct_keys",
        "add_classifications",
        "check_for_already_existing_datasets",
    )
    from contextlib import ExitStack

    with ExitStack() as stack:
        for name in methods:
            stack.enter_context(
                patch.object(
                    inventory_imports.BaseInventoryImport,
                    name,
                    timed(name, getattr(inventory_imports.BaseInventoryImport, name)),
                )
            )
        stack.enter_context(
            patch.object(
                database_module,
                "_extract_default_inventory_importers",
                timed(
                    "excel_extraction",
                    database_module._extract_default_inventory_importers,
                ),
            )
        )
        start = time.perf_counter()
        legacy = owner._NewDatabase__import_inventories()
        legacy_seconds = time.perf_counter() - start
        legacy_timings = dict(timings)
        timings.clear()
        sectors = args.sectors or list(registry()["sector_mappings"]) + [
            "battery",
            "metals",
            "mining",
        ]
        plan = plan_hybrid_inventories(
            sectors,
            [],
            owner._default_inventory_sources(),
            version=args.version,
            system_model=args.system_model,
        )
        runs = []
        for label in ("first", "repeat"):
            start = time.perf_counter()
            provider = ClicInventoryProvider(
                options={
                    "archive_path": args.archive,
                    "cache_dir": args.cache_dir,
                    "offline": True,
                    "allow_documented_proxies": args.allow_documented_proxies,
                }
            )
            records = provider.prepare(
                plan.clic_roots,
                source,
                version=args.version,
                system_model=args.system_model,
                keep_uncertainty=False,
            )
            clic_seconds = time.perf_counter() - start
            retained_start = time.perf_counter()
            retained, workbooks, _ = import_retained_inventories(
                plan,
                [*source, *records],
                records,
                version=args.version,
                system_model=args.system_model,
                keep_uncertainty=False,
                cache_dir=args.cache_dir / "retained",
                cache_signature={
                    "source_cache_sha256": __import__("hashlib")
                    .sha256(args.source_cache.read_bytes())
                    .hexdigest(),
                    "resolution": provider.manifest["resolution_fingerprint"],
                    "adapter": provider.manifest["adapter_fingerprint"],
                    "engine": provider.manifest["brightpath_version"],
                    "resources": provider.manifest["migration_resource_fingerprint"],
                },
            )
            runs.append(
                {
                    "run": label,
                    "clic_seconds": clic_seconds,
                    "retained_seconds": time.perf_counter() - retained_start,
                    "total_seconds": time.perf_counter() - start,
                    "clic_activities": len(records),
                    "retained_activities": len(retained),
                    "workbooks_read_or_cached": len(workbooks),
                    "cohort_cache_hits": sum(
                        row["cache_hit"] for row in provider.manifest["cohorts"]
                    ),
                    "legacy_cleaning_method_seconds_inclusive": dict(timings),
                }
            )
            timings.clear()
    result = {
        "source_version": args.version,
        "system_model": args.system_model,
        "sectors": sectors,
        "baseline": {
            "seconds": legacy_seconds,
            "activities": len(legacy),
            "workbooks": len(owner._default_inventory_sources()),
            "method_seconds_inclusive": legacy_timings,
        },
        "hybrid": runs,
        "note": "Latest CLIC inventories differ scientifically from bundled workbook versions. "
        "Preparation timings exclude source cleaning, IAM loading, scenario updates, live freshness checks and downloads.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
