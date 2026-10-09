"""Generate a public synthetic package coupling stock, manufacture and retirement.

Use a premise environment with this worktree on PYTHONPATH. The fixture tests
end-year stocks, common amortisation, scoped capital copies and a signed credit.
It supplies no empirical lifetime or technology assumption for a pilot.
"""

from copy import deepcopy
import json
import os
from pathlib import Path

from premise.export import Export, biosphere_flows_dictionary
from premise.inventory_store import CompactInventoryStore
from premise.new_database import NewDatabase
from premise.stock_cohorts import (
    SurvivalLaw,
    evolve_stock,
    retirement_record,
    service_weights,
)
from premise.stock_lifecycle import split_embedded_lifecycle
from premise.stock_vintage import IDENTITY_FIELDS, StockVintageExport
from premise.trails import TrailsDataPackage
from premise.utils import load_database


def generate(directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    bio_key, bio_code = next(
        (k, c)
        for k, c in biosphere_flows_dictionary("3.12").items()
        if k[0] == "Carbon dioxide, fossil" and k[1:3] == ("air", "unspecified")
    )

    def activity(name, tech=None, co2=0.0):
        d = dict(zip(IDENTITY_FIELDS, (name, name, "unit", "GLO")))
        d.update(database="synthetic", code=name, exchanges=[])
        for supplier, amount, kind in [(name, 1, "production")] + [
            (n, v, "technosphere") for n, v in (tech or {}).items()
        ]:
            d["exchanges"].append(
                {
                    "name": supplier,
                    "product": supplier,
                    "unit": "unit",
                    "location": "GLO",
                    "amount": amount,
                    "type": kind,
                }
            )
        if co2:
            d["exchanges"].append(
                {
                    "name": bio_key[0],
                    "categories": bio_key[1:3],
                    "unit": bio_key[3],
                    "input": ("biosphere3", bio_code),
                    "amount": co2,
                    "type": "biosphere",
                }
            )
        return d

    original = [
        activity("synthetic lifecycle service", {"capital market": 0.1}, 0.3),
        activity("capital market", {"asset manufacture": 1}),
        activity("asset manufacture", {"recycling credit": -1}, 2),
        activity("recycling credit", co2=0.5),
    ]
    rewritten, audit = split_embedded_lifecycle(
        original,
        caller=original[0],
        paths=[original[1:]],
        context_id="lifecycle-fixture",
        uncertainty_mode="deterministic",
    )
    projection = evolve_stock(
        {2005: 3, 2015: 1},
        2020,
        {2021: 5, 2022: 6},
        SurvivalLaw("fixed", 20),
        mode="stock_target",
    )
    manufacture, retirement = [], []
    for year, stock in projection.stocks.items():
        weights = service_weights(stock)["weights"]
        manufacture.append(
            {
                "service_year": year,
                "event_years": list(weights),
                "weights": list(weights.values()),
            }
        )
        retirement.append(retirement_record(projection, year, weights))
    zero = [
        {"service_year": y, "event_years": [y], "weights": [1.0]}
        for y in range(2005, 2043)
    ]
    profiles, bindings = [], []

    def bind(profile_id, caller, supplier, role, years):
        profiles.append(
            {
                "id": profile_id,
                "event_role": role,
                "allocation_basis": "common_amortisation",
                "asset_unit": supplier["unit"],
                "service_unit": caller["unit"],
                "provenance": {
                    "evidence_tier": "synthetic",
                    "lifetime_years": 20,
                    "survival_family": "fixed",
                    "year_convention": "end_year",
                    "exchange_total_preserved": True,
                },
                "years": years,
            }
        )
        bindings.append(
            {"profile_id": profile_id, "caller": caller, "supplier": supplier}
        )

    bind(
        "manufacture",
        audit["caller"],
        audit["capital_root"],
        "existing_asset_service",
        manufacture,
    )
    for i, lifted in enumerate(audit["lifted_exchanges"]):
        bind(
            f"retirement-{i}",
            lifted["caller"],
            lifted["supplier"],
            "lifecycle_service",
            retirement,
        )
    for i, binding in enumerate(audit["zero_shift_bindings"]):
        bind(
            f"wrapper-{i}",
            binding["caller"],
            binding["supplier"],
            "new_asset_component",
            zero,
        )
    payload = {
        "schema_version": 1,
        "inventory_context": {
            "source_database": "synthetic",
            "source_version": "3.12",
            "system_model": "cutoff",
            "model": "test",
            "pathway": "lifecycle",
        },
        "profiles": profiles,
        "bindings": bindings,
    }
    database = NewDatabase.__new__(NewDatabase)
    database._inventory_api_active = True
    database.scenarios = [
        {
            "model": "test",
            "pathway": "lifecycle",
            "year": year,
            "_inventory_store": CompactInventoryStore(deepcopy(rewritten)),
        }
        for year in (2020, 2022)
    ]
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.datapackage, obj.stock_vintage_export = database, StockVintageExport(payload)
    obj.scenario_names = ["test - lifecycle"]
    previous = Path.cwd()
    try:
        os.chdir(directory)
        obj.add_temporal_distributions()
        for scenario in database.scenarios:
            loaded = load_database(scenario, [])
            Export(
                scenario=loaded,
                filepath=directory
                / "trails_temp/inventories/test/lifecycle"
                / str(scenario["year"]),
                version="3.12",
            ).export_db_to_matrices()
        obj._build_datapackage("synthetic_lifecycle")
    finally:
        os.chdir(previous)
    expected = {
        "scope": "synthetic",
        "caller": audit["caller"],
        "original_caller": audit["original_caller"],
        "manufacture": manufacture,
        "retirement": retirement,
        "stock_balances": projection.balances,
        "co2_per_service": 0.45,
        "capital_per_service": 0.1,
        "recycling_credit_per_service": -0.1,
        "rewrite": audit,
    }
    (directory / "expectations.json").write_text(json.dumps(expected, indent=2) + "\n")
    return expected


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    generate(parser.parse_args().directory)
