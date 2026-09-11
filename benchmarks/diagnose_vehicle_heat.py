"""Reproduce vehicle carbon and heat-supplier failures without database export.

Set PREMISE_KEY or IAM_FILES_KEY. Outputs contain inventory data and belong in
the ignored export directory. Instrumentation observes existing transformations;
validation exceptions are recorded, never suppressed inside the build.
"""

import argparse
import copy
import json
import math
import os
from pathlib import Path
import sys
from contextlib import ExitStack
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("vehicles", "heat", "both"), default="both")
    parser.add_argument("--full-update", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
    if not key:
        raise RuntimeError("Set PREMISE_KEY or IAM_FILES_KEY")
    args.output_dir.mkdir(parents=True, exist_ok=False)

    import bw2data as bd
    from premise import NewDatabase
    from premise.car_energy import car_class, fuel_balance
    from premise.fuels.liquid_fuels import SyntheticFuelsMixin
    from premise.heat import Heat, SECONDARY_MARKET
    from premise.transport import Transport
    from premise.validation import CarValidation
    from premise.validation_framework import _plain

    bd.projects.set_current("ecoinvent-3.12-cutoff")

    def balance(dataset):
        energy, expected, fuels, production = fuel_balance(dataset)
        emissions = [
            copy.deepcopy(exchange)
            for exchange in dataset["exchanges"]
            if exchange.get("type") == "biosphere"
            and exchange.get("name", "").startswith("Carbon dioxide")
            and exchange.get("categories", [None])[0] == "air"
        ]
        actual = sum(exchange["amount"] for exchange in emissions) / production
        return {
            "name": dataset["name"],
            "location": dataset["location"],
            "code": dataset.get("code"),
            "energy_mj_per_km": energy,
            "expected_co2_kg_per_km": expected,
            "actual_co2_kg_per_km": actual,
            "co2_ratio": actual / expected,
            "co2_check_passes": math.isclose(actual, expected, rel_tol=0.1),
            "fuels": copy.deepcopy(fuels),
            "emissions": emissions,
            "log_parameters": copy.deepcopy(dataset.get("log parameters", {})),
        }

    def car_balances(database):
        result = []
        for dataset in database:
            if car_class(dataset.get("name", "")) is None:
                continue
            try:
                result.append(balance(dataset))
            except (ValueError, KeyError, TypeError):
                continue
        return result

    cases = (
        [
            ("vehicles", "SSP2-L", 2050, ["fuels", "cars"]),
            ("heat", "SSP2-M", 2020, ["heat"]),
        ]
        if args.case == "both"
        else (
            [("vehicles", "SSP2-L", 2050, ["fuels", "cars"])]
            if args.case == "vehicles"
            else [("heat", "SSP2-M", 2020, ["heat"])]
        )
    )
    for name, pathway, year, sectors in cases:
        evidence = {
            "case": name,
            "pathway": pathway,
            "year": year,
            "sectors": None if args.full_update else sectors,
            "events": [],
        }
        directory = args.output_dir / name
        directory.mkdir()

        def save():
            (directory / "evidence.json").write_text(
                json.dumps(_plain(evidence), indent=2)
            )

        original_fuel_update = SyntheticFuelsMixin.update_fuel_carbon_dioxide_emissions

        def fuel_update(instance, *positional, **keywords):
            before = car_balances(instance.database)
            result = original_fuel_update(instance, *positional, **keywords)
            after = car_balances(instance.database)
            changes = [
                (a, b)
                for a, b in zip(before, after)
                if a["emissions"] != b["emissions"]
            ]
            evidence["events"].append(
                {
                    "stage": "fuel carbon update",
                    "markets": keywords.get("market_names"),
                    "changes": changes,
                }
            )
            save()
            return result

        original_efficiency = Transport.adjust_transport_efficiency

        def efficiency(instance, dataset, *positional, **keywords):
            selected = (
                dataset["location"] == "BRA" and car_class(dataset["name"]) is not None
            )
            before = balance(dataset) if selected else None
            result = original_efficiency(instance, dataset, *positional, **keywords)
            if selected:
                evidence["events"].append(
                    {
                        "stage": "vehicle efficiency",
                        "before": before,
                        "after": balance(dataset),
                    }
                )
            return result

        original_validation = CarValidation.run_checks

        def validation(instance):
            rows = [
                row
                for row in car_balances(instance.database)
                if row["location"] in instance.regions
            ]
            evidence["car_validation"] = {
                "checked": len(rows),
                "failures": [row for row in rows if not row["co2_check_passes"]],
                "examples": [row for row in rows if row["location"] == "BRA"],
            }
            save()
            return original_validation(instance)

        original_market = Heat.create_heat_market

        def heat_market(instance, array, layer, market):
            selected = instance._select_year(array, instance.year)
            evidence["events"].append(
                {
                    "stage": "heat market",
                    "layer": layer,
                    "iam_values": selected.to_dict(),
                    "secondary_market_locations": [
                        dataset["location"]
                        for dataset in instance.database
                        if dataset["name"] == SECONDARY_MARKET["name"]
                    ],
                }
            )
            save()
            result = original_market(instance, array, layer, market)
            evidence["heat_diagnostics"] = copy.deepcopy(instance.diagnostics)
            save()
            return result

        try:
            with ExitStack() as patches:
                patches.enter_context(
                    patch.object(
                        SyntheticFuelsMixin,
                        "update_fuel_carbon_dioxide_emissions",
                        fuel_update,
                    )
                )
                patches.enter_context(
                    patch.object(Transport, "adjust_transport_efficiency", efficiency)
                )
                patches.enter_context(
                    patch.object(CarValidation, "run_checks", validation)
                )
                patches.enter_context(
                    patch.object(Heat, "create_heat_market", heat_market)
                )
                ndb = NewDatabase(
                    scenarios=[{"model": "image", "pathway": pathway, "year": year}],
                    source_db="ecoinvent-3.12-cutoff",
                    source_version="3.12",
                    biosphere_name="ecoinvent-3.12-biosphere",
                    key=key,
                    inventory_backend="legacy",
                    generate_reports=False,
                    keep_imports_uncertainty=True,
                    keep_source_db_uncertainty=False,
                    quiet=True,
                )
                ndb.update(None if args.full_update else sectors)
            evidence["status"] = "passed"
        except Exception as error:
            evidence["status"] = "failed"
            evidence["error"] = {"type": type(error).__name__, "message": str(error)}
            if hasattr(error, "report"):
                evidence["validation_report"] = error.report.to_dict()
        finally:
            save()
            print(name, evidence["status"], evidence.get("error", {}), flush=True)


if __name__ == "__main__":
    main()
