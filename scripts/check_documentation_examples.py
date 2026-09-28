"""Execute self-contained documentation examples in isolated temporary storage."""

from pathlib import Path
import os
import runpy
import tempfile


def main():
    repo = Path(__file__).resolve().parents[1]
    # Never switch or modify the reader's Brightway project for these examples.
    with tempfile.TemporaryDirectory(prefix="premise-doc-examples-") as directory:
        previous = os.environ.get("BRIGHTWAY2_DIR")
        os.environ["BRIGHTWAY2_DIR"] = directory
        try:
            for name in (
                "inspect_inventory",
                "interpret_changes",
                "validation_findings",
            ):
                runpy.run_path(
                    str(repo / f"docs/examples/{name}.py"), run_name="__main__"
                )
            preflight = runpy.run_path(str(repo / "docs/examples/preflight.py"))[
                "validate_inputs"
            ]
            settings = dict(
                PREMISE_BW_PROJECT="existing",
                PREMISE_SOURCE_DB="source",
                PREMISE_BIOSPHERE="biosphere",
                PREMISE_KEY="example-only",
            )
            assert preflight(settings, {"existing"}, {"source", "biosphere"})
            cases = [
                ({}, {"existing"}, {"source", "biosphere"}),
                (settings, set(), {"source", "biosphere"}),
                (settings, {"existing"}, {"source"}),
                (settings, {"existing"}, {"biosphere"}),
                (
                    settings,
                    {"existing"},
                    {"source", "biosphere", "docs-example-remind-SSP2-NPi-2025"},
                ),
            ]
            for args in cases:
                try:
                    preflight(*args)
                except ValueError:
                    pass
                else:
                    raise AssertionError("Invalid preflight configuration passed")
            # Exercise actual emissions implementation, including no-increase behaviour.
            import numpy as np
            from premise.emissions import Emissions

            for factor, expected in (
                (0.8, 8),
                (1, 10),
                (1.2, 10),
                (0, 10),
                (np.nan, 10),
            ):
                model = object.__new__(Emissions)
                model.ei_pollutants = {"Sulfur dioxide": "SO2"}
                model.find_gains_emissions_change = lambda **kw: factor
                dataset = {
                    "location": "CH",
                    "exchanges": [
                        {"type": "biosphere", "name": "Sulfur dioxide", "amount": 10}
                    ],
                }
                model.update_pollutant_emissions(dataset, "example", ["CH"])
                assert dataset["exchanges"][0]["amount"] == expected
            print("Preflight failure cases and emissions examples passed.")
        finally:
            if previous is None:
                os.environ.pop("BRIGHTWAY2_DIR", None)
            else:
                os.environ["BRIGHTWAY2_DIR"] = previous


if __name__ == "__main__":
    main()
