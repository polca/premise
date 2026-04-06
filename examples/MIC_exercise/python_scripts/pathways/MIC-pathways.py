#!/usr/bin/env python3
"""
Run pathways analysis on MIC datapackages
"""

import os
import yaml
from pathlib import Path

shared_cache = "/tmp/pathways_shared_cache"
os.makedirs(shared_cache, exist_ok=True)

with open("variables.yaml", "w") as f:
    yaml.dump({"USER_DATA_BASE_DIR": shared_cache}, f)

import time
import sys
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd
import zipfile
from io import StringIO

from pathways import Pathways


def get_geography_mapping(datapackage_path):
    """Determine geography mapping based on model"""
    try:
        with zipfile.ZipFile(datapackage_path, "r") as zip_file:
            # Read scenario data to determine model
            scenario_data = zip_file.read("scenario_data/scenario_data.csv").decode(
                "utf-8"
            )
            df = pd.read_csv(StringIO(scenario_data))

            if "model" not in df.columns:
                raise ValueError("No 'model' column found in scenario_data.csv")

            models = df["model"].unique()
            if len(models) > 1:
                raise ValueError(f"Multiple models found: {list(models)}")

            model = models[0].lower()

            geo_mapping_file = (
                f"/data/user/hahnme_a/MIC_exercise/geo_mapping_{model}.yaml"
            )

            if not Path(geo_mapping_file).exists():
                raise FileNotFoundError(
                    f"Geography mapping not found: {geo_mapping_file}"
                )

            print(f"Using geography mapping: geo_mapping_{model}.yaml")
            return geo_mapping_file, model

    except Exception as e:
        print(f"Error determining geography mapping: {e}")
        raise


def analyze_datapackage(datapackage_path, year, output_suffix=""):
    """Run pathways analysis on a datapackage"""
    start_time = time.time()

    print(f"Initializing Pathways for: {Path(datapackage_path).name}")

    geo_mapping_file, model = get_geography_mapping(datapackage_path)

    # Initialize Pathways
    p = Pathways(
        datapackage=datapackage_path,
        debug=False,
        geography_mapping=geo_mapping_file,
        activities_mapping="/data/user/hahnme_a/MIC_exercise/act_categories_agg.yaml",
        classification_system="ISIC rev.4 ecoinvent",
    )

    # Define LCIA methods
    methods = [
        "EF v3.1 - acidification - accumulated exceedance (AE)",
        # 'IPCC 2021 (incl. biogenic CO2) - climate change: total (incl. biogenic CO2) - global warming potential (GWP100)',
        "IPCC 2021 (incl. biogenic CO2) - climate change: total (incl. biogenic CO2, incl. SLCFs) - global warming potential (GWP100)",
        "EF v3.1 - ecotoxicity: freshwater - comparative toxic unit for ecosystems (CTUe)",
        "EF v3.1 - energy resources: non-renewable - abiotic depletion potential (ADP): fossil fuels",
        "EF v3.1 - eutrophication: freshwater - fraction of nutrients reaching freshwater end compartment (P)",
        "EF v3.1 - eutrophication: marine - fraction of nutrients reaching marine end compartment (N)",
        "EF v3.1 - eutrophication: terrestrial - accumulated exceedance (AE)",
        "EF v3.1 - human toxicity: carcinogenic - comparative toxic unit for human (CTUh)",
        "EF v3.1 - human toxicity: non-carcinogenic - comparative toxic unit for human (CTUh)",
        "ReCiPe 2016 v1.03, endpoint (H) - total: human health - human health",
        "ReCiPe 2016 v1.03, endpoint (H) - total: ecosystem quality - ecosystem quality",
        "EF v3.1 - ionising radiation: human health - human exposure efficiency relative to u235",
        "EF v3.1 - land use - soil quality index",
        "EF v3.1 - material resources: metals/minerals - abiotic depletion potential (ADP): elements (ultimate reserves)",
        "EF v3.1 - ozone depletion - ozone depletion potential (ODP)",
        "EF v3.1 - particulate matter formation - impact on human health",
        "EF v3.1 - photochemical oxidant formation: human health - tropospheric ozone concentration increase",
        "EF v3.1 - water use - user deprivation potential (deprivation-weighted water consumption)",
        ###
        #'ReCiPe 2016 v1.03, midpoint (H) - water use - water consumption potential (WCP)',
        "EN15804+A2 - Indicators describing resource use - net use of fresh water - FW",
        "Inventory results and indicators - resources - total surface occupation",
        "Inventory results and indicators - emissions to air - carbon dioxide, fossil and land use",
        #'Inventory results and indicators - emissions to air - total particulate matter',
        "Crustal Scarcity Indicator 2020 - material resources: metals/minerals - crustal scarcity potential (CSP)",
        "Primary Energy - IAM benchmarking - energy content (LHV)",
        "CO2 emissions - IAM benchmarking - CO2 emissions (kg)",
        ####
        #'RELICS - metals extraction - Copper',
        #'RELICS - metals extraction - Iron',
        #'RELICS - metals extraction - Nickel',
        #'RELICS - metals extraction - Zinc',
        #'RELICS - metals extraction - Lithium',
        #'RELICS - metals extraction - Cobalt',
        #'RELICS - metals extraction - Manganese',
        ### PGMs
        #'RELICS - metals extraction - Platinum',
        #'RELICS - metals extraction - Palladium',
        #'RELICS - metals extraction - Rhodium',
        #'RELICS - metals extraction - Iridium',
        #'RELICS - metals extraction - Ruthenium',
        #
        #'RELICS - metals extraction - Silver',
        ### REEs
        #'RELICS - metals extraction - Lanthanum',
        #'RELICS - metals extraction - Cerium',
        #'RELICS - metals extraction - Praseodymium',
        #'RELICS - metals extraction - Neodymium',
        #'RELICS - metals extraction - Samarium',
        #'RELICS - metals extraction - Europium',
        #'RELICS - metals extraction - Gadolinium',
        #'RELICS - metals extraction - Terbium',
        #'RELICS - metals extraction - Dysprosium',
        #'RELICS - metals extraction - Holmium',
        #'RELICS - metals extraction - Erbium',
        #'RELICS - metals extraction - Thulium',
        #'RELICS - metals extraction - Ytterbium',
        #'RELICS - metals extraction - Scandium',
        #'RELICS - metals extraction - Yttrium',
    ]

    # Add any RELICS methods if available
    methods += [m for m in p.lcia_methods if "RELICS" in m]

    # Define variables
    variables = [
        key
        for key, value in p.mapping.items()
        if key.startswith(("FE - final energy", "SE - cdr"))
    ]

    if model == "remind":
        remind_exclusions = {
            "FE - final energy - Industry - Chemicals - Elec",
            "FE - final energy - Industry - Other - Elec",
        }
        variables = [
            var
            for var in variables
            if not any(var == excl for excl in remind_exclusions)
            and "Non-energy use" not in var
        ]

    variables = [
        var for var in variables if not var.startswith("FE - final energy - CDR")
    ]
    print(f"Found {len(variables)} variables to analyze")

    if len(variables) == 0:
        print("ERROR: No variables found!")
        return False

    # Get scenarios and years
    scenarios = p.scenarios.pathway.values.tolist()
    # years = [2005, 2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050, 2060, 2070, 2080, 2090, 2100]

    print(
        f"Calculating for {len(scenarios)} scenarios, year {year}, {len(methods)} methods"
    )

    # Run calculation
    p.calculate(
        methods=methods,
        scenarios=scenarios,
        # years=years,
        years=[year],
        variables=variables,
        use_distributions=0,
        subshares=False,
        remove_uncertainty=True,
        multiprocessing=False,
        # double_accounting = [['Energy'],['Transport']]
    )

    # Export results
    p.export_results()

    del p
    import gc

    gc.collect()

    elapsed = time.time() - start_time
    print(f"Completed in {elapsed / 60:.1f} minutes")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run pathways analysis on MIC datapackage"
    )
    parser.add_argument(
        "--datapackage", required=True, help="Path to datapackage ZIP file"
    )
    parser.add_argument("--year", type=int, required=True, help="Year to process")
    parser.add_argument("--output-suffix", default="", help="Suffix for output files")

    args = parser.parse_args()

    if not Path(args.datapackage).exists():
        print(f"ERROR: Datapackage not found: {args.datapackage}")
        sys.exit(1)

    try:
        success = analyze_datapackage(args.datapackage, args.year, args.output_suffix)
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
