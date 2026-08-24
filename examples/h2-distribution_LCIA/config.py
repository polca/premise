"""Configuration for the hydrogen-distribution LCIA example.

Project and database names are deliberately selected in the notebook and passed to
``run_analysis``.  Everything in this module is declarative and importing it has no
Brightway side effects.
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"

GENERIC_MARKET_NAME = "market for hydrogen, gaseous, low pressure"
REFERENCE_PRODUCT = "hydrogen, gaseous, low pressure"
UNIT = "kilogram"
FUNCTIONAL_UNIT_KG = 1.0
TARGET_LOCATION = "RER"
SECTOR_LOCATION_FALLBACKS = ("EUR", "WEU")

TOP_N_PROCESSES = 10
RECONCILIATION_RTOL = 1e-7

PREMISE_GWP_METHOD = ("IPCC 2021", "climate change", "GWP 100a, incl. H")
CED_METHODS = (
    (
        "Cumulative Energy Demand (CED)",
        "energy resources: non-renewable",
        "energy content (HHV)",
    ),
    (
        "Cumulative Energy Demand (CED)",
        "energy resources: renewable",
        "energy content (HHV)",
    ),
)
EXCLUDED_METHOD_TERMS = ("no LT", "EN15804")

EXPECTED_SECTOR_MARKETS = {
    "Transport": f"{GENERIC_MARKET_NAME}, for transport",
    "Chemicals": f"{GENERIC_MARKET_NAME}, for chemicals",
    "Steel": f"{GENERIC_MARKET_NAME}, for steel",
    "Cement": f"{GENERIC_MARKET_NAME}, for cement",
    "Heating": f"{GENERIC_MARKET_NAME}, for heating",
    "Other end uses": f"{GENERIC_MARKET_NAME}, for other end uses",
}

SPECIFIC_IMPACT_CATEGORY_ORDER = (
    "acidification",
    "climate change — GWP 100a, incl. H (premise_gwp)",
    "ecotoxicity: freshwater",
    "energy resources: non-renewable",
    "eutrophication: freshwater",
    "eutrophication: marine",
    "human toxicity: carcinogenic",
    "human toxicity: non-carcinogenic",
    "material resources: metals/minerals",
    "particulate matter formation",
)

STEEL_MARKET_NAME = f"{GENERIC_MARKET_NAME}, for steel"
EXCLUDED_STEEL_LOCATION = "WORLD"

EXPORT_FILENAMES = {
    "selection": "hydrogen_markets_europe_selection.csv",
    "methods": "hydrogen_markets_europe_methods.csv",
    "scores": "hydrogen_markets_europe_lcia_scores_per_kg.csv",
    "RER comparison": "hydrogen_markets_europe_vs_generic_RER.csv",
    "EF 3.1 spider ratios": "hydrogen_markets_europe_ef31_ratios_vs_generic_RER.csv",
    "Hotspot process groups": "hydrogen_markets_europe_hotspot_process_groups.csv",
    "Hotspot reconciliation": "hydrogen_markets_europe_hotspot_reconciliation.csv",
    "contributions": "hydrogen_markets_europe_lcia_process_contributions.csv",
    "Stage Layer 1": "hydrogen_markets_europe_stage_layer1.csv",
    "Production inputs detailed": "hydrogen_markets_europe_production_inputs_detailed.csv",
    "Production input groups": "hydrogen_markets_europe_production_input_groups.csv",
    "Distribution processes": "hydrogen_markets_europe_distribution_processes.csv",
    "Stage classification audit": "hydrogen_markets_europe_stage_classification_audit.csv",
    "Stage reconciliation": "hydrogen_markets_europe_stage_reconciliation.csv",
    "Steel-region selection": "hydrogen_steel_regions_selection.csv",
    "Steel-region scores": "hydrogen_steel_regions_scores.csv",
    "Steel-region Layer 1": "hydrogen_steel_regions_stage_layer1.csv",
    "Steel-region distribution": "hydrogen_steel_regions_distribution_processes.csv",
    "Steel-region reconciliation": "hydrogen_steel_regions_reconciliation.csv",
}
EXPORT_PATHS = {
    label: RESULTS_DIR / filename
    for label, filename in EXPORT_FILENAMES.items()
}

PLOT_FILENAMES = {
    "absolute impacts": "01_absolute_lcia_impacts.png",
    "baseline heatmap": "02_difference_from_generic_RER.png",
    "EF spider": "03_ef31_spider_ratios.png",
    "hotspots": "04_selected_impact_hotspots.png",
    "stage layer 1": "05_stage_layer1.png",
    "distribution layer 2": "06_distribution_layer2.png",
    "steel stage layer 1": "07_steel_regions_stage_layer1.png",
    "steel distribution layer 2": "08_steel_regions_distribution_layer2.png",
}
PLOT_PATHS = {
    label: RESULTS_DIR / filename for label, filename in PLOT_FILENAMES.items()
}

# A deliberately non-repeating categorical palette. Plots also use hatches and
# markers so that category identification never depends on color alone. The
# notebook raises instead of recycling colors if this palette is exhausted.
ACCESSIBLE_CATEGORICAL_COLORS = (
    "#0072B2",
    "#E69F00",
    "#009E73",
    "#CC79A7",
    "#D55E00",
    "#56B4E9",
    "#F0E442",
    "#000000",
    "#332288",
    "#88CCEE",
    "#44AA99",
    "#117733",
    "#999933",
    "#DDCC77",
    "#CC6677",
    "#882255",
    "#AA4499",
    "#661100",
    "#6699CC",
    "#AA4466",
    "#4477AA",
    "#228833",
    "#EE6677",
    "#BBBBBB",
    "#66CCEE",
    "#AA3377",
    "#BBBB44",
    "#EE8866",
    "#77AADD",
    "#99DDFF",
    "#44BB99",
    "#BBCC33",
    "#AAAA00",
    "#EEDD88",
    "#FFAABB",
    "#DDDDDD",
    "#6F4E7C",
    "#2A9D8F",
    "#E76F51",
    "#264653",
    "#8AB17D",
    "#B56576",
    "#5F6CAF",
    "#C17C74",
    "#3C6E71",
    "#9C6644",
    "#7B2CBF",
    "#3A86FF",
    "#FF006E",
    "#FB5607",
    "#2D6A4F",
    "#4D908E",
    "#577590",
    "#F3722C",
    "#90BE6D",
    "#B5179E",
    "#4361EE",
    "#7209B7",
    "#A44A3F",
    "#52796F",
)
HATCHES = ("", "///", "\\\\", "xx", "..", "++", "oo", "--", "||", "**")
MARKERS = ("o", "s", "^", "D", "P", "X", "v", "<", ">", "h", "*")
LINESTYLES = ("-", "--", "-.", ":")
