from ..filesystem_constants import DATA_DIR, VARIABLES_DIR

REGION_CLIMATE_MAP = VARIABLES_DIR / "iam_region_to_climate.yaml"
FUEL_LABELS = DATA_DIR / "fuels" / "fuel_labels.csv"
SUPPLY_CHAIN_SCENARIOS = DATA_DIR / "fuels" / "supply_chain_scenarios.yml"
HEAT_SOURCES = DATA_DIR / "fuels" / "heat_sources_map.yml"
HYDROGEN_SOURCES = DATA_DIR / "fuels" / "hydrogen_efficiency_parameters.yml"
HYDROGEN_SUPPLY_LOSSES = DATA_DIR / "fuels" / "hydrogen_supply_losses.yml"
METHANE_SOURCES = DATA_DIR / "fuels" / "methane_activities.yml"
LIQUID_FUEL_SOURCES = DATA_DIR / "fuels" / "liquid_fuel_activities.yml"
FUEL_MARKETS = DATA_DIR / "fuels" / "fuel_markets.yml"
BIOFUEL_SOURCES = DATA_DIR / "fuels" / "biofuels_activities.yml"
FUEL_GROUPS = DATA_DIR / "fuels" / "fuel_groups.yaml"
CROPS_PROPERTIES = VARIABLES_DIR / "crops.yaml"
