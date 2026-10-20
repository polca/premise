"""
Integrates projections regarding use of metals in the economy from:
- Mining shares
- Metal intensities
- Transport distances

"""

import uuid
import ast
import re
from functools import lru_cache
from typing import Optional
from collections import Counter, defaultdict

import country_converter as coco
import numpy as np
import pandas as pd
import yaml

from .export import biosphere_flows_dictionary
from .logger import create_logger
from .metals_rules import (
    MATERIAL_RULES_PATH,
    ActivityPolicy,
    MaterialRule,
    MetalsConfigError,
    conversion_factors_as_dataframe_records,
    load_material_rules,
    load_technology_conversions,
    material_rules_as_dataframe_records,
)
from .provenance import record_change_event
from .inventory_store import (
    filter_biosphere_category,
    get_scenario_inventory,
    replace_scenario_inventory,
)
from .transformation import (
    BaseTransformation,
    Dict,
    IAMDataCollection,
    InventorySet,
    List,
    Set,
    ws,
)
from .utils import DATA_DIR, rescale_exchange
from .validation import MetalsValidation
from .validation_framework import record_validation_phase

logger = create_logger("metal")

NATURAL_RESOURCE_IN_GROUND = ("natural resource", "in ground")
MARKET_DATASET_PREFIXES = ("market for ", "market group for ")
OLD_METAL_MARKET_LOCATIONS = ("GLO", "RoW")
SECONDARY_METAL_SUPPLY_TERMS = (
    "treatment of",
    "used",
    "recover",
    "discard",
    "recycl",
    "scrap",
    "waste",
    "sludge",
)
RESOURCE_MATCH_STOPWORDS = {
    "7n",
    "acid",
    "alloy",
    "and",
    "anode",
    "bearing",
    "bulk",
    "cathode",
    "class",
    "concentrate",
    "crude",
    "dry",
    "from",
    "grade",
    "high",
    "hydroxide",
    "in",
    "ingot",
    "liquid",
    "metal",
    "metallurgical",
    "ore",
    "oxide",
    "primary",
    "raw",
    "refined",
    "semiconductor",
    "sponge",
    "sulfide",
    "tetrachloride",
    "the",
}
METAL_EXTRACTION_DATASET_KEYWORDS = (
    "beneficiation",
    "concentrate",
    "extraction",
    "mine",
    "mining",
    "ore",
    "quarry",
    "smelting",
)
RESOURCE_CARRIER_PRODUCTS_WITH_DOWNSTREAM_ATTRIBUTION = {
    "copper-cobalt ore",
    "copper, anode",
    "lead concentrate",
    "lithium brine, 6.7 % Li",
    "platinum group metal concentrate",
    "sodium chloride, brine solution",
    "sodium chloride, powder",
    "zinc concentrate",
}
RESOURCE_CARRIER_DATASETS_WITH_DOWNSTREAM_ATTRIBUTION = {
    (
        "cobalt hydroxide, via hydrometallurigcal ore procesing, mass allocation",
        "cobalt hydroxide",
    ),
    (
        "cobalt sulfate production, from copper mining, mass allocation",
        "cobalt sulfate",
    ),
    (
        "copper-cobalt mining, industrial, mass allocation",
        "copper-cobalt ore",
    ),
    (
        "copper-cobalt mining, artisanal",
        "copper-cobalt ore",
    ),
}
# Explicit downstream attribution boundary for the concentrate whose resource
# flows are cleared above. Zinc/zinc-oxide process inputs do not replace this
# primary feed; they retain their own supply-chain burdens. This is a resource
# allocation convention, not an inferred ore grade or recovery yield.
PRIMARY_RESOURCE_ATTRIBUTION_FEEDS = {
    ("primary zinc production from concentrate", "zinc"): "zinc concentrate",
}
METAL_RESOURCE_FLOW_NAMES = {
    "Aluminium",
    "Antimony",
    "Arsenic",
    "Barium",
    "Beryllium",
    "Boron",
    "Cadmium",
    "Calcium",
    "Cerium",
    "Chromium",
    "Cobalt",
    "Copper",
    "Dysprosium",
    "Erbium",
    "Europium",
    "Gadolinium",
    "Gallium",
    "Germanium",
    "Gold",
    "Graphite",
    "Hafnium",
    "Holmium",
    "Indium",
    "Iridium",
    "Iron",
    "Lanthanum",
    "Lead",
    "Lithium",
    "Lutetium",
    "Magnesium",
    "Manganese",
    "Mercury",
    "Molybdenum",
    "Neodymium",
    "Nickel",
    "Niobium",
    "Palladium",
    "Phosphorus",
    "Platinum",
    "Potassium",
    "Praseodymium",
    "Rhenium",
    "Rhodium",
    "Ruthenium",
    "Samarium",
    "Scandium",
    "Selenium",
    "Silicon",
    "Silver",
    "Sodium",
    "Spodumene",
    "Strontium",
    "Sulfur",
    "Tantalum",
    "Tellurium",
    "Terbium",
    "Thulium",
    "Tin",
    "Titanium",
    "Tungsten",
    "Uranium",
    "Vanadium",
    "Ytterbium",
    "Yttrium",
    "Zinc",
    "Zirconium",
}
RESOURCE_PRODUCT_ALIASES = {
    "7n arsenic": "arsenic",
    "bauxite": "aluminium",
    "chromite": "chromium",
    "molybdenite": "molybdenum",
    "phosphate": "phosphorus",
    "pyrochlore": "niobium",
    "silica": "silicon",
    "sodium borates": "sodium",
    "stibnite": "antimony",
    "vanadium bearing magnetite": "vanadium",
    "vanadium pentoxide": "vanadium",
    "zircon": "zirconium",
}
RESOURCE_FLOW_ALIASES = {
    "metamorphous rock graphite containing": "graphite",
}

ATOMIC_MASSES = {
    "Be": 9.0121831,
    "H": 1.00794,
    "O": 15.999,
    "Ti": 47.867,
    "Sb": 121.76,
    "S": 32.06,
}


def element_mass_fraction(element: str, formula: dict) -> float:
    """Return the mass fraction of an element in a simple chemical formula."""
    molecular_mass = sum(
        ATOMIC_MASSES[symbol] * count for symbol, count in formula.items()
    )
    return ATOMIC_MASSES[element] * formula[element] / molecular_mass


TITANIUM_DIOXIDE_TITANIUM_FRACTION = element_mass_fraction("Ti", {"Ti": 1, "O": 2})
TITANIUM_CHAIN_CONTENT_ADJUSTMENT = 1.002720132146914

METAL_BEARING_PRODUCT_CONTENT_FACTORS = {
    (
        "beryllium hydroxide",
        "beryllium",
    ): (
        element_mass_fraction("Be", {"Be": 1, "O": 2, "H": 2}),
        "stoichiometric Be content in Be(OH)2",
    ),
    (
        "stibnite concentrate",
        "antimony",
    ): (
        0.70 * element_mass_fraction("Sb", {"Sb": 2, "S": 3}),
        "documented 70 wt% Sb2S3 concentrate; stoichiometric Sb content",
    ),
    (
        "chromite ore concentrate",
        "chromium",
    ): (
        0.2655735274241075,
        "recoverable Cr content represented by the chromium production chain",
    ),
    (
        "manganese concentrate",
        "manganese",
    ): (
        0.26295358364884125,
        "recoverable Mn content represented by the manganese production chain",
    ),
    (
        "molybdenite",
        "molybdenum",
    ): (
        0.54545591456969,
        "recoverable Mo content represented by the molybdenum production chain",
    ),
    (
        "gold, unrefined",
        "gold",
    ): (
        0.8373709757567793,
        "recoverable Au content represented by the gold production chain",
    ),
    (
        "tin concentrate",
        "tin",
    ): (
        0.48379101906039534,
        "recoverable Sn content represented by the tin production chain",
    ),
    (
        "titania slag, 94% titanium dioxide",
        "titanium",
    ): (
        0.94 * TITANIUM_DIOXIDE_TITANIUM_FRACTION * TITANIUM_CHAIN_CONTENT_ADJUSTMENT,
        "94% TiO2 Ti content adjusted to the titanium production chain",
    ),
    (
        "rutile, 95% titanium dioxide",
        "titanium",
    ): (
        0.95 * TITANIUM_DIOXIDE_TITANIUM_FRACTION * TITANIUM_CHAIN_CONTENT_ADJUSTMENT,
        "95% TiO2 Ti content adjusted to the titanium production chain",
    ),
    (
        "ilmenite, 54% titanium dioxide",
        "titanium",
    ): (
        0.54 * TITANIUM_DIOXIDE_TITANIUM_FRACTION * TITANIUM_CHAIN_CONTENT_ADJUSTMENT,
        "54% TiO2 Ti content adjusted to the titanium production chain",
    ),
    (
        "zinc concentrate",
        "zinc",
    ): (
        0.6377594812446097,
        "recoverable Zn content represented by the zinc production chain",
    ),
    (
        "lead concentrate",
        "lead",
    ): (
        1 / 1.7629377841949463,
        "recoverable Pb content represented by the lead production chain",
    ),
}

PURE_PRODUCT_EXCLUDED_TERMS = {
    "concentrate",
    "hydroxide",
    "ore",
    "oxide",
    "rock",
    "slag",
}
PURE_PRODUCT_QUALIFIERS = {
    "7n",
    "cathode",
    "class",
    "grade",
    "high",
    "ingot",
    "liquid",
    "metal",
    "primary",
    "raw",
    "refined",
    "semiconductor",
    "sponge",
    "unrefined",
}


class PostAllocationCorrectionError(ValueError):
    """Raised when a metal post-allocation correction cannot be applied safely."""


def _update_metals(scenario, version, system_model):

    metals = Metals(
        database=get_scenario_inventory(scenario),
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    metals.create_metal_markets()
    metals.update_metals_use_in_database()
    metals.relink_datasets()
    replace_scenario_inventory(scenario, metals.database)
    scenario["cache"] = metals.cache
    scenario["index"] = metals.index
    scenario.setdefault("mapping", {})["metals"] = {
        "transformed activities": list(metals._validation_targets.values()),
        "material decisions": list(metals.material_decisions),
        "material update metrics": dict(metals.material_update_metrics),
        "applied market shares": dict(metals.applied_market_shares),
        "market diagnostics": list(metals.market_diagnostics),
        "resource diagnostics": list(metals.resource_diagnostics),
        "methodology": metals.methodology_summary(),
    }

    validate = MetalsValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=metals.database,
        iam_data=scenario["iam data"],
        system_model=metals.system_model,
        version=metals.version,
    )

    mining_shares_mapping = load_mining_shares_mapping(metals.version)
    validate.applied_market_shares = metals.applied_market_shares
    validate.market_diagnostics = metals.market_diagnostics
    validate.resource_diagnostics = metals.resource_diagnostics
    validate.mining_shares_mapping = mining_shares_mapping
    validate.interpolate_by_year = interpolate_by_year
    validate.metals_list = (
        mining_shares_mapping.loc[
            (mining_shares_mapping["Work done"] == "Yes")
            & mining_shares_mapping["Country"].notna(),
            "Metal",
        ]
        .unique()
        .tolist()
    )
    validate.material_update_diagnostics = metals.material_update_diagnostics
    validate.material_decisions = metals.material_decisions
    validate.material_update_metrics = metals.material_update_metrics
    record_validation_phase(scenario, validate.run_metals_checks())

    return scenario


def load_metals_alternative_names():
    """
    Load dataframe with alternative names for metals
    """

    filepath = DATA_DIR / "metals" / "transport_activities_mapping.yaml"

    with open(filepath, "r", encoding="utf-8") as stream:
        out = yaml.safe_load(stream)

    # this dictionary has lists as values

    # create a reversed dictionary where
    # the keys are the alternative names
    # and the values are the metals

    rev_out = {}

    for k, v in out.items():
        for i in v:
            rev_out[i] = k

    return rev_out


def load_metals_transport():
    """
    Load dataframe with metals transport
    """

    filepath = DATA_DIR / "metals" / "transport_markets_data.csv"
    df = pd.read_csv(filepath, sep=",")

    # remove rows without values under Weighted Average Distance
    df = df.loc[~df["Weighted Distance (km)"].isnull()]
    distances = pd.to_numeric(df["Weighted Distance (km)"], errors="raise")
    if not np.isfinite(distances).all() or (distances < 0).any():
        raise MetalsConfigError(
            "Metal freight distances must be finite and nonnegative."
        )
    if (
        not df["TransportMode Label"]
        .str.lower()
        .isin({"air", "sea", "railway", "road"})
        .all()
    ):
        raise MetalsConfigError("Unsupported metal freight mode.")
    # An explicit zero is an observation; a missing row is a coverage gap.
    df = df.copy()
    df["Weighted Distance (km)"] = distances

    df["country"] = df["Country"]

    return df


def build_transport_lookup(dataframe: pd.DataFrame) -> Dict[tuple, list[int]]:
    """Retain every mode/route for each country and metal."""
    lookup = defaultdict(list)
    for position, key in enumerate(zip(dataframe["country"], dataframe["Metal"])):
        lookup[key].append(position)
    return dict(lookup)


def select_transport_provider(database, mode: str) -> dict:
    """Select freight services in tonne-km, never vehicle manufacture in units."""
    terms = {
        "air": ("aircraft", "long haul"),
        "sea": ("sea", "container ship"),
        "railway": ("train",),
        "road": ("lorry", "unspecified"),
    }
    return ws.get_one(
        database,
        ws.either(
            ws.startswith("name", "market for transport, freight"),
            ws.startswith("name", "market group for transport, freight"),
        ),
        ws.equals("unit", "ton kilometer"),
        ws.equals("location", "GLO"),
        ws.exclude(ws.contains("name", "reefer")),
        *[ws.contains("name", term) for term in terms[mode]],
    )


@lru_cache(maxsize=None)
def _load_mining_shares_mapping(ei_version="3.12"):
    """
    Load mapping between mining shares from the different sources and ecoinvent
    """

    filepath = DATA_DIR / "metals" / "mining_shares_mapping.xlsx"

    if ei_version == "3.11":
        df = pd.read_excel(filepath, sheet_name="ei311")
    elif ei_version == "3.12":
        df = pd.read_excel(filepath, sheet_name="ei312")
    else:
        df = pd.read_excel(filepath, sheet_name="ei310")

    # replace all instances of "Year" in columns by ""
    df.columns = df.columns.str.replace("Year ", "")

    return df


def load_mining_shares_mapping(ei_version="3.12"):
    """Return an isolated copy of the cached mining-share mapping."""

    normalized_version = {
        "310": "3.10",
        "311": "3.11",
        "312": "3.12",
    }.get(str(ei_version), str(ei_version))
    return _load_mining_shares_mapping(normalized_version).copy(deep=True)


def load_primary_secondary_split():
    """
    Load mapping for primary and secondary split of metal markets.
    """
    path = DATA_DIR / "metals" / "primary_secondary_split.yaml"
    with open(path, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def load_activities_mapping():
    """
    Load mapping for the ecoinvent exchanges to be
    updated by the new metal intensities. Only rows
    where filter was set to yes are considered.
    """

    df = pd.DataFrame(material_rules_as_dataframe_records())
    return df.loc[df["filter"].str.lower() == "yes"].copy()


# Define a function to replicate rows based on the generated activity sets
def extend_dataframe(df, mapping):
    """ "
     Extend a DataFrame by duplicating rows based on a mapping dictionary.

    Parameters:
    - df (pd.DataFrame): The original DataFrame to be extended.
    - mapping (dict): A dictionary with keys corresponding to the 'technology'
                      values in the DataFrame and values that are sets of processes.
    """

    new_rows = []

    for key, processes in mapping.items():
        # Find the rows in the DataFrame where the 'technology' matches the key
        matching_rows = df[df["technology"] == key]
        # For each process in the set associated with the key, duplicate the matching rows
        for process in processes:
            temp_rows = matching_rows.copy()
            temp_rows["ecoinvent_technology"] = process["name"]
            new_rows.extend(temp_rows.to_dict("records"))
    new_df = pd.DataFrame(new_rows)

    return new_df


def get_ecoinvent_metal_factors():
    """
    Load dataframe with ecoinvent factors for metals
    and convert to xarray
    """

    filepath = DATA_DIR / "metals" / "ecoinvent_factors.csv"
    df = pd.read_csv(filepath)

    # create column "activity" as a tuple
    # of the columns "name", "product" and "location".
    df["activity"] = list(zip(df["name"], df["product"], df["location"]))
    df = df.drop(columns=["name", "product", "location"])
    df = df.melt(id_vars=["activity"], var_name="metal", value_name="value")

    # create an xarray with dimensions activity, metal and year
    ds = df.groupby(["activity", "metal"]).sum()["value"].to_xarray()

    return ds


def fetch_mapping(filepath: str) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, "r", encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


def rev_metals_map(mapping: dict) -> dict:
    """Returns a reversed dictionary"""

    rev_mapping = {}
    for key, val in mapping.items():
        for v in val:
            rev_mapping[v["name"]] = key
    return rev_mapping


def load_conversion_factors():
    """
    Load dataframe with conversion factors for metals
    """

    return pd.DataFrame(conversion_factors_as_dataframe_records())


def update_exchanges(
    activity: dict,
    new_amount: float,
    new_provider: dict,
    metal: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> dict:
    """
    Update exchanges for a given activity.

    :param activity: Activity to update
    :param new_amount: Result of the calculation
    :param new_provider: Dataset of the new provider
    :param metal: Metal name
    :param min_value: Minimum value for uncertainty
    :param max_value: Maximum value for uncertainty
    :return: Updated activity
    """
    # fetch old amount
    old_amount = sum(
        exc["amount"]
        for exc in activity["exchanges"]
        if exc.get("product", "").lower() == new_provider["reference product"].lower()
        and exc.get("type") == "technosphere"
    )

    activity["exchanges"] = [
        e
        for e in activity["exchanges"]
        if not (
            e.get("type") == "technosphere"
            and e.get("product", "").lower()
            == new_provider["reference product"].lower()
        )
    ]

    new_exchange = {
        "amount": new_amount,
        "product": new_provider["reference product"],
        "name": new_provider["name"],
        "unit": new_provider["unit"],
        "location": new_provider["location"],
        "type": "technosphere",
        "uncertainty type": 0,  # assumes no uncertainty
    }

    if min_value is not None and max_value is not None:
        if min_value != max_value:
            if min_value <= new_amount <= max_value:
                new_exchange.update(
                    {
                        "uncertainty type": 5,
                        "loc": new_amount,
                        "minimum": min_value,
                        "maximum": max_value,
                        "preserve uncertainty": True,
                    }
                )
            else:
                print(
                    f"Value {new_amount} outside of range {min_value} - {max_value} for {metal} in {activity['name']}"
                )

    activity["exchanges"].append(new_exchange)

    # Log changes
    activity.setdefault("log parameters", {})
    activity["log parameters"].setdefault("old amount", {}).update({metal: old_amount})
    activity["log parameters"].setdefault("new amount", {}).update({metal: new_amount})

    return activity


def filter_technology(dataset_names, database):
    return list(
        ws.get_many(
            database,
            ws.either(*[ws.contains("name", name) for name in dataset_names]),
        )
    )


def build_ws_filter(field: str, query: dict):
    """
    Given a field and a query like {'contains': 'foo'}, return a filter function.
    """

    if not isinstance(query, list):
        queries = [query]
    else:
        queries = query

    filters = []

    for query in queries:
        for operator, value in query.items():
            if value == "":
                continue

            if operator == "contains":
                filters.append(ws.contains(field, value))
            elif operator == "equals":
                filters.append(ws.equals(field, value))
            elif operator == "startswith":
                filters.append(ws.startswith(field, value))
            elif operator == "all":
                for q in value:
                    filters += build_ws_filter(field, q)

            elif operator == "either":
                res = []
                for q in value:
                    res += build_ws_filter(field, q)
                if res:
                    filters.append(ws.either(*res))

            else:
                raise ValueError(
                    f"Unsupported operator {operator} for field {field} in query {query}"
                )

    if not filters:
        raise ValueError(f"No valid filters provided for field {field}")

    return filters


def matches_filter_query(value: str, query) -> bool:
    """Evaluate a mining-share filter without constructing Wurst predicates."""

    if isinstance(query, list):
        for item in query:
            if not matches_filter_query(value, item):
                return False
        return True

    if not isinstance(query, dict):
        raise ValueError(f"Invalid filter query: {query!r}")

    found_operator = False
    for operator, expected in query.items():
        if expected == "":
            continue
        found_operator = True
        if operator == "contains":
            if expected not in value:
                return False
        elif operator == "equals":
            if value != expected:
                return False
        elif operator == "startswith":
            if not value.startswith(expected):
                return False
        elif operator == "all":
            for item in expected:
                if not matches_filter_query(value, item):
                    return False
        elif operator == "either":
            for item in expected:
                if matches_filter_query(value, item):
                    break
            else:
                return False
        else:
            raise ValueError(f"Unsupported operator {operator} in query {query}")

    if not found_operator:
        raise ValueError(f"No valid filters provided in query {query}")
    return True


def extract_exact_filter_values(query) -> Optional[Set[str]]:
    """Return exact values represented by a filter, or ``None`` if not exact."""

    if isinstance(query, list):
        values = None
        for item in query:
            item_values = extract_exact_filter_values(item)
            if item_values is None:
                return None
            values = item_values if values is None else values & item_values
        return values

    if not isinstance(query, dict) or len(query) != 1:
        return None

    operator, expected = next(iter(query.items()))
    if operator == "equals" and expected != "":
        return {expected}
    if operator == "either":
        values = set()
        for item in expected:
            item_values = extract_exact_filter_values(item)
            if item_values is None:
                return None
            values.update(item_values)
        return values
    if operator == "all":
        return extract_exact_filter_values(expected)
    return None


def extract_reference_products_from_filter(value) -> List[str]:
    """
    Extract exact reference-product labels from a mining-share filter.

    The Excel mapping stores filters as stringified dictionaries. Most are simple
    {"equals": "..."} filters, but some use {"either": [...]} for version-specific
    product labels.
    """
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return [value]

    if not isinstance(value, dict):
        return []

    if "equals" in value:
        return [value["equals"]]

    if "either" in value:
        products = []
        for item in value["either"]:
            products.extend(extract_reference_products_from_filter(item))
        return products

    return []


def parse_mining_filter(value):
    """Parse the workbook's serialized selectors at a single boundary."""
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except (ValueError, SyntaxError) as error:
            raise MetalsConfigError(f"Invalid mining filter {value!r}") from error
    if not isinstance(value, (dict, list)):
        raise MetalsConfigError(f"Invalid mining filter {value!r}")
    return value


def is_secondary_metal_supply_exchange(
    exchange: dict, reference_product: Optional[str] = None
) -> bool:
    """Return True for old-market inputs that represent secondary metal supply."""
    if exchange.get("type") != "technosphere":
        return False

    try:
        amount = float(exchange.get("amount", 0))
    except (TypeError, ValueError):
        return False

    if amount <= 0:
        return False

    if exchange.get("unit") != "kilogram":
        return False

    if reference_product is not None and exchange.get("product") != reference_product:
        return False

    text = f"{exchange.get('name', '')} {exchange.get('product', '')}".lower()
    return any(term in text for term in SECONDARY_METAL_SUPPLY_TERMS)


@lru_cache(maxsize=None)
def normalize_resource_label(value: str) -> str:
    """Normalize resource labels enough for conservative fuzzy matching."""
    value = value or ""
    value = value.lower().replace(", in ground", "")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


@lru_cache(maxsize=None)
def canonical_resource_flow_label(value: str) -> str:
    """Return a canonical metal label for version-specific resource flows."""
    label = normalize_resource_label(value)
    return RESOURCE_FLOW_ALIASES.get(label, label)


@lru_cache(maxsize=None)
def resource_label_tokens(value: str) -> Set[str]:
    return frozenset(
        token
        for token in normalize_resource_label(value).split()
        if len(token) > 1
        and not token.isdigit()
        and token not in RESOURCE_MATCH_STOPWORDS
    )


@lru_cache(maxsize=None)
def get_metal_bearing_product_content_factor(
    reference_product: str, flow_name: str
) -> tuple[Optional[float], str]:
    """Return kg target metal per kg metal-bearing product, when known."""
    product_label = normalize_resource_label(reference_product)
    flow_label = canonical_resource_flow_label(flow_name)

    for (product, flow), (
        amount,
        basis,
    ) in METAL_BEARING_PRODUCT_CONTENT_FACTORS.items():
        factor_product_label = normalize_resource_label(product)
        factor_flow_label = normalize_resource_label(flow)

        if factor_product_label not in product_label or factor_flow_label != flow_label:
            continue

        product_metal_tokens = (
            set(product_label.split()) & get_metal_resource_flow_labels()
        )
        if factor_product_label != product_label and product_metal_tokens - {
            flow_label
        }:
            continue

        return amount, basis

    return None, ""


@lru_cache(maxsize=None)
def is_pure_target_resource_product(reference_product: str, flow_name: str) -> bool:
    """Return True if the reference product denotes the target metal itself."""
    product_label = normalize_resource_label(reference_product)
    flow_label = canonical_resource_flow_label(flow_name)

    if product_label == flow_label:
        return True

    product_tokens = {token for token in product_label.split() if not token.isdigit()}
    if flow_label not in product_tokens:
        return False
    if product_tokens & PURE_PRODUCT_EXCLUDED_TERMS:
        return False

    return product_tokens <= {flow_label} | PURE_PRODUCT_QUALIFIERS


def get_target_resource_amount(
    dataset: dict, target_exchange: dict
) -> tuple[float, str]:
    """
    Return kg target resource per kg dataset reference product.

    Pure metal products are set to one kilogram. For metal-bearing products such
    as hydroxides, concentrates, and oxide products, use the recoverable metal
    content represented by that product in the supply chain. If no explicit
    content factor is known, keep the dataset's existing target resource amount:
    it is the best available product-specific content and avoids treating ores
    and concentrates as pure metals.
    """
    reference_product = dataset.get("reference product", "")
    flow_name = target_exchange.get("name", "")

    amount, basis = get_metal_bearing_product_content_factor(
        reference_product, flow_name
    )
    if amount is not None:
        return amount, basis

    if is_pure_target_resource_product(reference_product, flow_name):
        return 1.0, "pure target resource product"

    return (
        float(target_exchange.get("amount", 0.0)),
        "unresolved metal-bearing product content; retained existing target amount",
    )


def target_resource_amount_is_resolved(dataset: dict, target_exchange: dict) -> bool:
    """Return True when target content can be set from explicit rules."""
    _, basis = get_target_resource_amount(dataset, target_exchange)
    return not basis.startswith("unresolved metal-bearing product content")


def has_resolved_target_resource_context(dataset: dict) -> bool:
    """
    Return True for non-market datasets with one safely correctable target flow.

    This catches metal co-products from production datasets whose names do not
    contain mining/extraction keywords, while avoiding broad correction of
    unresolved compounds such as metal salts.
    """
    if is_market_dataset(dataset):
        return False

    matches = get_target_resource_exchanges(dataset)
    matched_names = {exc.get("name") for exc in matches}
    if len(matched_names) != 1 or len(matches) != 1:
        return False

    return target_resource_amount_is_resolved(dataset, matches[0])


@lru_cache(maxsize=None)
def is_downstream_attributed_resource_carrier(reference_product: str) -> bool:
    """Return True for generic intermediates whose metal flows are handled downstream."""
    product_label = normalize_resource_label(reference_product)
    return product_label in {
        normalize_resource_label(product)
        for product in RESOURCE_CARRIER_PRODUCTS_WITH_DOWNSTREAM_ATTRIBUTION
    }


def is_downstream_attributed_resource_carrier_dataset(dataset: dict) -> bool:
    """Return True when a dataset's resource flows are assigned downstream."""
    if is_downstream_attributed_resource_carrier(dataset.get("reference product", "")):
        return True

    dataset_key = (
        normalize_resource_label(dataset.get("name", "")),
        normalize_resource_label(dataset.get("reference product", "")),
    )
    return dataset_key in {
        (normalize_resource_label(name), normalize_resource_label(product))
        for name, product in RESOURCE_CARRIER_DATASETS_WITH_DOWNSTREAM_ATTRIBUTION
    }


@lru_cache(maxsize=None)
def get_reference_product_resource_flow_name(reference_product: str) -> Optional[str]:
    """Return the metal resource flow represented by a pure metal product."""
    product_tokens = resource_label_tokens(reference_product)
    if len(product_tokens) != 1:
        return None

    product_token = next(iter(product_tokens))
    for flow_name in METAL_RESOURCE_FLOW_NAMES:
        if normalize_resource_label(flow_name) == product_token:
            return flow_name

    return None


def classify_resource_activity(dataset: dict, primary_route: bool = False) -> str:
    """Separate extraction attribution from secondary and downstream supply.

    A pure reference product alone is not evidence of virgin extraction. The
    mining-share mapping provides explicit primary-route evidence when present.
    """
    if is_market_dataset(dataset):
        return "market"
    if dataset.get("unit") != "kilogram":
        return "unsupported_unit"
    name = dataset.get("name", "").lower()
    if any(term in name for term in SECONDARY_METAL_SUPPLY_TERMS):
        return "secondary"
    if is_downstream_attributed_resource_carrier_dataset(dataset):
        return "carrier"
    if has_metal_extraction_context(dataset):
        return "extraction"
    if primary_route or has_resolved_target_resource_context(dataset):
        return "primary_or_coproduct"
    return "unresolved"


def can_add_missing_target_resource_exchange(
    dataset: dict, primary_route: bool = False
) -> bool:
    """Return True when a missing target flow can be inferred safely."""
    if classify_resource_activity(dataset, primary_route) not in {
        "extraction",
        "primary_or_coproduct",
    }:
        return False

    flow_name = get_reference_product_resource_flow_name(
        dataset.get("reference product", "")
    )
    if not flow_name:
        return False

    return is_pure_target_resource_product(
        dataset.get("reference product", ""), flow_name
    )


def set_resource_amount(exchange: dict, amount: float) -> None:
    """Rescale positive distributions; make intentional zero/new values deterministic."""
    old_amount = float(exchange.get("amount", 0))
    if amount > 0 and old_amount > 0:
        rescale_exchange(exchange, amount / old_amount, remove_uncertainty=False)
    else:
        exchange["uncertainty type"] = 0
        for field in ("loc", "scale", "shape", "minimum", "maximum", "negative"):
            exchange.pop(field, None)
    exchange["amount"] = amount


def make_target_resource_exchange(flow_name: str, amount: float) -> dict:
    """Create a natural-resource exchange for an in-ground metal resource."""
    return {
        "amount": amount,
        "type": "biosphere",
        "name": flow_name,
        "unit": "kilogram",
        "categories": NATURAL_RESOURCE_IN_GROUND,
        "uncertainty type": 0,
    }


def resolve_existing_resource_flow_name(
    flow_name: str, resource_flow_names: Set[str]
) -> str:
    """Return an existing version-specific flow name for a target resource."""
    flow_label = canonical_resource_flow_label(flow_name)
    matches = sorted(
        name
        for name in resource_flow_names
        if canonical_resource_flow_label(name) == flow_label
    )
    if not matches:
        return flow_name

    for name in matches:
        if normalize_resource_label(name) == normalize_resource_label(flow_name):
            return name

    return matches[0]


def is_market_dataset(dataset: dict) -> bool:
    return dataset.get("name", "").lower().startswith(MARKET_DATASET_PREFIXES)


def get_in_ground_resource_exchanges(dataset: dict) -> List[dict]:
    """Return natural-resource in-ground kilogram biosphere exchanges."""
    return filter_biosphere_category(
        dataset.get("exchanges", []), NATURAL_RESOURCE_IN_GROUND, "kilogram"
    )


@lru_cache(maxsize=None)
def get_resource_label_variants(reference_product: str) -> Set[str]:
    """Return product-derived labels that may identify the target resource flow."""
    variants = {normalize_resource_label(reference_product)}

    for alias, target in RESOURCE_PRODUCT_ALIASES.items():
        if alias in normalize_resource_label(reference_product):
            variants.add(normalize_resource_label(target))

    tokens = resource_label_tokens(reference_product)
    variants.update(tokens)

    return frozenset(variant for variant in variants if variant)


@lru_cache(maxsize=None)
def resource_flow_matches_reference_product(
    flow_name: str, reference_product: str
) -> bool:
    """Conservatively match a resource flow name to an activity reference product."""
    flow_label = canonical_resource_flow_label(flow_name)
    product_variants = get_resource_label_variants(reference_product)

    if flow_label in product_variants:
        return True

    flow_tokens = resource_label_tokens(flow_name)
    if not flow_tokens:
        return False

    for variant in product_variants:
        variant_tokens = resource_label_tokens(variant)
        if not variant_tokens:
            continue
        if flow_tokens <= variant_tokens or variant_tokens <= flow_tokens:
            return True

    return False


@lru_cache
def get_metal_resource_flow_labels() -> Set[str]:
    return {normalize_resource_label(name) for name in METAL_RESOURCE_FLOW_NAMES}


@lru_cache(maxsize=None)
def is_metal_resource_flow(flow_name: str) -> bool:
    return canonical_resource_flow_label(flow_name) in get_metal_resource_flow_labels()


def get_matching_resource_exchanges(
    dataset: dict, resource_exchanges: Optional[List[dict]] = None
) -> List[dict]:
    reference_product = dataset.get("reference product", "")
    if resource_exchanges is None:
        resource_exchanges = get_in_ground_resource_exchanges(dataset)
    return [
        exc
        for exc in resource_exchanges
        if resource_flow_matches_reference_product(
            exc.get("name", ""), reference_product
        )
    ]


def get_content_factor_resource_exchanges(
    dataset: dict, resource_exchanges: Optional[List[dict]] = None
) -> List[dict]:
    """Return resource exchanges with an explicit product-content factor."""
    reference_product = dataset.get("reference product", "")
    matches = []

    if resource_exchanges is None:
        resource_exchanges = get_in_ground_resource_exchanges(dataset)

    for exc in resource_exchanges:
        if not is_metal_resource_flow(exc.get("name", "")):
            continue

        amount, _ = get_metal_bearing_product_content_factor(
            reference_product, exc.get("name", "")
        )
        if amount is not None:
            matches.append(exc)

    return matches


def get_target_resource_exchanges(
    dataset: dict, resource_exchanges: Optional[List[dict]] = None
) -> List[dict]:
    content_factor_matches = get_content_factor_resource_exchanges(
        dataset, resource_exchanges
    )
    if len(content_factor_matches) == 1:
        return content_factor_matches

    return [
        exc
        for exc in get_matching_resource_exchanges(dataset, resource_exchanges)
        if is_metal_resource_flow(exc.get("name", ""))
    ]


@lru_cache(maxsize=None)
def product_label_may_carry_target_resource(product_label: str, flow_name: str) -> bool:
    """Return True when a product label denotes the target resource or carrier."""
    if is_downstream_attributed_resource_carrier(product_label):
        return False
    return resource_flow_matches_reference_product(flow_name, product_label)


def dataset_may_carry_target_resource(dataset: dict, flow_name: str) -> bool:
    """Return True if the dataset reference product can carry the target resource."""
    return product_label_may_carry_target_resource(
        dataset.get("reference product", ""), flow_name
    )


def exchange_may_carry_target_resource(exchange: dict, flow_name: str) -> bool:
    """Match a kilogram material input, rather than its supplier's industry name.

    Use the name only when the product is missing, so an identifiable but broken
    material link remains unresolved. Include attribution carriers here: their
    provider must resolve before the traversal can stop at that boundary.
    """
    if exchange.get("unit") != "kilogram":
        return False
    label = exchange.get("product") or exchange.get("name", "")
    return resource_flow_matches_reference_product(flow_name, label)


def has_metal_extraction_context(dataset: dict) -> bool:
    return has_keyword_in_dataset_label(dataset, METAL_EXTRACTION_DATASET_KEYWORDS)


def has_keyword_in_dataset_label(dataset: dict, keywords: tuple[str, ...]) -> bool:
    label = normalize_resource_label(
        f"{dataset.get('name', '')} {dataset.get('reference product', '')}"
    )
    return any(re.search(rf"\b{re.escape(keyword)}\b", label) for keyword in keywords)


def correct_metal_resource_exchanges(
    dataset: dict,
    strict: bool = False,
    add_missing_target_resource: bool = False,
    target_resource_flow_name: Optional[str] = None,
    resource_exchanges: Optional[List[dict]] = None,
) -> bool:
    """
    Correct target in-ground resource content and zero co-mined resources.

    Returns True when a dataset was processed by the correction. Non-market
    datasets without a resolvable target flow are skipped unless strict=True.
    """
    if resource_exchanges is None:
        resource_exchanges = get_in_ground_resource_exchanges(dataset)

    if classify_resource_activity(dataset, strict) in {
        "market",
        "secondary",
        "unsupported_unit",
    }:
        return False

    if not resource_exchanges:
        if add_missing_target_resource and can_add_missing_target_resource_exchange(
            dataset, primary_route=True
        ):
            flow_name = get_reference_product_resource_flow_name(
                dataset.get("reference product", "")
            )
            exchange_flow_name = target_resource_flow_name or flow_name
            target_exchange = make_target_resource_exchange(exchange_flow_name, 1.0)
            dataset.setdefault("exchanges", []).append(target_exchange)
            resource_exchanges.append(target_exchange)

            return True

        return False

    reference_product = dataset.get("reference product", "")

    if (
        not strict
        and not is_downstream_attributed_resource_carrier_dataset(dataset)
        and not has_metal_extraction_context(dataset)
        and not has_resolved_target_resource_context(dataset)
    ):
        return False

    if is_downstream_attributed_resource_carrier_dataset(dataset):
        for exc in resource_exchanges:
            if is_metal_resource_flow(exc.get("name", "")):
                set_resource_amount(exc, 0.0)
        return True

    matching_resource_exchanges = get_matching_resource_exchanges(
        dataset, resource_exchanges
    )
    matches = get_target_resource_exchanges(dataset, resource_exchanges)

    if matching_resource_exchanges and not matches:
        return False

    if not matches:
        if strict:
            raise PostAllocationCorrectionError(
                "Could not find a target in-ground resource flow for "
                f"{dataset.get('name')!r} / {reference_product!r} / "
                f"{dataset.get('location')!r}. Candidate flows: "
                f"{sorted({exc.get('name', '') for exc in resource_exchanges})}",
            )
        return False

    matched_names = {exc.get("name") for exc in matches}
    if len(matched_names) > 1:
        if not strict:
            return False
        raise PostAllocationCorrectionError(
            "Ambiguous target in-ground resource flows for "
            f"{dataset.get('name')!r} / {reference_product!r} / "
            f"{dataset.get('location')!r}: {sorted(matched_names)}",
        )

    if len(matches) > 1:
        if not strict:
            return False
        raise PostAllocationCorrectionError(
            "Duplicate target in-ground resource flow for "
            f"{dataset.get('name')!r} / {reference_product!r} / "
            f"{dataset.get('location')!r}: {matches[0].get('name')!r}",
        )

    target_exchange = matches[0]
    target_amount, _ = get_target_resource_amount(dataset, target_exchange)

    for exc in resource_exchanges:
        if not is_metal_resource_flow(exc.get("name", "")):
            continue
        is_target = exc is target_exchange
        new_amount = target_amount if is_target else 0.0
        set_resource_amount(exc, new_amount)

    return True


def interpolate_by_year(target_year: int, data: dict) -> float:
    """
    Interpolate (or extrapolate) a value for the given `target_year`
    from a dictionary like {2020: 0.1, 2030: 0.5, ...}.
    """
    data = {int(k): v for k, v in data.items()}
    years = sorted(data)

    if target_year in years:
        return data[target_year]
    elif target_year < years[0]:
        return data[years[0]]
    elif target_year > years[-1]:
        return data[years[-1]]
    else:
        for i in range(len(years) - 1):
            y0, y1 = years[i], years[i + 1]
            if y0 < target_year < y1:
                v0, v1 = data[y0], data[y1]
                return v0 + (v1 - v0) * (target_year - y0) / (y1 - y0)


def mining_shares_for_year(df: pd.DataFrame, year: int) -> pd.Series | None:
    """Select/interpolate declared shares, without inventing endpoint forecasts."""
    columns = {int(str(c)): c for c in df.columns if str(c).isdigit()}
    if not columns:
        raise MetalsConfigError("Mining shares have no observation-year columns.")
    years = sorted(columns)
    values = df[[columns[y] for y in years]].apply(pd.to_numeric, errors="raise")
    if not np.isfinite(values.to_numpy()).all() or (values < 0).any().any():
        raise MetalsConfigError(
            "Invalid mining shares: must be finite and nonnegative."
        )
    if not years[0] <= year <= years[-1]:
        return None
    if year in columns:
        return values[columns[year]]
    lower = max(y for y in years if y < year)
    upper = min(y for y in years if y > year)
    weight = (year - lower) / (upper - lower)
    return values[columns[lower]] * (1 - weight) + values[columns[upper]] * weight


class Metals(BaseTransformation):
    """
    Class that modifies metal demand of different technologies
    according to the Database built
    """

    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        cache: dict = None,
        index: dict = None,
    ):
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
            cache,
            index,
        )

        # ``write_log`` is already called at each created, updated, or relinked
        # activity boundary. Retain those object references as the exact,
        # incremental validation target set instead of rescanning the graph.
        self._validation_targets: Dict[int, dict] = {}

        self.country_codes = {}
        self.version = version

        self.metals = iam_data.metals_intensity_factors  # 1
        if not {"source_year_min", "source_year_max"} <= set(self.metals.coords):
            raise MetalsConfigError(
                "Metal intensity data is missing observation-year metadata; rebuild the IAM data cache."
            )
        # Select the requested year, then check each pair's actual support before
        # applying a rule. Filling endpoints in the array is not a forecast.
        available_years = sorted(self.metals.coords["year"].values)
        clamped_year = max(available_years[0], min(self.year, available_years[-1]))

        if clamped_year in available_years:
            self.precomputed_medians = self.metals.sel(year=clamped_year)
        else:
            self.precomputed_medians = self.metals.interp(
                year=clamped_year, method="linear"
            )

        self.material_rules_config = load_material_rules()
        self.material_rules = self.material_rules_config.enabled_rules
        self.material_policies = self.material_rules_config.policies
        self.material_rules_by_technology: Dict[str, List[MaterialRule]] = defaultdict(
            list
        )
        for rule in self.material_rules:
            self.material_rules_by_technology[rule.technology].append(rule)

        self.technology_conversions = load_technology_conversions()
        self.technology_conversions_by_name = {
            item.activity_name: item for item in self.technology_conversions
        }

        inv = InventorySet(self.database, self.version)

        self.activities_metals_map: Dict[str, Set] = (  # 2
            inv.generate_metals_activities_map()
        )

        self.metals_transport = load_metals_transport()
        self.alt_names = load_metals_alternative_names()

        self.metals_transport_activities = {
            mode: select_transport_provider(self.database, mode)
            for mode in ("air", "sea", "railway", "road")
        }

        self.build_db_indexes()
        self.material_update_diagnostics: List[dict] = []
        self.material_decisions: List[dict] = []
        self.market_diagnostics: List[dict] = []
        self.resource_diagnostics: List[dict] = []
        self.applied_market_shares: Dict[str, dict] = {}
        self.material_update_plan = self._compile_material_update_plan()
        self.material_update_metrics = {
            "compiled pairs": len(self.material_update_plan),
            "executed pairs": 0,
            "provider index lookups": 0,
        }

        self.transport_lookup = build_transport_lookup(self.metals_transport)

        self.prim_sec_split = load_primary_secondary_split()

    def update_metals_use_in_database(self):
        """Apply every compiled material rule to its target exactly once."""

        for item in self.material_update_plan:
            self.material_update_metrics["executed pairs"] += 1
            self._apply_material_rule(
                dataset=item["dataset"],
                technology=item["technology"],
                rule=item["rule"],
                conversion_factor=item["conversion_factor"],
            )

    def _compile_material_update_plan(self) -> List[dict]:
        """Compile unique dataset/rule work items before mutating the database."""

        plan: List[dict] = []
        seen: Dict[tuple[int, str], float | None] = {}
        writers = {}
        for technology, mapped_datasets in self.activities_metals_map.items():
            rules = self.material_rules_by_technology.get(technology, ())
            if not rules:
                continue
            for rule in rules:
                for mapped_dataset in mapped_datasets:
                    if rule.target.kind == "mapped_activity":
                        targets = (mapped_dataset,)
                    else:
                        targets = tuple(
                            self.db_index.get(rule.target.name, {}).get(
                                rule.target.reference_product, ()
                            )
                        )
                    for target in targets:
                        conversion_factor = (
                            None
                            if rule.preserve_reason
                            or self._material_policy_for(target, technology)
                            else self._technology_conversion_factor(
                                mapped_dataset, technology
                            )
                        )
                        if conversion_factor is not None and rule.provider is not None:
                            product = rule.provider.for_version(
                                self.version
                            ).reference_product.lower()
                            write_key = (id(target), product)
                            previous = writers.get(write_key)
                            if previous is not None and previous != rule.id:
                                raise MetalsConfigError(
                                    f"Overlapping material rules {previous!r} and {rule.id!r} "
                                    f"write {product!r} in {target.get('name')!r}."
                                )
                            writers[write_key] = rule.id
                        key = (id(target), rule.id)
                        if key in seen:
                            if seen[key] != conversion_factor:
                                raise MetalsConfigError(
                                    f"Rule {rule.id!r} reaches "
                                    f"{target.get('name')!r} through mapped activities "
                                    "with different conversion factors."
                                )
                            continue
                        seen[key] = conversion_factor
                        plan.append(
                            {
                                "dataset": target,
                                "technology": technology,
                                "rule": rule,
                                "conversion_factor": conversion_factor,
                            }
                        )
        return plan

    @lru_cache()
    def get_metal_market_dataset(
        self, metal_activity_name: str, reference_product: str | None = None
    ):
        """Return one exact provider using a deterministic location preference."""

        if not isinstance(metal_activity_name, str) or not metal_activity_name:
            raise ValueError(f"Invalid metal activity name: {metal_activity_name}")
        metrics = getattr(self, "material_update_metrics", None)
        if metrics is not None:
            metrics["provider index lookups"] += 1
        if not isinstance(reference_product, str) or not reference_product:
            candidates = self.db_index_by_name.get(metal_activity_name, ())
        else:
            candidates = self.db_index.get(metal_activity_name, {}).get(
                reference_product, ()
            )
        candidates = [
            dataset
            for dataset in candidates
            if dataset.get("location") in {"World", "GLO", "RoW"}
            and self.is_in_index(dataset)
        ]
        for location in ("World", "GLO", "RoW"):
            matches = [
                dataset for dataset in candidates if dataset.get("location") == location
            ]
            if len(matches) > 1:
                raise ValueError(
                    f"Ambiguous metal provider {metal_activity_name!r} / "
                    f"{reference_product!r} in {location!r}."
                )
            if matches:
                return matches[0]
        raise ws.NoResults(
            f"Could not find metal provider {metal_activity_name!r} / "
            f"{reference_product!r}."
        )

    def _material_rule_provider(self, dataset, rule, selector):
        """Prefer the market; allow only an explicitly declared existing input.

        A missing market must not lead to an arbitrary regional producer or
        another product form. Resolve a unique already used supplier in the
        current database, with the same location and a one-kg reference output.
        """
        try:
            provider = self.get_metal_market_dataset(
                selector.name, selector.reference_product
            )
            if (
                rule.existing_provider_fallback is not None
                and provider.get("unit") != "kilogram"
            ):
                raise MetalsConfigError(
                    f"Material rule {rule.id!r} requires a kilogram provider, got {provider.get('unit')!r}."
                )
            return provider, None
        except ws.NoResults as error:
            if rule.existing_provider_fallback is None:
                raise
            market_error = str(error)

        def unavailable(reason):
            raise ws.NoResults(
                f"{market_error} Existing-input fallback unavailable: {reason}."
            )

        fallback = rule.existing_provider_fallback.for_version(self.version)
        exchanges = [
            exc
            for exc in dataset.get("exchanges", ())
            if exc.get("type") == "technosphere"
            and exc.get("product", "").lower() == selector.reference_product.lower()
        ]
        if len(exchanges) != 1:
            unavailable("expected one existing material input")
        exchange = exchanges[0]
        amount = exchange.get("amount")
        if (
            exchange.get("name") != fallback.name
            or exchange.get("product") != fallback.reference_product
            or exchange.get("unit") != "kilogram"
            or not exchange.get("location")
            or exchange.get("formula")
            or isinstance(amount, bool)
            or not isinstance(amount, (int, float, np.number))
            or not np.isfinite(amount)
            or amount < 0
        ):
            unavailable("unrecognized material identity, unit or coefficient")
        providers = [
            provider
            for provider in self.db_index.get(fallback.name, {}).get(
                fallback.reference_product, ()
            )
            if provider.get("location") == exchange["location"]
            and self.is_in_index(provider)
        ]
        if len(providers) != 1:
            unavailable("missing or ambiguous supplier at the existing location")
        provider = providers[0]
        outputs = [
            exc
            for exc in provider.get("exchanges", ())
            if exc.get("type") == "production"
        ]
        if (
            provider.get("name") != fallback.name
            or provider.get("reference product") != fallback.reference_product
            or provider.get("unit") != "kilogram"
            or len(outputs) != 1
            or outputs[0].get("unit") != "kilogram"
            or outputs[0].get("product", "").lower()
            != selector.reference_product.lower()
            or outputs[0].get("formula")
            or isinstance(outputs[0].get("amount"), bool)
            or not isinstance(outputs[0].get("amount"), (int, float, np.number))
            or not np.isclose(outputs[0]["amount"], 1, rtol=0, atol=1e-12)
        ):
            unavailable("supplier is not a one-kg output of the same material")
        return provider, exchange

    def update_metal_use(
        self,
        dataset: dict,
        technology: str,
    ) -> None:
        """
        Update metal use based on metal intensity data.
        :param dataset: dataset to adjust metal use for
        :param technology: metal intensity variable name to look up
        :return: Does not return anything. Modified in place.
        """

        conversion_factor = (
            None
            if self._material_policy_for(dataset, technology)
            else self._technology_conversion_factor(dataset, technology)
        )
        for rule in self.material_rules_by_technology.get(technology, ()):
            if rule.target.kind == "mapped_activity":
                self._apply_material_rule(
                    dataset=dataset,
                    technology=technology,
                    rule=rule,
                    conversion_factor=conversion_factor,
                )

    def process_metal_update(
        self, metal_row, dataset, final_technology, technology, conversion_factor
    ):
        """Compatibility wrapper around the typed material-rule implementation."""

        matching_rules = [
            rule
            for rule in self.material_rules_by_technology.get(technology, ())
            if rule.element == metal_row.get("Element")
            and rule.provider is not None
            and rule.provider.name == metal_row.get("Activity")
            and rule.target.name in {None, final_technology}
        ]
        for rule in matching_rules:
            self._apply_material_rule(
                dataset=dataset,
                technology=technology,
                rule=rule,
                conversion_factor=conversion_factor,
            )

    def _technology_conversion_factor(
        self, dataset: dict, technology: str | None = None
    ) -> float:
        """Require a declared conversion before applying a material intensity."""

        conversion = self.technology_conversions_by_name.get(dataset.get("name"))
        if conversion is None:
            raise MetalsConfigError(
                f"Missing material-intensity conversion for {dataset.get('name')!r} "
                f"({dataset.get('unit')!r}, {dataset.get('location')!r}). "
                "Declare a unit-compatible technology conversion or an explicit "
                "preserve_source policy; no material overrides have been applied."
            )
        if dataset.get("unit") != conversion.ecoinvent_unit:
            raise MetalsConfigError(
                f"Material-intensity conversion unit mismatch for "
                f"{dataset.get('name')!r}: expected {conversion.ecoinvent_unit!r}, "
                f"got {dataset.get('unit')!r}."
            )
        if technology is not None:
            factors = self.precomputed_medians
            if technology in factors.origin_var.values:
                if "intensity_unit" not in factors.coords:
                    raise MetalsConfigError(
                        "Material intensity data is missing unit metadata; rebuild the IAM data cache."
                    )
                intensity_unit = factors.intensity_unit.sel(
                    origin_var=technology
                ).item()
                if intensity_unit != conversion.intensity_unit:
                    raise MetalsConfigError(
                        f"Material-intensity conversion unit mismatch for {dataset.get('name')!r}: "
                        f"source {intensity_unit!r}, conversion {conversion.intensity_unit!r}."
                    )
        return conversion.factor

    def _material_policy_for(
        self, dataset: dict, technology: str
    ) -> ActivityPolicy | None:
        for policy in self.material_policies:
            if policy.matches(dataset, technology):
                return policy
        return None

    @staticmethod
    def _direct_material_amount(dataset: dict, reference_product: str) -> float:
        return sum(
            exchange.get("amount", 0.0)
            for exchange in dataset.get("exchanges", ())
            if exchange.get("type") == "technosphere"
            and exchange.get("product", "").lower() == reference_product.lower()
        )

    def _record_material_decision(
        self,
        dataset: dict,
        rule: MaterialRule,
        *,
        status: str,
        reason_code: str,
        explanation: str,
        values: dict,
    ) -> None:
        self._validation_targets[id(dataset)] = dataset
        provider = (
            rule.provider.for_version(getattr(self, "version", None))
            if rule.provider
            else None
        )
        values = {
            "material rule id": rule.id,
            "technology": rule.technology,
            "element": rule.element,
            "provider name": provider.name if provider else None,
            "provider product": provider.reference_product if provider else None,
            **values,
        }
        self.material_decisions.append(
            {
                "activity": {
                    "code": dataset.get("code"),
                    "name": dataset.get("name"),
                    "reference product": dataset.get("reference product"),
                    "location": dataset.get("location"),
                    "unit": dataset.get("unit"),
                },
                "status": status,
                "reason code": reason_code,
                "explanation": explanation,
                **values,
            }
        )
        record_change_event(
            self,
            dataset,
            status,
            sector="metals",
            reason_code=reason_code,
            explanation=explanation,
            iam_variable=rule.technology,
            algorithm=rule.application,
            configuration_reference=(
                f"premise/data/metals/{MATERIAL_RULES_PATH.name}#{rule.id}"
            ),
            computed_target_values=values,
        )

    def _material_design_guard(self, dataset, technology, rule):
        """Preflight every member before changing a matched material design."""

        if not rule.design_group:
            return None
        expected = {
            r.id
            for r in load_material_rules().enabled_rules
            if r.design_group == rule.design_group
        }
        members = [
            r
            for r in self.material_rules_by_technology.get(technology, ())
            if r.design_group == rule.design_group
        ]
        if not expected or {r.id for r in members} != expected:
            return "The configured matched design is incomplete."
        for member in members:
            if member.preserve_reason or member.provider is None:
                return f"Design member {member.id!r} cannot be applied."
            try:
                factors = self.precomputed_medians.sel(
                    metal=member.element, origin_var=technology
                )
                values = [
                    float(factors.sel(variable=v).item())
                    for v in ("min", "median", "max")
                ]
            except KeyError:
                return f"Design member {member.id!r} has no intensity."
            if not all(np.isfinite(v) for v in values) or not (
                0 <= values[0] <= values[1] <= values[2]
            ):
                return f"Design member {member.id!r} has invalid intensity bounds."
            if hasattr(self, "year") and not (
                float(factors.source_year_min.item())
                <= self.year
                <= float(factors.source_year_max.item())
            ):
                return (
                    f"Design member {member.id!r} supports "
                    f"{float(factors.source_year_min.item()):g}–"
                    f"{float(factors.source_year_max.item()):g}, "
                    f"not requested year {self.year}."
                )
            if values[2] > 0:
                try:
                    provider, _ = self._material_rule_provider(
                        dataset, member, member.provider.for_version(self.version)
                    )
                    if provider.get("unit") != "kilogram":
                        raise MetalsConfigError(
                            f"Material rule {member.id!r} requires a kilogram "
                            f"provider, got {provider.get('unit')!r}."
                        )
                except ws.NoResults as error:
                    return str(error)
        return None

    def _apply_material_rule(
        self,
        *,
        dataset: dict,
        technology: str,
        rule: MaterialRule,
        conversion_factor: float | None,
    ) -> None:
        """Calculate and apply, preserve, or diagnose one material rule."""

        if rule.preserve_reason:
            self._record_material_decision(
                dataset,
                rule,
                status="skipped",
                reason_code="metals.material_rule.component_scope_unverified",
                explanation=rule.preserve_reason,
                values={},
            )
            return
        policy = self._material_policy_for(dataset, technology)
        if policy is not None:
            self._record_material_decision(
                dataset,
                rule,
                status="skipped",
                reason_code="metals.material_rule.preserved_source",
                explanation=(
                    f"Preserved {dataset['name']!r} for material rule {rule.id!r}: "
                    f"{policy.reason}"
                ),
                values={
                    "material rule id": rule.id,
                    "technology": technology,
                    "element": rule.element,
                    "activity policy id": policy.id,
                },
            )
            return
        if (
            conversion_factor is None
            or not np.isfinite(conversion_factor)
            or conversion_factor <= 0
        ):
            raise MetalsConfigError(
                f"Invalid material-intensity conversion for {dataset.get('name')!r}: "
                f"{conversion_factor!r}."
            )

        if rule.provider is None or rule.element is None:
            return
        design_error = self._material_design_guard(dataset, technology, rule)
        if design_error:
            self._record_material_decision(
                dataset,
                rule,
                status="skipped",
                reason_code="metals.material_rule.design_incomplete",
                explanation=(
                    f"Preserved matched design {rule.design_group!r}: {design_error}"
                ),
                values={
                    "design group": rule.design_group,
                    "requested year": getattr(self, "year", None),
                },
            )
            return
        provider_selector = rule.provider.for_version(self.version)
        try:
            use_factors = self.precomputed_medians.sel(
                metal=rule.element, origin_var=technology
            )
        except KeyError:
            self._record_material_decision(
                dataset,
                rule,
                status="skipped",
                reason_code="metals.material_rule.intensity_missing",
                explanation="No intensity is available for this metal/technology pair; source input preserved.",
                values={
                    "material rule id": rule.id,
                    "technology": technology,
                    "element": rule.element,
                },
            )
            return

        exchange_factor = rule.exchange_amount_factor or 1.0
        median_value = (
            use_factors.sel(variable="median").item()
            * exchange_factor
            * conversion_factor
        )
        min_value = (
            use_factors.sel(variable="min").item() * exchange_factor * conversion_factor
        )
        max_value = (
            use_factors.sel(variable="max").item() * exchange_factor * conversion_factor
        )
        if np.isnan(median_value):
            self._record_material_decision(
                dataset,
                rule,
                status="skipped",
                reason_code="metals.material_rule.intensity_missing",
                explanation="No intensity is available for this metal/technology pair; source input preserved.",
                values={
                    "material rule id": rule.id,
                    "technology": technology,
                    "element": rule.element,
                },
            )
            return
        temporal_values = {}
        if hasattr(self, "year"):
            first = float(use_factors.source_year_min.item())
            last = float(use_factors.source_year_max.item())
            temporal_values = {
                "requested year": self.year,
                "source year minimum": first,
                "source year maximum": last,
                "temporal treatment": (
                    "matched design within configured support"
                    if rule.design_group
                    else "within observed range"
                ),
            }
            if not first <= self.year <= last:
                self._record_material_decision(
                    dataset,
                    rule,
                    status="skipped",
                    reason_code="metals.material_rule.year_outside_support",
                    explanation=(
                        "Preserved source input: the requested year is outside this "
                        "metal/technology series' observation range. No endpoint "
                        "hold or extrapolation is treated as a future projection."
                    ),
                    values={**temporal_values, "temporal treatment": "preserve source"},
                )
                return
        if (
            not all(np.isfinite(v) for v in (min_value, median_value, max_value))
            or not 0 <= min_value <= median_value <= max_value
        ):
            raise MetalsConfigError(
                f"Invalid intensity bounds for material rule {rule.id!r}."
            )

        old_amount = self._direct_material_amount(
            dataset, provider_selector.reference_product
        )
        values = {
            **temporal_values,
            **({"design group": rule.design_group} if rule.design_group else {}),
            "material rule id": rule.id,
            "technology": technology,
            "element": rule.element,
            "provider name": provider_selector.name,
            "provider product": provider_selector.reference_product,
            "intensity median": use_factors.sel(variable="median").item(),
            "intensity minimum": use_factors.sel(variable="min").item(),
            "intensity maximum": use_factors.sel(variable="max").item(),
            "technology conversion factor": conversion_factor,
            "exchange amount factor": exchange_factor,
            "old direct amount": old_amount,
            "target direct amount": median_value,
        }

        if median_value == 0 and max_value == 0:
            # A known zero must remove the previous positive input even if its
            # replacement provider is absent in this database version.
            dataset["exchanges"] = [
                exc
                for exc in dataset.get("exchanges", [])
                if not (
                    exc.get("type") == "technosphere"
                    and exc.get("product", "").lower()
                    == provider_selector.reference_product.lower()
                )
            ]
            self._record_material_decision(
                dataset,
                rule,
                status="updated",
                reason_code="metals.material_rule.zero_intensity",
                explanation="Removed the direct material input because its specified intensity range is zero.",
                values=values,
            )
            return

        try:
            provider, existing_exchange = self._material_rule_provider(
                dataset, rule, provider_selector
            )
        except ws.NoResults as error:
            diagnostic = {
                **values,
                "activity name": dataset.get("name"),
                "location": dataset.get("location"),
                "error": str(error),
            }
            self.material_update_diagnostics.append(diagnostic)
            logger.warning(str(error))
            self._record_material_decision(
                dataset,
                rule,
                status="skipped",
                reason_code="metals.material_rule.provider_missing",
                explanation=str(error),
                values=diagnostic,
            )
            return

        values["requested provider name"] = provider_selector.name
        values["provider name"] = provider["name"]
        values["provider location"] = provider.get("location")
        values["provider selection"] = (
            "existing input fallback"
            if existing_exchange is not None
            else "configured market"
        )
        values["provider fallback rank"] = (
            None
            if existing_exchange is not None
            else ("World", "GLO", "RoW").index(provider.get("location"))
        )
        update_exchanges(
            activity=dataset,
            new_amount=median_value,
            new_provider=provider,
            metal=rule.element,
            min_value=min_value,
            max_value=max_value,
        )
        if existing_exchange is not None:
            note = (
                f"Premise material rule {rule.id}: configured market unavailable; "
                "retained the existing refinery supplier and geography. "
                f"Set {median_value:.15g} kg {provider_selector.reference_product} "
                "per activity output using the configured intensity and explicit "
                "technology conversion. This does not validate the catalyst design."
            )
            original_comment = str(existing_exchange.get("comment") or "")
            dataset["exchanges"][-1]["comment"] = (
                original_comment
                if note in original_comment
                else (original_comment + "\n" + note).strip()
            )
        if rule.design_group and rule.comment:
            note = f"Matched material design {rule.design_group}: {rule.comment}"
            comment = str(dataset["exchanges"][-1].get("comment") or "")
            if note not in comment:
                dataset["exchanges"][-1]["comment"] = (comment + "\n" + note).strip()
        self._record_material_decision(
            dataset,
            rule,
            status="updated",
            reason_code="metals.material_rule.applied",
            explanation=(
                f"Set the direct {provider_selector.reference_product!r} input using "
                f"material rule {rule.id!r}."
            ),
            values=values,
        )

    def post_allocation_correction(self):
        """
        Correct metal resource flows distorted by economic allocation.

        The correction scans non-market datasets from the mining-share mapping
        and additional extraction-like datasets. When an in-ground kilogram
        natural resource flow can be unambiguously matched to the dataset
        reference product, that target flow is set to the target-metal content
        of one kilogram of the reference product. Pure target-metal products
        are set to 1 kg; metal-bearing intermediates use explicit content
        factors. Other co-mined metal resource flows are set to zero;
        non-metal resource flows are preserved.
        """

        if getattr(self, "system_model", None) == "consequential":
            self._record_resource_diagnostic(
                {"name": "consequential resource inventory"},
                "preserved_consequential_substitution",
            )
            return

        self.build_in_ground_resource_exchange_index()
        from .metals_resource_boundaries import build_resource_boundary_plan

        resource_flow_names = (
            self.get_existing_in_ground_resource_flow_names()
            | self.get_biosphere_in_ground_resource_flow_names()
        )
        boundary_plan = build_resource_boundary_plan(self.database, resource_flow_names)
        considered_dataset_ids = (
            self.get_considered_metal_dataset_ids() - boundary_plan.reserved_ids
        )
        strict_dataset_ids = (
            self.get_mapped_metal_dataset_ids() - boundary_plan.reserved_ids
        )
        missing_target_dataset_ids = self.get_missing_target_resource_dataset_ids(
            considered_dataset_ids
        )
        provider_index = self.build_exchange_provider_index()
        upstream = {}
        for ds in self.database:
            if id(ds) not in considered_dataset_ids:
                continue
            matches = get_target_resource_exchanges(
                ds, self._get_in_ground_resource_exchanges(ds)
            )
            if len(matches) != 1:
                continue
            # A mine's reagents can contain the same element as its output.
            # That upstream demand does not replace extraction at the mine.
            role = classify_resource_activity(ds, id(ds) in strict_dataset_ids)
            target_amount = get_target_resource_amount(ds, matches[0])[0]
            feed_mass = sum(
                exc.get("amount", 0)
                for exc in ds.get("exchanges", [])
                if exc.get("type") == "technosphere"
                and exc.get("unit") == "kilogram"
                and exc.get("amount", 0) > 0
                and exchange_may_carry_target_resource(exc, matches[0]["name"])
            )
            if (
                role == "primary_or_coproduct"
                and target_amount > 0
                and feed_mass >= target_amount
            ):
                upstream[id(ds)] = self.target_resource_is_supplied_upstream(
                    ds,
                    matches[0]["name"],
                    provider_index,
                    {},
                    planned_resource_ids=missing_target_dataset_ids,
                )

        for ds in self.database:
            if id(ds) not in considered_dataset_ids:
                continue

            target_resource_flow_name = None
            if id(ds) in missing_target_dataset_ids:
                inferred_flow_name = get_reference_product_resource_flow_name(
                    ds.get("reference product", "")
                )
                target_resource_flow_name = resolve_existing_resource_flow_name(
                    inferred_flow_name, resource_flow_names
                )

            resource_exchanges = self._get_in_ground_resource_exchanges(ds)
            before = {
                (
                    exchange.get("name"),
                    tuple(exchange.get("categories", ())),
                    exchange.get("unit"),
                ): exchange.get("amount")
                for exchange in resource_exchanges
            }
            role = classify_resource_activity(ds, id(ds) in strict_dataset_ids)
            if upstream.get(id(ds)) is True and role not in {
                "secondary",
                "market",
                "unsupported_unit",
            }:
                for exchange in resource_exchanges:
                    if is_metal_resource_flow(exchange.get("name", "")):
                        set_resource_amount(exchange, 0.0)
                corrected = True
            elif id(ds) in upstream and upstream[id(ds)] is None:
                corrected = False
                self._record_resource_diagnostic(ds, "upstream_unresolved")
            else:
                corrected = correct_metal_resource_exchanges(
                    ds,
                    strict=id(ds) in strict_dataset_ids,
                    add_missing_target_resource=id(ds) in missing_target_dataset_ids,
                    target_resource_flow_name=target_resource_flow_name,
                    resource_exchanges=resource_exchanges,
                )
            if not corrected:
                self._record_resource_diagnostic(ds, f"preserved_{role}")
            after = {
                (
                    exchange.get("name"),
                    tuple(exchange.get("categories", ())),
                    exchange.get("unit"),
                ): exchange.get("amount")
                for exchange in get_in_ground_resource_exchanges(ds)
            }
            if corrected and before != after:
                self._validation_targets[id(ds)] = ds
                record_change_event(
                    self,
                    ds,
                    "updated",
                    sector="metals",
                    reason_code="metals.post_allocation_resource_correction",
                    explanation=(
                        "Corrected in-ground metal resource flows after economic "
                        "allocation."
                    ),
                    algorithm="post-allocation resource-flow correction",
                    computed_target_values={
                        "old resource amounts": {
                            str(key): value for key, value in before.items()
                        },
                        "new resource amounts": {
                            str(key): value for key, value in after.items()
                        },
                    },
                )

        self._apply_resource_boundary_plan(boundary_plan)

    def _apply_resource_boundary_plan(self, plan):
        """Apply validated elementary-flow assignments without changing suppliers."""
        from .metals_resource_boundaries import ground_resources

        for dataset, reason in plan.diagnostics:
            self._record_resource_diagnostic(dataset, reason)
        for dataset, element, amount, reason in plan.assignments:
            matches = ground_resources(dataset, element)
            old_amount = matches[0]["amount"] if matches else 0.0
            if old_amount == amount:
                continue
            if matches:
                set_resource_amount(matches[0], amount)
            else:
                dataset.setdefault("exchanges", []).append(
                    make_target_resource_exchange(element, amount)
                )
            self._in_ground_resource_exchange_index[id(dataset)] = (
                get_in_ground_resource_exchanges(dataset)
            )
            self._validation_targets[id(dataset)] = dataset
            self._record_resource_diagnostic(dataset, reason)
            record_change_event(
                self,
                dataset,
                "updated",
                sector="metals",
                reason_code="metals.scoped_resource_attribution",
                explanation="Applied a dataset-specific primary resource attribution boundary.",
                algorithm=reason,
                computed_target_values={
                    "resource": element,
                    "old amount": old_amount,
                    "new amount": amount,
                },
            )

    def _record_resource_diagnostic(self, dataset, reason):
        if not hasattr(self, "resource_diagnostics"):
            self.resource_diagnostics = []
        self.resource_diagnostics.append(
            {
                "name": dataset.get("name"),
                "product": dataset.get("reference product"),
                "location": dataset.get("location"),
                "reason": reason,
            }
        )

    def build_in_ground_resource_exchange_index(self) -> None:
        """Index qualifying in-ground exchanges once for the correction pass."""

        index = {}
        for dataset in self.database:
            resource_exchanges = get_in_ground_resource_exchanges(dataset)
            if resource_exchanges:
                index[id(dataset)] = resource_exchanges
        self._in_ground_resource_exchange_index = index

    def _get_in_ground_resource_exchanges(self, dataset: dict) -> List[dict]:
        """Return indexed resource exchanges or use the standalone fallback."""

        index = getattr(self, "_in_ground_resource_exchange_index", None)
        if index is None:
            return get_in_ground_resource_exchanges(dataset)
        return index.get(id(dataset), [])

    def get_considered_metal_dataset_ids(self) -> Set[int]:
        """
        Return dataset object IDs considered for post-allocation correction.

        The primary source is the metals mining-share mapping. A secondary scan
        catches extraction-like non-market datasets with in-ground resource
        flows.
        """
        dataset_ids = self.get_mining_share_dataset_ids()

        for dataset in self.database:
            if is_market_dataset(dataset) or id(dataset) in dataset_ids:
                continue

            resource_exchanges = self._get_in_ground_resource_exchanges(dataset)
            if any(
                is_metal_resource_flow(exc.get("name", ""))
                for exc in resource_exchanges
            ) and (
                has_metal_extraction_context(dataset)
                or has_resolved_target_resource_context(dataset)
                or is_downstream_attributed_resource_carrier_dataset(dataset)
            ):
                dataset_ids.add(id(dataset))
                continue

            if not resource_exchanges and can_add_missing_target_resource_exchange(
                dataset
            ):
                dataset_ids.add(id(dataset))
                continue

        return dataset_ids

    def get_existing_in_ground_resource_flow_names(self) -> Set[str]:
        """Return in-ground resource flow names already used in the database."""
        return {
            exc.get("name", "")
            for dataset in self.database
            for exc in self._get_in_ground_resource_exchanges(dataset)
            if exc.get("name")
        }

    def get_biosphere_in_ground_resource_flow_names(self) -> Set[str]:
        """Return version-specific in-ground resource flow names from biosphere."""
        return {
            name
            for name, category, subcategory, unit in biosphere_flows_dictionary(
                version=self.version
            )
            if (category, subcategory) == NATURAL_RESOURCE_IN_GROUND
            and unit == "kilogram"
        }

    def build_exchange_provider_index(self) -> Dict[tuple, List[dict]]:
        """Return exact provider lookup for technosphere exchanges."""
        provider_index = defaultdict(list)
        for dataset in self.database:
            provider_index[
                (
                    dataset.get("name"),
                    dataset.get("reference product"),
                    dataset.get("location"),
                )
            ].append(dataset)

        return provider_index

    @staticmethod
    def dataset_has_target_resource_exchange(
        dataset: dict,
        flow_name: str,
        resource_exchanges: Optional[List[dict]] = None,
    ) -> bool:
        """Return True if a dataset directly extracts the target resource."""
        flow_label = canonical_resource_flow_label(flow_name)
        if resource_exchanges is None:
            resource_exchanges = get_in_ground_resource_exchanges(dataset)
        return any(
            canonical_resource_flow_label(exc.get("name", "")) == flow_label
            and abs(float(exc.get("amount", 0.0))) > 0
            for exc in resource_exchanges
        )

    def target_resource_is_supplied_upstream(
        self,
        dataset: dict,
        flow_name: str,
        provider_index: Dict[tuple, List[dict]],
        cache: dict,
        max_depth: int = 12,
        visited: Optional[Set[int]] = None,
        planned_resource_ids: Optional[Set[int]] = None,
    ) -> Optional[bool]:
        """Return True if direct technosphere suppliers already carry the target."""
        if max_depth < 0:
            return None

        cache_key = (id(dataset), normalize_resource_label(flow_name), max_depth)
        if cache_key in cache:
            return cache[cache_key]

        visited = visited or set()
        visited.add(id(dataset))
        unresolved = False

        for exc in dataset.get("exchanges", []):
            if exc.get("type") != "technosphere" or exc.get("amount", 0) <= 0:
                continue
            if not exchange_may_carry_target_resource(exc, flow_name):
                continue

            providers = provider_index.get(
                (exc.get("name"), exc.get("product"), exc.get("location")),
                [],
            )
            if len(providers) != 1:
                unresolved = True
                continue
            for provider in providers:
                if id(provider) in visited:
                    unresolved = True
                    continue
                if is_downstream_attributed_resource_carrier_dataset(provider):
                    continue
                if dataset_may_carry_target_resource(provider, flow_name) and (
                    id(provider) in (planned_resource_ids or set())
                    or self.dataset_has_target_resource_exchange(
                        provider,
                        flow_name,
                        self._get_in_ground_resource_exchanges(provider),
                    )
                ):
                    cache[cache_key] = True
                    return True
                supplied = self.target_resource_is_supplied_upstream(
                    provider,
                    flow_name,
                    provider_index,
                    cache,
                    max_depth=max_depth - 1,
                    visited=visited.copy(),
                    planned_resource_ids=planned_resource_ids,
                )
                if supplied is True:
                    cache[cache_key] = True
                    return True
                unresolved |= supplied is None

        # Negative reachability depends on the visited path; never cache it.
        return None if unresolved else False

    @staticmethod
    def has_primary_resource_attribution_feed(
        dataset: dict, flow_name: str, provider_index: Dict[tuple, List[dict]]
    ) -> bool:
        """Recognize an explicit primary feed despite circular auxiliary inputs.

        All target-bearing links must resolve uniquely. The configured carrier
        feed must weigh at least the pure-metal output, and the other inputs
        must weigh less than that output even assuming 100% metal content. These
        checks bound the rule's applicability; they do not estimate recovery.
        """
        feed_product = PRIMARY_RESOURCE_ATTRIBUTION_FEEDS.get(
            (dataset.get("name"), dataset.get("reference product"))
        )
        if not feed_product or not can_add_missing_target_resource_exchange(
            dataset, primary_route=True
        ):
            return False

        feed_mass, other_mass = 0.0, 0.0
        for exc in dataset.get("exchanges", []):
            if (
                exc.get("type") != "technosphere"
                or exc.get("amount", 0) <= 0
                or not exchange_may_carry_target_resource(exc, flow_name)
            ):
                continue
            providers = provider_index.get(
                (exc.get("name"), exc.get("product"), exc.get("location")), []
            )
            if len(providers) != 1 or providers[0].get("unit") != "kilogram":
                return False
            if exc.get("product") == feed_product:
                if not is_downstream_attributed_resource_carrier_dataset(providers[0]):
                    return False
                feed_mass += exc["amount"]
            else:
                other_mass += exc["amount"]

        return feed_mass >= 1.0 and other_mass < 1.0

    def get_missing_target_resource_dataset_ids(
        self, strict_dataset_ids: Set[int]
    ) -> Set[int]:
        """
        Return mapped pure-metal datasets that need a direct target resource flow.

        A missing direct flow is only added when the supply chain does not
        already provide the target resource through a target-bearing product.
        This avoids double-counting normal production chains whose upstream ore
        or concentrate dataset is corrected separately, while still adding the
        target flow for eligible primary byproduct routes whose upstream
        co-mined flows are zeroed or absent. Recycling routes are excluded.
        """
        provider_index = self.build_exchange_provider_index()
        cache = {}
        # Plan the explicitly declared primary boundaries together, before any
        # regional supplier is examined. Otherwise a surviving Canadian zinc
        # flow can conceal missing extraction on all the other market branches.
        dataset_ids = {
            id(ds)
            for ds in self.database
            if id(ds) in strict_dataset_ids
            and not self._get_in_ground_resource_exchanges(ds)
            and self.has_primary_resource_attribution_feed(
                ds,
                get_reference_product_resource_flow_name(
                    ds.get("reference product", "")
                ),
                provider_index,
            )
        }
        # Potential extraction boundaries without target-bearing inputs. Resolve
        # these before downstream refiners so two missing flows cannot both be
        # filled along the same chain merely because of database iteration order.
        leaf_candidates = dataset_ids | {
            id(ds)
            for ds in self.database
            if id(ds) in strict_dataset_ids
            and not self._get_in_ground_resource_exchanges(ds)
            and can_add_missing_target_resource_exchange(ds, primary_route=True)
            and (ds.get("name"), ds.get("reference product"))
            not in PRIMARY_RESOURCE_ATTRIBUTION_FEEDS
            and not any(
                exc.get("type") == "technosphere"
                and exc.get("amount", 0) > 0
                and exchange_may_carry_target_resource(
                    exc,
                    get_reference_product_resource_flow_name(ds["reference product"]),
                )
                for exc in ds.get("exchanges", [])
            )
        }

        for dataset in self.database:
            if id(dataset) not in strict_dataset_ids:
                continue
            if id(dataset) in dataset_ids:
                self._record_resource_diagnostic(dataset, "primary_feed_attribution")
                continue
            if self._get_in_ground_resource_exchanges(dataset):
                continue
            if not can_add_missing_target_resource_exchange(
                dataset, primary_route=True
            ):
                continue
            if (
                dataset.get("name"),
                dataset.get("reference product"),
            ) in PRIMARY_RESOURCE_ATTRIBUTION_FEEDS:
                # A declared boundary that failed its feed checks must not fall
                # back to the generic absence rule or become a planned leaf.
                self._record_resource_diagnostic(dataset, "primary_feed_unresolved")
                continue

            flow_name = get_reference_product_resource_flow_name(
                dataset.get("reference product", "")
            )
            if not flow_name:
                continue
            supplied = self.target_resource_is_supplied_upstream(
                dataset,
                flow_name,
                provider_index,
                cache,
                planned_resource_ids=leaf_candidates,
            )
            if supplied is not False:
                if supplied is None:
                    self._record_resource_diagnostic(dataset, "upstream_unresolved")
                continue

            dataset_ids.add(id(dataset))

        return dataset_ids

    def get_mining_share_dataset_ids(self) -> Set[int]:
        """Return dataset object IDs matched by the mining-share mapping."""
        cached = getattr(self, "_mining_share_dataset_ids_cache", None)
        if cached is not None:
            return set(cached)

        dataframe = load_mining_shares_mapping(self.version)
        dataframe = dataframe.loc[dataframe["Work done"] == "Yes"]

        dataset_ids = set()
        grouped = dataframe[["Process", "Reference product"]].drop_duplicates()

        for _, row in grouped.iterrows():
            try:
                proc_filter = parse_mining_filter(row["Process"])
                ref_prod_filter = parse_mining_filter(row["Reference product"])
            except (ValueError, SyntaxError) as exc:
                logger.warning(
                    f"[Metals] Invalid mining-share mapping filter skipped: {exc}"
                )
                continue

            for dataset in self.get_datasets_matching_filters(
                proc_filter, ref_prod_filter
            ):
                if is_market_dataset(dataset):
                    continue
                if self._get_in_ground_resource_exchanges(
                    dataset
                ) or can_add_missing_target_resource_exchange(
                    dataset, primary_route=True
                ):
                    dataset_ids.add(id(dataset))

        self._mining_share_dataset_ids_cache = frozenset(dataset_ids)
        return set(self._mining_share_dataset_ids_cache)

    def get_mapped_metal_dataset_ids(self) -> Set[int]:
        """Return mapped dataset IDs with one unambiguous target resource flow."""
        dataset_ids = set()
        mining_share_dataset_ids = self.get_mining_share_dataset_ids()

        for dataset in self.database:
            if id(dataset) not in mining_share_dataset_ids:
                continue
            matches = get_target_resource_exchanges(
                dataset, self._get_in_ground_resource_exchanges(dataset)
            )
            matched_names = {exc.get("name") for exc in matches}
            if len(matched_names) == 1 or can_add_missing_target_resource_exchange(
                dataset
            ):
                dataset_ids.add(id(dataset))

        return dataset_ids

    def get_shares(
        self, df: pd.DataFrame, new_locations: dict, name, ref_prod, normalize=True
    ) -> dict:
        """
        Get shares of each location in the dataframe.
        :param df: Dataframe with mining shares
        :param new_locations: List of new locations
        :return: Dictionary with shares of each location
        """
        shares = {}

        selected = mining_shares_for_year(df, self.year)
        if selected is None:
            return {}
        for long_location, short_location in new_locations.items():
            share = selected.loc[df["Country"] == long_location]
            if len(share) > 0:
                share = share.sum()
                if not np.isfinite(share) or share < 0:
                    raise MetalsConfigError(
                        f"Invalid mining share for {name!r} in {long_location!r}: {share!r}"
                    )
                key = (name, ref_prod, short_location)
                shares[key] = shares.get(key, 0.0) + share

        shares = {k: v for k, v in shares.items() if v > 0}
        if not shares:
            return {}

        if normalize:
            total = sum(shares.values())
            shares = {k: v / total for k, v in shares.items()}

        return shares

    def get_geo_mapping(self, df: pd.DataFrame, new_locations: dict) -> dict:
        mapping = {}

        regions_df = df[["Country", "Region"]].drop_duplicates()
        for long_loc, iso2 in new_locations.items():
            region_row = regions_df.loc[regions_df["Country"] == long_loc]
            if not region_row.empty:
                mapping[iso2] = region_row["Region"].iloc[0]

        return mapping

    def build_db_indexes(self):
        self.db_index_by_name = defaultdict(list)
        self.db_index = defaultdict(lambda: defaultdict(list))
        self.db_index_full = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for ds in self.database:
            name = ds.get("name")
            ref_prod = ds.get("reference product")
            location = ds.get("location")

            self.db_index_by_name[name].append(ds)
            self.db_index[name][ref_prod].append(ds)
            self.db_index_full[name][ref_prod][location].append(ds)

    def get_datasets_matching_filters(self, process_filter, reference_product_filter):
        """Use the activity index for exact filters and scan only as fallback."""

        names = extract_exact_filter_values(process_filter)
        reference_products = extract_exact_filter_values(reference_product_filter)
        if (
            names is not None
            and reference_products is not None
            and hasattr(self, "db_index")
        ):
            candidates = [
                dataset
                for name in names
                for reference_product in reference_products
                for dataset in self.db_index.get(name, {}).get(reference_product, [])
            ]
            if len(names) == 1 and len(reference_products) == 1:
                return candidates

            # Wurst yields matches in database order. Preserve that observable
            # behavior for filters containing several exact alternatives.
            candidate_ids = {id(dataset) for dataset in candidates}
            return [
                dataset for dataset in self.database if id(dataset) in candidate_ids
            ]

        return [
            dataset
            for dataset in self.database
            if matches_filter_query(dataset.get("name", ""), process_filter)
            and matches_filter_query(
                dataset.get("reference product", ""), reference_product_filter
            )
        ]

    def create_region_specific_markets(self, df: pd.DataFrame) -> List[dict]:
        new_exchanges, new_datasets = [], []

        df["Process_str"] = df["Process"].apply(str)
        df["Reference_product_str"] = df["Reference product"].apply(str)

        for (_, _), group in df.groupby(["Process_str", "Reference_product_str"]):
            try:
                proc_filter = parse_mining_filter(group["Process"].iloc[0])
                ref_prod_filter = parse_mining_filter(
                    group["Reference product"].iloc[0]
                )
            except (ValueError, SyntaxError) as exc:
                raise MetalsConfigError(
                    f"Invalid mining supplier selector: {exc}"
                ) from exc

            try:
                subset = self.get_datasets_matching_filters(
                    proc_filter, ref_prod_filter
                )
            except (TypeError, ValueError) as e:
                logger.error(
                    f"[Metals] Error fetching datasets for process '{proc_filter}' and reference product '{ref_prod_filter}': {e}"
                )
                print(
                    f"failed with process '{proc_filter}' and reference product '{ref_prod_filter}"
                )
                continue

            if not subset:
                raise MetalsConfigError(
                    f"[Metals] No datasets found for filter combination:\n"
                    f"  name: {proc_filter}\n"
                    f"  reference product: {ref_prod_filter}"
                )

            first_match = subset[0]
            name = first_match["name"]
            ref_prod = first_match["reference product"]

            new_locations = {
                c: self.country_codes[c] for c in group["Country"].unique()
            }

            # fetch shares for each location in df
            # Do not normalize yet - THIS WAS CAUSING A DOUBLE NORMALIZATION AND BREAKING THINGS!
            shares = self.get_shares(
                group, new_locations, name, ref_prod, normalize=False
            )
            if not shares:
                continue
            new_locations = {
                c: loc
                for c, loc in new_locations.items()
                if (name, ref_prod, loc) in shares
            }
            geography_mapping = self.get_geo_mapping(group, new_locations)

            # if not, we create it
            datasets = self.create_new_mining_activity(
                name=name,
                reference_product=ref_prod,
                new_locations=new_locations,
                geography_mapping=geography_mapping,
            )

            new_datasets.extend(datasets.values())

            new_exchanges.extend(
                [
                    {
                        "name": k[0],
                        "product": k[1],
                        "location": k[2],
                        "unit": "kilogram",
                        "amount": share,
                        "type": "technosphere",
                    }
                    for k, share in shares.items()
                ]
            )

        # Normalize shares to sum to 1
        total = sum(exc["amount"] for exc in new_exchanges)
        if total > 0:
            for exc in new_exchanges:
                exc["amount"] /= total

        for dataset in new_datasets:
            self.database.append(dataset)
            self.add_to_index(dataset)
            self.write_log(dataset, "created")

        self._market_provider_index = self.build_exchange_provider_index()
        unresolved = [exc for exc in new_exchanges if not self.activity_exists(exc)]
        if unresolved:
            raise MetalsConfigError(f"Unresolved mining suppliers: {unresolved!r}")

        return new_exchanges

    def get_old_market_reference_products(self, df_metal: pd.DataFrame) -> List[str]:
        """Return exact old-market reference products from mining-share filters."""
        reference_products = []

        for value in df_metal["Reference product"].dropna().unique():
            for product in extract_reference_products_from_filter(value):
                if product and product not in reference_products:
                    reference_products.append(product)

        return reference_products

    def get_existing_metal_markets(self, df_metal: pd.DataFrame) -> List[dict]:
        """
        Return existing ecoinvent markets that can source secondary supply.

        The mining-share mapping may point to a fallback reference product that is
        different from the new market key, e.g. lithium carbonate for lithium
        carbonate, battery grade.
        """
        markets = []
        seen = set()

        for reference_product in self.get_old_market_reference_products(df_metal):
            market_name = f"market for {reference_product}"
            found_for_product = False

            if hasattr(self, "db_index_full"):
                for location in OLD_METAL_MARKET_LOCATIONS:
                    for dataset in (
                        self.db_index_full.get(market_name, {})
                        .get(reference_product, {})
                        .get(location, [])
                    ):
                        if id(dataset) not in seen:
                            markets.append(dataset)
                            seen.add(id(dataset))
                            found_for_product = True

            if not found_for_product:
                for dataset in ws.get_many(
                    self.database,
                    ws.equals("name", market_name),
                    ws.equals("reference product", reference_product),
                    ws.either(
                        *[
                            ws.equals("location", location)
                            for location in OLD_METAL_MARKET_LOCATIONS
                        ]
                    ),
                ):
                    if id(dataset) not in seen:
                        markets.append(dataset)
                        seen.add(id(dataset))

        markets.sort(
            key=lambda dataset: (
                OLD_METAL_MARKET_LOCATIONS.index(dataset["location"])
                if dataset.get("location") in OLD_METAL_MARKET_LOCATIONS
                else len(OLD_METAL_MARKET_LOCATIONS)
            )
        )
        return markets

    def get_existing_metal_market(self, df_metal: pd.DataFrame) -> Optional[dict]:
        """Return the first existing ecoinvent market candidate."""
        markets = self.get_existing_metal_markets(df_metal)
        if not markets:
            return None

        if len(markets) > 1:
            logger.warning(
                "[Metals] Multiple existing markets found for secondary supply "
                f"source; using {markets[0]['name']} | "
                f"{markets[0]['reference product']} | {markets[0]['location']}."
            )

        return markets[0]

    def build_secondary_market_exchanges_from_existing_market(
        self, df_metal: pd.DataFrame
    ) -> List[dict]:
        """Copy secondary-supply exchanges from the existing ecoinvent market."""
        if self.system_model == "consequential":
            return []

        for old_market in self.get_existing_metal_markets(df_metal):
            reference_product = old_market["reference product"]
            exchanges = [
                {
                    "name": exc["name"],
                    "product": exc["product"],
                    "location": exc["location"],
                    "amount": float(exc["amount"]),
                    "type": "technosphere",
                    "unit": exc["unit"],
                }
                for exc in old_market.get("exchanges", [])
                if is_secondary_metal_supply_exchange(exc, reference_product)
            ]

            secondary_share = sum(exc["amount"] for exc in exchanges)
            if secondary_share <= 0:
                continue

            if secondary_share > 1 + 1e-6:
                logger.warning(
                    "[Metals] Existing market secondary exchanges exceed one "
                    f"for {old_market['name']} | {reference_product} | "
                    f"{old_market['location']}: {secondary_share}. "
                    "Trying the next old market candidate."
                )
                continue

            return exchanges

        return []

    def create_market(self, metal, df) -> Optional[dict]:
        """
        Create regionalized technosphere exchanges for a metal market based on production shares.

        This function reads a DataFrame containing information on mining activity filters
        (process and reference product), countries, and associated production shares.
        For each unique combination of process and reference product filters, it:
        - Finds matching datasets in the database using flexible filter logic.
        - Derives geographic mappings and production shares.
        - Creates new mining activities for countries where no dataset exists yet.
        - Appends these to the database and builds the corresponding technosphere exchanges.

        The function also normalizes all generated exchanges to sum to 1.

        Args:
            df (pd.DataFrame): A dataframe containing the following columns:
                - 'Process': dict defining filter(s) for activity names.
                - 'Reference product': dict defining filter(s) for reference products.
                - 'Country': country names to create new datasets for.
                - Production share columns per year (e.g., '2020', '2030', ...)
        """
        if getattr(self, "system_model", None) == "consequential":
            self._record_market_diagnostic(
                "market_preserved_consequential",
                metal=metal,
                explanation="Historical average production shares do not identify marginal suppliers.",
            )
            return None

        if hasattr(self, "year"):
            shares = mining_shares_for_year(df, self.year)
            if shares is None:
                years = sorted(int(str(c)) for c in df.columns if str(c).isdigit())
                self._record_market_diagnostic(
                    "market_preserved_year_outside_support",
                    metal=metal,
                    requested_year=self.year,
                    first_year=years[0],
                    last_year=years[-1],
                )
                return None
            # Zero routes do not supply this market and need no provider.
            df = df.loc[shares > 0].copy()
            shares = shares.loc[shares > 0]
            total = shares.sum()
            if total <= 0:
                self._record_market_diagnostic(
                    "market_preserved_zero_supply", metal=metal
                )
                return None
            missing = {}
            for country, share in shares.groupby(df["Country"]).sum().items():
                location = self.country_codes[country]
                if self.get_weighted_average_distance(location, metal).empty:
                    missing[location] = missing.get(location, 0) + float(share / total)
            if missing:
                self._record_market_diagnostic(
                    "market_preserved_transport_missing",
                    metal=metal,
                    missing_primary_fraction=sum(missing.values()),
                    missing_origins=missing,
                )
                return None

        # Preserve the entire source market if any positive primary route is
        # unavailable. Renormalizing only the available routes hides lost supply.
        for _, row in df.drop_duplicates(["Process", "Reference product"]).iterrows():
            if not self.get_datasets_matching_filters(
                parse_mining_filter(row["Process"]),
                parse_mining_filter(row["Reference product"]),
            ):
                self._record_market_diagnostic(
                    "market_preserved_supplier_missing",
                    metal=metal,
                    process=row["Process"],
                    product=row["Reference product"],
                )
                return None
        primary_exchanges = self.create_region_specific_markets(df)

        if not primary_exchanges:
            logger.warning(
                f"[Metals] No primary exchanges created for {metal}. Skipping market creation."
            )
            return None

        ref_prod = metal
        market_name = f"market for {ref_prod}"

        dataset = {
            "name": market_name,
            "location": "World",
            "exchanges": [
                {
                    "name": market_name,
                    "product": ref_prod,
                    "location": "World",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                }
            ],
            "reference product": ref_prod,
            "unit": "kilogram",
            "production amount": 1,
            "comment": "Created by premise",
            "code": str(uuid.uuid4()),
        }

        secondary_exchanges = (
            self.build_secondary_market_exchanges_from_existing_market(df)
        )
        if secondary_exchanges:
            s_share = sum(exc["amount"] for exc in secondary_exchanges)
            p_share = max(0, 1 - s_share)
        else:
            p_share = 1.0
            s_share = 0.0
            secondary_exchanges = []

        # add mining exchanges
        # dataset["exchanges"].extend(self.create_region_specific_markets(df))
        for exc in primary_exchanges:
            exc["amount"] *= p_share
        dataset["exchanges"].extend(primary_exchanges)

        # add transport exchanges
        # Primary exchanges already include the primary fraction. Secondary
        # transport is embodied in the retained recycling supply chains.
        trspt_exc = self.add_transport_to_market(dataset, metal)
        if len(trspt_exc) > 0:
            dataset["exchanges"].extend(trspt_exc)
        dataset["exchanges"].extend(secondary_exchanges)

        unresolved = [
            exc for exc in dataset["exchanges"] if not self.activity_exists(exc)
        ]
        if unresolved:
            raise MetalsConfigError(
                f"Unresolved suppliers in {market_name!r}: {unresolved!r}"
            )
        if not hasattr(self, "applied_market_shares"):
            self.applied_market_shares = {}
        self.applied_market_shares[metal] = {
            "primary share": p_share,
            "secondary share": s_share,
            "primary suppliers": [dict(exc) for exc in primary_exchanges],
            "secondary suppliers": [dict(exc) for exc in secondary_exchanges],
        }

        # remove old market dataset
        for old_market in ws.get_many(
            self.database,
            ws.equals("name", dataset["name"]),
            ws.equals("reference product", dataset["reference product"]),
            ws.exclude(ws.equals("location", "World")),
        ):
            self.remove_from_index(old_market)
            if self.is_in_index(old_market):
                key = (
                    old_market["name"],
                    old_market["reference product"],
                    old_market["location"],
                )
                raise PostAllocationCorrectionError(
                    f"Metal market {key!r} remains in the provider index after removal."
                )

        return dataset

    def activity_exists(self, exchange: dict) -> bool:
        """Check if an activity referenced by an exchange exists in the database."""
        if exchange.get("type") != "technosphere":
            return True

        if hasattr(self, "_market_provider_index"):
            matches = self._market_provider_index.get(
                (
                    exchange.get("name"),
                    exchange.get("product"),
                    exchange.get("location"),
                ),
                [],
            )
            return len(matches) == 1 and matches[0].get("unit") == exchange.get("unit")

        activities = list(
            ws.get_many(
                self.database,
                ws.equals("name", exchange["name"]),
                ws.equals("reference product", exchange["product"]),
                ws.equals("location", exchange["location"]),
            )
        )

        return len(activities) == 1

    def substitute_old_markets(self, new_dataset: dict, df_metal: pd.DataFrame) -> None:
        """
        Substitute all old market links with the new 'World' market for the given metal key.
        Uses flexible matching based on Reference product field in Excel.
        """
        old_ref_products = self.get_old_market_reference_products(df_metal)

        for ref_prod in old_ref_products:
            old_markets = list(
                ws.get_many(
                    self.database,
                    ws.equals("name", f"market for {ref_prod}"),
                    ws.equals("reference product", ref_prod),
                    ws.either(
                        ws.equals("location", "GLO"), ws.equals("location", "RoW")
                    ),
                )
            )

            for old_market in old_markets:
                consumers = [
                    ds
                    for ds in self.database
                    if any(
                        exc.get("type") == "technosphere"
                        and exc.get("name") == old_market["name"]
                        and exc.get("product") == old_market["reference product"]
                        and exc.get("location") == old_market["location"]
                        for exc in ds.get("exchanges", [])
                    )
                ]

                for consumer in consumers:
                    modified = False
                    for exc in consumer["exchanges"]:
                        if (
                            exc.get("name") == old_market["name"]
                            and exc.get("product") == old_market["reference product"]
                            and exc.get("location") == old_market["location"]
                            and exc.get("type") == "technosphere"
                        ):
                            exc.update(
                                {
                                    "name": new_dataset["name"],
                                    "product": new_dataset["reference product"],
                                    "location": new_dataset["location"],
                                }
                            )
                            exc.pop("input", None)
                            modified = True

                    if modified:
                        self.write_log(consumer, "relinked to new metal market")

    def create_new_mining_activity(
        self,
        name: str,
        reference_product: str,
        new_locations: dict,
        geography_mapping: dict = None,
    ) -> dict:
        """
        Create new mining activities for specified locations if they do not exist.

        We try to use the geography_mapping to find the correct location (Region column) in the database.
        It falls back to any matching activity if the specified region does not work.
        """

        candidates = self.db_index.get(name, {}).get(reference_product, [])
        if not candidates:
            raise MetalsConfigError(f"No proxy for {name!r} / {reference_product!r}.")
        by_location = defaultdict(list)
        for candidate in candidates:
            by_location[candidate["location"]].append(candidate)
        fallback = sorted(
            by_location, key=lambda loc: (loc != "RoW", loc != "GLO", loc)
        )[0]
        geo_map_filtered = {}
        for location in sorted(set(new_locations.values())):
            identity = {
                "name": name,
                "reference product": reference_product,
                "location": location,
            }
            if self.is_in_index(identity):
                continue
            source = (geography_mapping or {}).get(location)
            if source not in by_location:
                self._record_market_diagnostic(
                    "proxy_fallback",
                    name=name,
                    product=reference_product,
                    country=location,
                    requested=source,
                    selected=fallback,
                )
                source = fallback
            if len(by_location[source]) != 1:
                raise MetalsConfigError(
                    f"Ambiguous mining proxy: {name!r}, {reference_product!r}, {source!r}."
                )
            geo_map_filtered[location] = by_location[source][0]
        if not geo_map_filtered:
            return {}
        return self.fetch_proxies(
            datasets=candidates,
            regions=list(geo_map_filtered),
            geo_mapping=geo_map_filtered,
            unlist=False,
        )

    def add_transport_to_market(self, dataset, metal) -> list:
        origin_shares = defaultdict(float)
        for exc in dataset["exchanges"]:
            if exc["type"] == "technosphere" and exc.get("unit") == "kilogram":
                origin_shares[exc["location"]] += exc["amount"]

        exc_map = defaultdict(float)

        for c, share in origin_shares.items():
            if share <= 0:
                continue
            trspt_data = self.get_weighted_average_distance(c, metal)
            if trspt_data.empty:
                self._record_market_diagnostic(
                    "transport_missing", metal=metal, country=c, share=share
                )
            if metal in self.alt_names:
                for _, row in trspt_data.iterrows():
                    provider = self.metals_transport_activities[
                        row["TransportMode Label"].lower()
                    ]
                    if provider.get("unit") != "ton kilometer":
                        raise MetalsConfigError(
                            f"Freight provider must use ton kilometer: {provider.get('name')!r}."
                        )
                    key = (
                        self.metals_transport_activities[
                            row["TransportMode Label"].lower()
                        ]["name"],
                        self.metals_transport_activities[
                            row["TransportMode Label"].lower()
                        ]["reference product"],
                        "GLO",  # fixed location
                        self.metals_transport_activities[
                            row["TransportMode Label"].lower()
                        ]["unit"],
                    )
                    exc_map[key] += (row["Weighted Distance (km)"] / 1000) * share

        excs = [
            {
                "name": k[0],
                "product": k[1],
                "location": k[2],
                "unit": k[3],
                "type": "technosphere",
                "amount": v,
            }
            for k, v in exc_map.items()
        ]

        return excs

    def get_weighted_average_distance(self, country, metal):
        alt_metal = self.alt_names.get(metal)
        if not alt_metal:
            return pd.DataFrame()  # fallback

        row_positions = self.transport_lookup.get((country, alt_metal))
        if row_positions is None:
            return pd.DataFrame()
        return self.metals_transport.iloc[row_positions]

    def _record_market_diagnostic(self, reason, **values):
        if not hasattr(self, "market_diagnostics"):
            self.market_diagnostics = []
        self.market_diagnostics.append({"reason": reason, **values})

    def methodology_summary(self):
        """Expose assumptions separately from successful numerical validation."""
        return {
            "system model": self.system_model,
            "requested year": self.year,
            "unsupported years": "preserve source; no extrapolation or endpoint hold",
            "consequential supply": "preserve source marginal markets and substitution resource flows",
            "attributional resource correction": "declared attribution convention; not a physical recovery model",
            "recycling": "source-market shares; no dynamic scrap-stock model",
            "mining constraints": "not modelled; no ore-grade, capacity or depletion projection",
            "market decisions": dict(
                Counter(d["reason"] for d in self.market_diagnostics)
            ),
            "material decisions": dict(
                Counter(d["reason code"] for d in self.material_decisions)
            ),
        }

    def create_metal_markets(self):
        dataframe = load_mining_shares_mapping(self.version)
        dataframe = dataframe.loc[dataframe["Work done"] == "Yes"]
        dataframe = dataframe.loc[~dataframe["Country"].isnull()]

        dataframe_shares = dataframe

        self.country_codes.update(
            dict(
                zip(
                    dataframe["Country"].unique(),
                    coco.convert(dataframe["Country"].unique(), to="ISO2"),
                )
            )
        )

        # fix France (French Guiana) to GF
        self.country_codes["France (French Guiana)"] = "GF"

        grouped_metal_dfs = dict(tuple(dataframe_shares.groupby("Metal")))

        for metal, df_metal in grouped_metal_dfs.items():
            dataset = self.create_market(metal, df_metal)
            if dataset:
                self.database.append(dataset)
                self.add_to_index(dataset)
                self.write_log(dataset, "created")
                self.substitute_old_markets(new_dataset=dataset, df_metal=df_metal)

        # Regional mining proxies were added after the indexes were built in
        # ``__init__``. Refresh once so the optimized exact-match path sees the
        # same complete database that the legacy Wurst scan saw here.
        self.build_db_indexes()
        self.post_allocation_correction()

    def get_market_split_shares(self, metal_key: str) -> tuple[str, str, float, float]:
        """
        For a given metal_key (e.g., 'copper, cathod'), return:
        - market name
        - reference product
        - primary share (interpolated)
        - secondary share (interpolated)

        For consequential system model, always return 100% primary.
        """
        if self.system_model == "consequential":
            default_name = f"market for {metal_key}"
            default_reference_product = metal_key
            return default_name, default_reference_product, 1.0, 0.0

        entry = self.prim_sec_split.get(metal_key, None)
        if not entry:
            default_name = f"market for {metal_key}"
            default_reference_product = metal_key
            logger.warning(
                f"[Metals] WARNING: No entry found for metal key: {metal_key} in 'primary_secondary_split.yaml'"
            )
            return default_name, default_reference_product, 1.0, 0.0

        name = entry["name"]
        reference_product = entry["reference product"]

        p = interpolate_by_year(self.year, entry["shares"]["primary"])
        s = interpolate_by_year(self.year, entry["shares"]["secondary"])
        total = p + s

        if total > 1:
            logger.warning(
                f"[Metals] WARNING: Total shares for {metal_key} exceed 1: {total}. Normalizing."
            )

        return (
            name,
            reference_product,
            p / total if total > 0 else 0,  # Avoid division by zero
            s / total if total > 0 else 0,  # Avoid division by zero
        )

    def write_log(self, dataset, status="created"):
        """Record a structured metals provenance event."""

        self._validation_targets[id(dataset)] = dataset
        record_change_event(self, dataset, status, sector="metals")
