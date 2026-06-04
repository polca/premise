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
from collections import defaultdict

import country_converter as coco
import numpy as np
import pandas as pd
import yaml

from .export import biosphere_flows_dictionary
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    Dict,
    IAMDataCollection,
    InventorySet,
    List,
    Set,
    ws,
)
from .utils import DATA_DIR
from .validation import MetalsValidation

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
        0.24943856834512806,
        "recoverable Sb content represented by the antimony production chain",
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
        database=scenario["database"],
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
    scenario["database"] = metals.database
    scenario["cache"] = metals.cache
    scenario["index"] = metals.index

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

    validate.prim_sec_split = metals.prim_sec_split
    validate.interpolate_by_year = interpolate_by_year
    validate.metals_list = load_mining_shares_mapping()["Metal"].unique().tolist()
    validate.run_metals_checks()

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
    # remove rows with value 0 under Weighted Average Distance
    df = df.loc[df["Weighted Distance (km)"] != 0]

    df["country"] = df["Country"]

    return df


def load_mining_shares_mapping(ei_version="312"):
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

    # remove suppliers whose markets share is below the cutoff
    cut_off = 0.01

    df_filtered = df.loc[df.loc[:, "2020":"2030"].max(axis=1) > cut_off].copy()

    # Normalize remaining data back to 100% for each metal
    years = [str(year) for year in range(2020, 2031)]
    for metal in df_filtered["Metal"].unique():
        metal_indices = df_filtered["Metal"] == metal
        df_filtered.loc[:, years] = df_filtered.groupby("Metal")[years].transform(
            lambda x: x / x.sum()
        )

    return df


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

    filepath = DATA_DIR / "metals" / "metal_products.xlsx"
    df = pd.read_excel(filepath, sheet_name="activities_mapping")
    df = df.loc[(df["filter"] == "Yes") | (df["filter"] == "yes")]

    return df


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

    filepath = DATA_DIR / "metals" / "conversion_factors.xlsx"
    df = pd.read_excel(filepath, sheet_name="Conversion factors")
    return df


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
        if e.get("product", "").lower() != new_provider["reference product"].lower()
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


def normalize_resource_label(value: str) -> str:
    """Normalize resource labels enough for conservative fuzzy matching."""
    value = value or ""
    value = value.lower().replace(", in ground", "")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def canonical_resource_flow_label(value: str) -> str:
    """Return a canonical metal label for version-specific resource flows."""
    label = normalize_resource_label(value)
    return RESOURCE_FLOW_ALIASES.get(label, label)


def resource_label_tokens(value: str) -> Set[str]:
    return {
        token
        for token in normalize_resource_label(value).split()
        if len(token) > 1
        and not token.isdigit()
        and token not in RESOURCE_MATCH_STOPWORDS
    }


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


def can_add_missing_target_resource_exchange(dataset: dict) -> bool:
    """Return True when a missing target flow can be inferred safely."""
    if is_market_dataset(dataset):
        return False

    flow_name = get_reference_product_resource_flow_name(
        dataset.get("reference product", "")
    )
    if not flow_name:
        return False

    return is_pure_target_resource_product(
        dataset.get("reference product", ""), flow_name
    )


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
    return [
        exc
        for exc in dataset.get("exchanges", [])
        if exc.get("type") == "biosphere"
        and tuple(exc.get("categories", ())) == NATURAL_RESOURCE_IN_GROUND
        and exc.get("unit") == "kilogram"
    ]


def get_resource_label_variants(reference_product: str) -> Set[str]:
    """Return product-derived labels that may identify the target resource flow."""
    variants = {normalize_resource_label(reference_product)}

    for alias, target in RESOURCE_PRODUCT_ALIASES.items():
        if alias in normalize_resource_label(reference_product):
            variants.add(normalize_resource_label(target))

    tokens = resource_label_tokens(reference_product)
    variants.update(tokens)

    return {variant for variant in variants if variant}


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


def is_metal_resource_flow(flow_name: str) -> bool:
    return canonical_resource_flow_label(flow_name) in get_metal_resource_flow_labels()


def get_matching_resource_exchanges(dataset: dict) -> List[dict]:
    reference_product = dataset.get("reference product", "")
    return [
        exc
        for exc in get_in_ground_resource_exchanges(dataset)
        if resource_flow_matches_reference_product(
            exc.get("name", ""), reference_product
        )
    ]


def get_content_factor_resource_exchanges(dataset: dict) -> List[dict]:
    """Return resource exchanges with an explicit product-content factor."""
    reference_product = dataset.get("reference product", "")
    matches = []

    for exc in get_in_ground_resource_exchanges(dataset):
        if not is_metal_resource_flow(exc.get("name", "")):
            continue

        amount, _ = get_metal_bearing_product_content_factor(
            reference_product, exc.get("name", "")
        )
        if amount is not None:
            matches.append(exc)

    return matches


def get_target_resource_exchanges(dataset: dict) -> List[dict]:
    content_factor_matches = get_content_factor_resource_exchanges(dataset)
    if len(content_factor_matches) == 1:
        return content_factor_matches

    return [
        exc
        for exc in get_matching_resource_exchanges(dataset)
        if is_metal_resource_flow(exc.get("name", ""))
    ]


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
    """Return True if a technosphere exchange can be on the target-resource path."""
    labels = [
        exchange.get("product", ""),
        exchange.get("name", ""),
    ]
    return any(
        product_label_may_carry_target_resource(label, flow_name) for label in labels
    )


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
) -> bool:
    """
    Correct target in-ground resource content and zero co-mined resources.

    Returns True when a dataset was processed by the correction. Non-market
    datasets without a resolvable target flow are skipped unless strict=True.
    """
    resource_exchanges = get_in_ground_resource_exchanges(dataset)

    if is_market_dataset(dataset):
        return False

    if not resource_exchanges:
        if add_missing_target_resource and can_add_missing_target_resource_exchange(
            dataset
        ):
            flow_name = get_reference_product_resource_flow_name(
                dataset.get("reference product", "")
            )
            exchange_flow_name = target_resource_flow_name or flow_name
            target_exchange = make_target_resource_exchange(exchange_flow_name, 1.0)
            dataset.setdefault("exchanges", []).append(target_exchange)

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
            exc["amount"] = 0.0
        return True

    matching_resource_exchanges = get_matching_resource_exchanges(dataset)
    matches = get_target_resource_exchanges(dataset)

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
        is_target = exc is target_exchange
        new_amount = target_amount if is_target else 0.0
        exc["amount"] = new_amount

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

        self.country_codes = {}
        self.version = version

        self.metals = iam_data.metals_intensity_factors  # 1
        # Precompute the median values for each metal and origin_var for the year 2020
        available_years = sorted(self.metals.coords["year"].values)
        clamped_year = max(available_years[0], min(self.year, available_years[-1]))

        if clamped_year in available_years:
            self.precomputed_medians = self.metals.sel(year=clamped_year)
        else:
            self.precomputed_medians = self.metals.interp(
                year=clamped_year, method="linear"
            )

        self.activities_mapping = load_activities_mapping()  # 4

        self.conversion_factors = load_conversion_factors()  # 3
        # Precompute conversion factors as a dictionary for faster lookups
        self.conversion_factors_dict = self.conversion_factors.set_index("Activity")[
            "Conversion_factor"
        ].to_dict()

        inv = InventorySet(self.database, self.version)

        self.activities_metals_map: Dict[str, Set] = (  # 2
            inv.generate_metals_activities_map()
        )

        self.rev_activities_metals_map: Dict[str, str] = rev_metals_map(
            self.activities_metals_map
        )

        self.extended_dataframe = extend_dataframe(
            self.activities_mapping, self.activities_metals_map
        )
        self.extended_dataframe["final_technology"] = self.extended_dataframe.apply(
            lambda row: (
                row["demanding_process"]
                if pd.notna(row["demanding_process"]) and row["demanding_process"] != ""
                else row["ecoinvent_technology"]
            ),
            axis=1,
        )

        self.metals_transport = load_metals_transport()
        self.alt_names = load_metals_alternative_names()

        self.metals_transport_activities = {
            "air": ws.get_one(
                self.database,
                ws.contains("name", "aircraft"),
                ws.contains("name", "freight"),
                ws.contains("name", "belly"),
                ws.contains("name", "long haul"),
                ws.contains("name", "market"),
                ws.equals("location", "GLO"),
            ),
            "sea": ws.get_one(
                self.database,
                ws.contains("name", "transport"),
                ws.contains("name", "freight"),
                ws.contains("name", "sea"),
                ws.contains("name", "container ship"),
                ws.contains("name", "market"),
                ws.exclude(ws.contains("name", "reefer")),
                ws.equals("location", "GLO"),
            ),
            "railway": ws.get_one(
                self.database,
                ws.contains("name", "transport"),
                ws.contains("name", "freight"),
                ws.contains("name", "train"),
                ws.contains("name", "market"),
                ws.exclude(ws.contains("name", "reefer")),
                ws.equals("location", "GLO"),
            ),
            "road": ws.get_one(
                self.database,
                ws.contains("name", "transport"),
                ws.contains("name", "freight"),
                ws.contains("name", "lorry"),
                ws.contains("name", "market"),
                ws.contains("name", "unspecified"),
                ws.exclude(ws.contains("name", "reefer")),
                ws.equals("location", "GLO"),
            ),
        }

        self.build_db_indexes()

        self.weighted_transport_distances = {
            (row["country"], row["Metal"]): row
            for _, row in self.metals_transport.iterrows()
        }

        self.transport_lookup = {
            (row["country"], row["Metal"]): row
            for _, row in self.metals_transport.iterrows()
        }

        self.prim_sec_split = load_primary_secondary_split()

    def update_metals_use_in_database(self):
        """
        Update the database with metals use factors.
        """

        for dataset in self.database:
            if dataset["name"] in self.rev_activities_metals_map:
                origin_var = self.rev_activities_metals_map[dataset["name"]]
                self.update_metal_use(dataset, origin_var)

    @lru_cache()
    def get_metal_market_dataset(self, metal_activity_name: str):
        if pd.notna(metal_activity_name) and isinstance(metal_activity_name, str):
            metal_markets = list(
                ws.get_many(
                    self.database,
                    ws.equals("name", metal_activity_name),
                    ws.either(
                        *[ws.equals("location", loc) for loc in ["World", "GLO", "RoW"]]
                    ),
                )
            )
            metal_markets = [ds for ds in metal_markets if self.is_in_index(ds)]
            if metal_markets:
                return metal_markets[0]
            else:
                raise ws.NoResults(
                    f"Could not find dataset for metal market {metal_activity_name}"
                )
        else:
            raise ValueError(f"Invalid metal activity name: {metal_activity_name}")

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

        # Pre-fetch relevant data to minimize DataFrame operations
        tech_rows = self.extended_dataframe.loc[
            self.extended_dataframe["ecoinvent_technology"] == dataset["name"]
        ]

        if tech_rows.empty:
            logger.warning(
                f"No matching rows for {dataset['name']}, {dataset['location']}."
            )
            return

        conversion_factor = self.conversion_factors_dict.get(
            tech_rows["ecoinvent_technology"].iloc[0], None
        )
        available_metals = (
            self.precomputed_medians.sel(origin_var=technology)
            .dropna(dim="metal", how="all")["metal"]
            .values
        )

        unique_final_technologies = tech_rows["final_technology"].unique()

        for final_technology in unique_final_technologies:

            demanding_process_rows = tech_rows[
                (tech_rows["final_technology"] == final_technology)
                & tech_rows["demanding_process"].notna()
            ]

            if not demanding_process_rows.empty:
                for index, row in demanding_process_rows.iterrows():
                    self.process_metal_update(
                        metal_row=row,
                        dataset=dataset,
                        conversion_factor=conversion_factor,
                        final_technology=final_technology,
                        technology=technology,
                    )
            else:
                tech_specific_rows = tech_rows[
                    tech_rows["final_technology"] == final_technology
                ]
                for metal in available_metals:
                    if metal in tech_specific_rows["Element"].values:
                        match = tech_specific_rows[
                            tech_specific_rows["Element"] == metal
                        ]
                        if not match.empty:
                            metal_row = match.iloc[0]
                            self.process_metal_update(
                                metal_row=metal_row,
                                dataset=dataset,
                                conversion_factor=conversion_factor,
                                final_technology=final_technology,
                                technology=technology,
                            )

    def process_metal_update(
        self, metal_row, dataset, final_technology, technology, conversion_factor
    ):
        """
        Process the update for a given metal and technology.
        """
        conversion_factor = conversion_factor or 1
        unit_converter = metal_row.get("unit_convertor")
        metal_activity_name = metal_row["Activity"]

        if pd.notna(unit_converter) and pd.notna(metal_activity_name):
            use_factors = self.precomputed_medians.sel(
                metal=metal_row["Element"], origin_var=technology
            )
            median_value = (
                use_factors.sel(variable="median").item()
                * unit_converter
                * conversion_factor
            )

            min_value = (
                use_factors.sel(variable="min").item()
                * unit_converter
                * conversion_factor
            )
            max_value = (
                use_factors.sel(variable="max").item()
                * unit_converter
                * conversion_factor
            )

            if median_value != 0 and not np.isnan(median_value):
                try:
                    dataset_metal = self.get_metal_market_dataset(metal_activity_name)
                except ws.NoResults:
                    return

                metal_users = self.db_index_by_name.get(final_technology, [])
                for metal_user in metal_users:
                    update_exchanges(
                        activity=metal_user,
                        new_amount=median_value,
                        new_provider=dataset_metal,
                        metal=metal_row["Element"],
                        min_value=min_value,
                        max_value=max_value,
                    )
                    self.write_log(metal_user, "updated")
        else:
            print(
                f"Warning: Missing data for {metal_row['Element']} for {dataset['name']}:"
            )
            if pd.isna(unit_converter):
                print("- unit converter")
            if pd.isna(metal_activity_name):
                print("- activity name")

    def post_allocation_correction(self):
        """
        Correct metal resource flows distorted by economic allocation.

        The correction scans non-market datasets from the mining-share mapping
        and additional extraction-like datasets. When an in-ground kilogram
        natural resource flow can be unambiguously matched to the dataset
        reference product, that target flow is set to the target-metal content
        of one kilogram of the reference product. Pure target-metal products
        are set to 1 kg; metal-bearing intermediates use explicit content
        factors. Other in-ground kilogram natural resource flows are set to
        zero.
        """

        considered_dataset_ids = self.get_considered_metal_dataset_ids()
        strict_dataset_ids = self.get_mapped_metal_dataset_ids()
        missing_target_dataset_ids = self.get_missing_target_resource_dataset_ids(
            considered_dataset_ids
        )
        resource_flow_names = (
            self.get_existing_in_ground_resource_flow_names()
            | self.get_biosphere_in_ground_resource_flow_names()
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

            correct_metal_resource_exchanges(
                ds,
                strict=id(ds) in strict_dataset_ids,
                add_missing_target_resource=id(ds) in missing_target_dataset_ids,
                target_resource_flow_name=target_resource_flow_name,
            )

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

            resource_exchanges = get_in_ground_resource_exchanges(dataset)
            if resource_exchanges and (
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
            for exc in get_in_ground_resource_exchanges(dataset)
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
    def dataset_has_target_resource_exchange(dataset: dict, flow_name: str) -> bool:
        """Return True if a dataset directly extracts the target resource."""
        flow_label = canonical_resource_flow_label(flow_name)
        return any(
            canonical_resource_flow_label(exc.get("name", "")) == flow_label
            and abs(float(exc.get("amount", 0.0))) > 0
            for exc in get_in_ground_resource_exchanges(dataset)
        )

    def target_resource_is_supplied_upstream(
        self,
        dataset: dict,
        flow_name: str,
        provider_index: Dict[tuple, List[dict]],
        cache: dict,
        max_depth: int = 12,
        visited: Optional[Set[int]] = None,
    ) -> bool:
        """Return True if direct technosphere suppliers already carry the target."""
        if max_depth < 0:
            return False

        cache_key = (id(dataset), normalize_resource_label(flow_name), max_depth)
        if cache_key in cache:
            return cache[cache_key]

        visited = visited or set()
        visited.add(id(dataset))

        for exc in dataset.get("exchanges", []):
            if exc.get("type") != "technosphere" or exc.get("amount", 0) <= 0:
                continue
            if not exchange_may_carry_target_resource(exc, flow_name):
                continue

            providers = provider_index.get(
                (exc.get("name"), exc.get("product"), exc.get("location")),
                [],
            )
            for provider in providers:
                if id(provider) in visited:
                    continue
                if is_market_dataset(provider) and (
                    normalize_resource_label(provider.get("reference product", ""))
                    == normalize_resource_label(flow_name)
                ):
                    continue
                if is_downstream_attributed_resource_carrier_dataset(provider):
                    continue
                if dataset_may_carry_target_resource(
                    provider, flow_name
                ) and self.dataset_has_target_resource_exchange(provider, flow_name):
                    cache[cache_key] = True
                    return True
                if self.target_resource_is_supplied_upstream(
                    provider,
                    flow_name,
                    provider_index,
                    cache,
                    max_depth=max_depth - 1,
                    visited=visited.copy(),
                ):
                    cache[cache_key] = True
                    return True

        cache[cache_key] = False
        return False

    def get_missing_target_resource_dataset_ids(
        self, strict_dataset_ids: Set[int]
    ) -> Set[int]:
        """
        Return mapped pure-metal datasets that need a direct target resource flow.

        A missing direct flow is only added when the supply chain does not
        already provide the target resource through a target-bearing product.
        This avoids double-counting normal production chains whose upstream ore
        or concentrate dataset is corrected separately, while still adding the
        target flow for byproduct and recovered-metal routes whose upstream
        co-mined flows are zeroed or absent.
        """
        provider_index = self.build_exchange_provider_index()
        cache = {}
        dataset_ids = set()

        for dataset in self.database:
            if id(dataset) not in strict_dataset_ids:
                continue
            if get_in_ground_resource_exchanges(dataset):
                continue
            if not can_add_missing_target_resource_exchange(dataset):
                continue

            flow_name = get_reference_product_resource_flow_name(
                dataset.get("reference product", "")
            )
            if not flow_name:
                continue
            if self.target_resource_is_supplied_upstream(
                dataset, flow_name, provider_index, cache
            ):
                continue

            dataset_ids.add(id(dataset))

        return dataset_ids

    def get_mining_share_dataset_ids(self) -> Set[int]:
        """Return dataset object IDs matched by the mining-share mapping."""
        dataframe = load_mining_shares_mapping(self.version)
        dataframe = dataframe.loc[dataframe["Work done"] == "Yes"]

        dataset_ids = set()
        grouped = dataframe[["Process", "Reference product"]].drop_duplicates()

        for _, row in grouped.iterrows():
            try:
                proc_filter = ast.literal_eval(row["Process"])
                ref_prod_filter = ast.literal_eval(row["Reference product"])
                filters = build_ws_filter("name", proc_filter) + build_ws_filter(
                    "reference product", ref_prod_filter
                )
            except (ValueError, SyntaxError) as exc:
                logger.warning(
                    f"[Metals] Invalid mining-share mapping filter skipped: {exc}"
                )
                continue

            for dataset in ws.get_many(self.database, *filters):
                if is_market_dataset(dataset):
                    continue
                if get_in_ground_resource_exchanges(
                    dataset
                ) or can_add_missing_target_resource_exchange(dataset):
                    dataset_ids.add(id(dataset))

        return dataset_ids

    def get_mapped_metal_dataset_ids(self) -> Set[int]:
        """Return mapped dataset IDs with one unambiguous target resource flow."""
        dataset_ids = set()
        mining_share_dataset_ids = self.get_mining_share_dataset_ids()

        for dataset in self.database:
            if id(dataset) not in mining_share_dataset_ids:
                continue
            matches = get_target_resource_exchanges(dataset)
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

        # we fetch the shares for each location in df
        # and we interpolate if necessary between the columns
        # 2020 to 2030

        for long_location, short_location in new_locations.items():
            share = df.loc[df["Country"] == long_location, "2020":"2030"]
            if len(share) > 0:

                # we interpolate depending on if self.year is between 2020 and 2030
                # otherwise, we back or forward fill

                if self.year < 2020:
                    share = share.iloc[:, 0]
                elif self.year > 2030:
                    share = share.iloc[:, -1]
                else:
                    share = share.iloc[:, self.year - 2020]

                share = share.values[0]
                shares[(name, ref_prod, short_location)] = share

        # filter-out shares that are below 1% and normalize the rest
        shares = {k: v for k, v in shares.items() if v >= 0.01}
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

    def create_region_specific_markets(self, df: pd.DataFrame) -> List[dict]:
        new_exchanges, new_datasets = [], []

        df["Process_str"] = df["Process"].apply(str)
        df["Reference_product_str"] = df["Reference product"].apply(str)

        for (_, _), group in df.groupby(["Process_str", "Reference_product_str"]):
            try:
                proc_filter = ast.literal_eval(group["Process"].iloc[0])
                ref_prod_filter = ast.literal_eval(group["Reference product"].iloc[0])
            except (ValueError, SyntaxError) as exc:
                logger.error(
                    f"[Metals] Invalid filter expression for process/reference product: {exc}"
                )
                continue

            try:
                filters = build_ws_filter("name", proc_filter) + build_ws_filter(
                    "reference product", ref_prod_filter
                )
                subset = list(ws.get_many(self.database, *filters))

            except Exception as e:
                logger.error(
                    f"[Metals] Error fetching datasets for process '{proc_filter}' and reference product '{ref_prod_filter}': {e}"
                )
                print(
                    f"failed with process '{proc_filter}' and reference product '{ref_prod_filter}"
                )
                continue

            if not subset:
                logger.warning(
                    f"[Metals] No datasets found for filter combination:\n"
                    f"  name: {proc_filter}\n"
                    f"  reference product: {ref_prod_filter}"
                )
                continue

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
        # check if market already exists
        # if so, remove it

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
            secondary_exchanges = []

        # add mining exchanges
        # dataset["exchanges"].extend(self.create_region_specific_markets(df))
        for exc in primary_exchanges:
            exc["amount"] *= p_share
        dataset["exchanges"].extend(primary_exchanges)

        if secondary_exchanges:
            dataset["exchanges"].extend(secondary_exchanges)

        # add transport exchanges
        trspt_exc = self.add_transport_to_market(dataset, metal)
        for exc in trspt_exc:
            exc["amount"] *= p_share
        if len(trspt_exc) > 0:
            dataset["exchanges"].extend(trspt_exc)

        # # filter out None
        dataset["exchanges"] = [
            exc
            for exc in dataset["exchanges"]
            if self.activity_exists(exc) or exc["type"] == "production"
        ]

        # remove old market dataset
        for old_market in ws.get_many(
            self.database,
            ws.equals("name", dataset["name"]),
            ws.equals("reference product", dataset["reference product"]),
            ws.exclude(ws.equals("location", "World")),
        ):
            self.remove_from_index(old_market)
            assert (
                self.is_in_index(old_market) is False
            ), f"Market {(old_market['name'], old_market['reference product'], old_market['location'])} still in index"

        return dataset

    def activity_exists(self, exchange: dict) -> bool:
        """Check if an activity referenced by an exchange exists in the database."""
        if exchange.get("type") != "technosphere":
            return True

        activities = list(
            ws.get_many(
                self.database,
                ws.equals("name", exchange["name"]),
                ws.equals("reference product", exchange["product"]),
                ws.equals("location", exchange["location"]),
            )
        )

        return len(activities) > 0

    def substitute_old_markets(self, new_dataset: dict, df_metal: pd.DataFrame) -> None:
        """
        Substitute all old market links with the new 'World' market for the given metal key.
        Uses flexible matching based on Reference product field in Excel.
        """
        old_ref_products = set()

        for _, row in df_metal.iterrows():
            ref_field = row.get("Reference product")
            if isinstance(ref_field, dict):
                if "either" in ref_field:
                    old_ref_products.update(
                        q.get("equals") for q in ref_field["either"] if "equals" in q
                    )
                elif "equals" in ref_field:
                    old_ref_products.add(ref_field["equals"])
            elif isinstance(ref_field, str):
                old_ref_products.add(ref_field)

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

        geo_map_filtered = {
            k: self.db_index_full[name][reference_product][v][0]
            for k, v in geography_mapping.items()
            if self.db_index_full[name][reference_product].get(v)
            and not self.is_in_index(
                {"name": name, "reference product": reference_product, "location": k}
            )
        }

        if (
            not geo_map_filtered
            and name in self.db_index_full
            and reference_product in self.db_index_full[name]
        ):
            # Get any available activity to use as proxy
            available_locations = list(
                self.db_index_full[name][reference_product].keys()
            )
            if available_locations:
                proxy_activity = self.db_index_full[name][reference_product][
                    available_locations[0]
                ][0]

                logger.warning(
                    f"Falling back to proxy activity for {name}, {reference_product}. "
                    f"Using location '{available_locations[0]}' for regions: "
                    f"{[k for k in new_locations.values() if not self.is_in_index({'name': name, 'reference product': reference_product, 'location': k})]}"
                )

                # Build geo_map_filtered with this proxy for all needed locations
                geo_map_filtered = {
                    k: proxy_activity
                    for k in new_locations.values()
                    if not self.is_in_index(
                        {
                            "name": name,
                            "reference product": reference_product,
                            "location": k,
                        }
                    )
                }

        if not geo_map_filtered:
            logger.error(
                f"Failed to create activities for {name} in locations: "
                f"{list(new_locations.values())}"
            )
            return {}

        datasets = self.db_index.get(name, {}).get(reference_product, [])

        return self.fetch_proxies(
            datasets=datasets,
            regions=new_locations.values(),
            geo_mapping=geo_map_filtered,
        )

    def add_transport_to_market(self, dataset, metal) -> list:

        origin_shares = {
            e["location"]: e["amount"]
            for e in dataset["exchanges"]
            if e["type"] == "technosphere"
        }

        exc_map = defaultdict(float)

        for c, share in origin_shares.items():
            if metal in self.alt_names:
                trspt_data = self.get_weighted_average_distance(c, metal)
                for _, row in trspt_data.iterrows():
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

        rows = [
            row
            for (c, m), row in self.transport_lookup.items()
            if c == country and m == alt_metal
        ]

        return pd.DataFrame(rows)

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
        """
        Write log file.
        """

        txt = (
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['reference product']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('post-allocation correction', '')}|"
            f"{dataset.get('log parameters', {}).get('old amount', '')}|"
            f"{dataset.get('log parameters', {}).get('new amount', '')}"
        )

        logger.info(txt)
