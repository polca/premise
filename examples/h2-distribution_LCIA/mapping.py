"""Pure name normalization, display-name, and classification lookups."""

try:
    from config import CED_METHODS, PREMISE_GWP_METHOD
except ImportError:  # pragma: no cover - supports package-style imports
    from .config import CED_METHODS, PREMISE_GWP_METHOD


HYDROGEN_PRODUCT = "hydrogen, gaseous, low pressure"

TRANSPORT_NAMES = {
    "transport, hydrogen, gaseous, lorry, unspecified": "Gaseous H2 truck",
    "transport, hydrogen, liquid, lorry, unspecified": "Liquid H2 truck",
    "hydrogen supply, distributed by pipeline": "Pipeline distribution",
    "transport, freight, sea, tanker for liquefied ammonia, ammonia and mdo": "Ammonia tanker",
    "transport, freight, sea, tanker for liquefied hydrogen, heavy fuel oil": "Liquid H2 tanker",
}
CONVERSION_NAMES = {
    "gaseous hydrogen production": "Compression (lorry)",
    "liquid hydrogen production": "Liquefaction",
    "liquid ammonia production": "Ammonia production",
    "market group for electricity, low voltage": "Compression (pipeline)",
    "compressor assembly for transmission hydrogen pipeline": "Compression (pipeline)",
}
RECONVERSION_NAMES = {
    "ammonia cracking": "Ammonia cracking",
    "liquid hydrogen regasification": "Hydrogen regasification",
}


def normalized(value):
    """Return a whitespace-normalized, lowercase representation."""
    return " ".join(str(value or "").replace(" ,", ",").split()).lower()


def is_hydrogen_market(activity):
    return (
        normalized(activity.get("name")).startswith(
            "market for hydrogen, gaseous, low pressure"
        )
        and normalized(activity.get("reference product")) == HYDROGEN_PRODUCT
    )


def short_production_name(name):
    text = normalized(name)
    if "pem electrolysis" in text:
        return "PEM electrolysis"
    if "alkaline electrolysis" in text:
        return "Alkaline electrolysis"
    if "woody biomass" in text and "with ccs" in text:
        return "Biomass gasification with CCS"
    if "woody biomass" in text:
        return "Biomass gasification"
    if "coal gasification" in text and "with ccs" in text:
        return "Coal gasification with CCS"
    if "steam methane reforming" in text and "with ccs" in text:
        return "Steam methane reforming with CCS"
    if "steam methane reforming" in text:
        return "Steam methane reforming"
    return name


def classify_market_branch(provider):
    """Return stage, substage, display name, and matched classification rule."""
    name = normalized(provider.get("name"))
    if name in TRANSPORT_NAMES:
        return "Distribution", "Transport", TRANSPORT_NAMES[name], "exact transport activity"
    if name in CONVERSION_NAMES:
        return "Distribution", "Conversion", CONVERSION_NAMES[name], "exact conversion activity"
    if name in RECONVERSION_NAMES or "ammonia cracking" in name or "regasification" in name:
        return (
            "Distribution",
            "Reconversion",
            RECONVERSION_NAMES.get(name, provider.get("name")),
            "reconversion activity",
        )
    if name.startswith("hydrogen production"):
        return (
            "Production",
            "Production technology",
            short_production_name(provider.get("name")),
            "hydrogen production activity",
        )
    raise ValueError(
        "Unclassified direct hydrogen-market input: "
        f"{provider.get('name')} | {provider.get('reference product')} | "
        f"{provider.get('unit')} | {provider.key}"
    )


def classify_production_input(provider):
    name = normalized(provider.get("name"))
    product = normalized(provider.get("reference product"))
    unit = normalized(provider.get("unit"))
    text = f"{name} | {product}"
    if "electricity" in text:
        return "Electricity"
    if "heat" in text or "steam" in text:
        return "Heat"
    if "water" in text:
        return "Water"
    if "carbon dioxide, captured" in text or "carbon capture" in text:
        return "CO2 capture and storage"
    if any(term in text for term in ("wood", "biomass", "biomethane")):
        return "Biomass feedstock"
    if any(
        term in text
        for term in ("natural gas", "hard coal", "lignite", "petroleum", "coke")
    ):
        return "Fossil feedstock"
    if name.startswith("transport") or "transport," in product:
        return "Transport services"
    if name.startswith("treatment") or "waste" in product:
        return "Waste treatment"
    if unit in {"unit", "kilometer"} or any(
        term in text
        for term in ("construction", "factory", "plant", "electrolyzer", "pipeline")
    ):
        return "Infrastructure"
    return "Other raw materials"


def impact_category_label(method):
    if method == PREMISE_GWP_METHOD:
        return "climate change — GWP 100a, incl. H (premise_gwp)"
    if method == CED_METHODS[0]:
        return "cumulative energy demand: non-renewable resources"
    if method == CED_METHODS[1]:
        return "cumulative energy demand: renewable sources"
    return method[1]
