"""Pure helpers shared by hydrogen transformation and validation.

The functions in this module only inspect dictionaries and routing rules.  They
deliberately do not depend on a transformation or validator instance so the
same activity cannot be classified differently during construction and final
database validation.
"""

from functools import lru_cache

import yaml

from .config import HYDROGEN_CONSUMER_ROUTING

HYDROGEN_MARKET = "market for hydrogen, gaseous, low pressure"
HYDROGEN_PRODUCT = "hydrogen, gaseous, low pressure"

HYDROGEN_SUPPORT_ACTIVITY_IDENTITIES = {
    (
        "transport, hydrogen, gaseous, lorry, unspecified",
        "transport, hydrogen, gaseous, lorry, unspecified",
    ),
    (
        "transport, hydrogen, liquid, lorry, unspecified",
        "transport, hydrogen, liquid, lorry, unspecified",
    ),
    (
        "hydrogen supply, distributed by pipeline",
        "hydrogen, gaseous, from pipeline",
    ),
    (
        "transport, freight, sea, tanker for liquefied ammonia, ammonia and mdo",
        "transport, freight, sea, tanker for liquefied ammonia, ammonia and mdo",
    ),
    (
        "transport, freight, sea, tanker for liquefied hydrogen, heavy fuel oil",
        "transport, freight, sea, tanker for liquefied hydrogen, heavy fuel oil",
    ),
    ("gaseous hydrogen production", "gaseous hydrogen production"),
    ("liquid hydrogen production", "liquid hydrogen production"),
    ("liquid hydrogen regasification", "liquid hydrogen regasification"),
    ("liquid ammonia production", "liquid ammonia production"),
    ("ammonia cracking", "ammonia cracking"),
}


@lru_cache(maxsize=1)
def load_hydrogen_consumer_routing():
    """Return the packaged sector-routing configuration."""

    with open(HYDROGEN_CONSUMER_ROUTING, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def hydrogen_sector_markets(routing=None):
    """Return the configured mapping from sector key to market name."""

    routing = routing or load_hydrogen_consumer_routing()
    return {
        sector: rules["market"]
        for sector, rules in routing.get("sectors", {}).items()
        if rules.get("market")
    }


def hydrogen_market_sectors(routing=None):
    """Return the configured mapping from market name to sector key."""

    return {
        market: sector
        for sector, market in hydrogen_sector_markets(routing).items()
    }


def hydrogen_consumer_text(dataset):
    """Return normalized text used by name-based consumer routing."""

    return " | ".join(
        str(dataset.get(field, ""))
        for field in ("name", "reference product", "unit")
    ).lower()


def hydrogen_consumer_isic_codes(dataset):
    """Return normalized ISIC rev.4 codes attached to a dataset."""

    codes = []
    for classification in dataset.get("classifications", []):
        if not isinstance(classification, (list, tuple)) or len(classification) < 2:
            continue
        if classification[0] != "ISIC rev.4 ecoinvent":
            continue
        code = str(classification[1]).split(":", 1)[0].strip()
        if code:
            codes.append(code)
    return codes


def hydrogen_isic_matches_rule(code, rule):
    """Return whether an ISIC code satisfies one routing rule."""

    if code in {str(value) for value in rule.get("isic_exact", [])}:
        return True
    if any(code.startswith(str(prefix)) for prefix in rule.get("isic_prefix", [])):
        return True

    for prefix, excluded_codes in rule.get(
        "isic_prefix_excluding_exact", {}
    ).items():
        if code.startswith(str(prefix)) and code not in {
            str(value) for value in excluded_codes
        }:
            return True

    for prefix, excluded_prefixes in rule.get(
        "isic_prefix_excluding_prefix", {}
    ).items():
        if code.startswith(str(prefix)) and not any(
            code.startswith(str(excluded_prefix))
            for excluded_prefix in excluded_prefixes
        ):
            return True

    return False


def classify_hydrogen_consumer_sector(dataset, routing=None):
    """Return one unambiguous sector and all candidates for a consumer."""

    routing = routing or load_hydrogen_consumer_routing()
    text = hydrogen_consumer_text(dataset)
    sectors = routing.get("sectors", {})

    matches = [
        sector
        for sector, rules in sectors.items()
        if any(
            str(keyword).lower() in text
            for keyword in rules.get("name_contains", [])
        )
    ]
    if len(matches) == 1:
        return matches[0], matches
    if len(matches) > 1:
        return None, matches

    codes = hydrogen_consumer_isic_codes(dataset)
    matches = [
        sector
        for sector, rules in sectors.items()
        if any(hydrogen_isic_matches_rule(code, rules) for code in codes)
    ]
    if len(matches) == 1:
        return matches[0], matches
    return None, matches


def keep_general_hydrogen_market(dataset, routing=None):
    """Return whether routing deliberately retains the generic market."""

    routing = routing or load_hydrogen_consumer_routing()
    rules = routing.get("keep_general_market", {})
    text = hydrogen_consumer_text(dataset)
    if any(
        str(keyword).lower() in text for keyword in rules.get("name_contains", [])
    ):
        return True
    return any(
        hydrogen_isic_matches_rule(code, rules)
        for code in hydrogen_consumer_isic_codes(dataset)
    )


def is_hydrogen_support_dataset(dataset):
    """Return whether a dataset is shared hydrogen logistics support."""

    return (
        dataset.get("name"),
        dataset.get("reference product"),
    ) in HYDROGEN_SUPPORT_ACTIVITY_IDENTITIES


def is_hydrogen_supplier_dataset(dataset, routing=None):
    """Return whether a dataset supplies hydrogen rather than consuming it."""

    market_names = set(hydrogen_sector_markets(routing).values())
    return (
        dataset.get("reference product") == HYDROGEN_PRODUCT
        or dataset.get("name") == HYDROGEN_MARKET
        or dataset.get("name") in market_names
    )


def is_generic_hydrogen_market_exchange(exchange):
    """Return whether an exchange points to the generic low-pressure market."""

    return (
        exchange.get("type") == "technosphere"
        and exchange.get("name") == HYDROGEN_MARKET
        and exchange.get("product") == HYDROGEN_PRODUCT
    )


def hydrogen_market_exchange_sector(exchange, routing=None):
    """Return ``generic``, a sector key, or ``None`` for an exchange."""

    if exchange.get("type") != "technosphere":
        return None
    if exchange.get("product") != HYDROGEN_PRODUCT:
        return None
    if exchange.get("name") == HYDROGEN_MARKET:
        return "generic"
    return hydrogen_market_sectors(routing).get(exchange.get("name"))


def is_hydrogen_market_exchange(exchange, routing=None):
    """Return whether an exchange points to any generic or sector H2 market."""

    return hydrogen_market_exchange_sector(exchange, routing) is not None
