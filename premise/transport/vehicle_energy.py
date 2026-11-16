"""Identify purchased vehicle energy without selecting fuel-labelled equipment."""

from functools import lru_cache

from ..core.filesystem_constants import DATA_DIR
from ..core.runtime_cache import load_yaml_cached


@lru_cache(maxsize=1)
def _energy_aliases():
    definitions = load_yaml_cached(
        DATA_DIR / "transport" / "vehicle_energy_aliases.yaml"
    )
    return {(name, row["unit"]): row["product"] for name, row in definitions.items()}


def canonical_vehicle_energy_product(product, unit):
    """Interpret exact declared labels without changing the supplier or unit."""
    return _energy_aliases().get((product, unit), product)


def vehicle_energy_exchange(exchange):
    """Match an energy product and unit, including direct or market suppliers."""
    if exchange.get("type") != "technosphere":
        return False
    product = exchange.get("product", exchange.get("reference product"))
    if not product:
        product = (
            exchange.get("name", "")
            .removeprefix("market for ")
            .removeprefix("market group for ")
        )
    product = canonical_vehicle_energy_product(product, exchange.get("unit"))
    for prefix, units in (
        ("electricity", {"kilowatt hour"}),
        ("hydrogen", {"kilogram"}),
        ("natural gas", {"kilogram", "cubic meter"}),
        ("diesel", {"kilogram"}),
        ("petrol", {"kilogram"}),
        ("gasoline", {"kilogram"}),
        ("liquefied petroleum gas", {"kilogram"}),
        ("biogas", {"kilogram", "cubic meter"}),
        ("methane", {"kilogram", "cubic meter"}),
        ("biodiesel", {"kilogram"}),
        ("ethanol", {"kilogram"}),
        ("methanol", {"kilogram"}),
    ):
        if product == prefix or product.startswith(prefix + ","):
            return exchange.get("unit") in units
    return False
