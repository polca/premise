"""Identify purchased vehicle energy without selecting fuel-labelled equipment."""


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
