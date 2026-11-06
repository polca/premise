"""Select vehicle emissions whose model equations are proportional to fuel.

The refreshed carculator inventories calculate CO2 from burned fuel and SO2
from its sulphur content. Generic-air methane represents pump-to-tank losses;
HBEFA combustion methane uses the urban/rural/highway air subcompartments.
HBEFA hot, cold-start and evaporative factors, refrigerants and abrasion are
not proportional to an IAM energy-intensity index.
"""


def fuel_proportional_emission(exchange):
    """Select fuel-derived air flows, preserving compartmented HBEFA methane.

    Importers represent generic air either as ``("air",)`` or
    ``("air", "unspecified")``. Unknown flows/compartments are not assigned
    a fuel relationship. A later vehicle-sizing operation can independently
    update mass-dependent burdens; this filter concerns energy scaling only.
    """
    if exchange.get("type") != "biosphere":
        return False
    categories = exchange.get("categories") or ()
    if isinstance(categories, str):
        categories = tuple(categories.split("::"))
    if not categories or categories[0] != "air":
        return False
    name = exchange.get("name")
    if name in ("Carbon dioxide, fossil", "Carbon dioxide, non-fossil"):
        return True
    return tuple(categories) in (("air",), ("air", "unspecified")) and name in (
        "Sulfur dioxide",
        "Methane, fossil",
        "Methane, non-fossil",
    )
