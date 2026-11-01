"""System-model behavior shared by import and transformation code."""

import math

ATTRIBUTIONAL_SYSTEM_MODELS = ("cutoff", "EN15804")

# Native EN15804 identities and reference-output signs. New aluminium uses
# RoW as an explicit geographic proxy; its positive-output market supplies a
# credit for the existing negative handoff. Post-consumer aluminium and iron
# instead request their negative-output treatment markets. Copper retains an
# unburdened handoff through the native burden-free provider.
EN15804_SCRAP_SUPPLIERS = {
    "aluminium scrap, new": ("market for aluminium scrap, new", "RoW", 1),
    "aluminium scrap, post-consumer": (
        "market for aluminium scrap, post-consumer",
        "GLO",
        -1,
    ),
    "copper scrap, sorted, pressed": ("burden-free production", "GLO", 1),
    "iron scrap, unsorted": ("market for iron scrap, unsorted", "GLO", -1),
}


def reference_output_amount(dataset):
    """Read one explicit reference output, avoiding assumptions from names."""
    output = [e for e in dataset.get("exchanges", ()) if e.get("type") == "production"]
    if (
        len(output) != 1
        or not math.isfinite(output[0]["amount"])
        or output[0]["amount"] == 0
    ):
        raise ValueError(
            f"Expected one finite nonzero reference output: {dataset['name']}"
        )
    return output[0]["amount"]


def adapt_en15804_scrap_exchanges(data, database):
    """Validate every native target before changing any supplemental handoff."""
    selected = []
    for dataset in data:
        for exchange in dataset.get("exchanges", ()):
            product = exchange.get("reference product", exchange.get("product"))
            if (
                exchange.get("type") != "technosphere"
                or product not in EN15804_SCRAP_SUPPLIERS
            ):
                continue
            if (
                exchange.get("name", "").casefold()
                != f"{product}, Recycled Content cut-off".casefold()
            ):
                continue
            if (
                exchange.get("unit") != "kilogram"
                or exchange.get("location") != "GLO"
                or not math.isfinite(exchange["amount"])
                or exchange["amount"] > 0
                or ("product" in exchange and exchange["product"] != product)
            ):
                raise ValueError(
                    f"Invalid EN15804 scrap handoff in {dataset['name']}: {product}"
                )
            selected.append((exchange, product))
    if not selected:
        return 0

    required = {product for _, product in selected}
    matches = {product: [] for product in required}
    for supplier in database:
        product = supplier.get("reference product")
        if product not in required:
            continue
        name, location, _ = EN15804_SCRAP_SUPPLIERS[product]
        if (supplier.get("name"), supplier.get("location"), supplier.get("unit")) == (
            name,
            location,
            "kilogram",
        ):
            matches[product].append(supplier)
    for product, suppliers in matches.items():
        name, location, expected_output = EN15804_SCRAP_SUPPLIERS[product]
        if len(suppliers) != 1:
            raise ValueError(
                f"EN15804 scrap requires one native supplier: {name} | {product} | {location}"
            )
        supplier = suppliers[0]
        if reference_output_amount(supplier) != expected_output:
            raise ValueError(
                f"Unexpected EN15804 reference-output sign: {name} | {product}"
            )
        if product == "copper scrap, sorted, pressed" and any(
            e.get("type") != "production" for e in supplier.get("exchanges", ())
        ):
            raise ValueError("EN15804 copper scrap provider must be burden-free")

    for exchange, product in selected:
        name, location, output = EN15804_SCRAP_SUPPLIERS[product]
        exchange.update(name=name, product=product, location=location)
        if "reference product" in exchange:
            exchange["reference product"] = product
        # Explicit supplier links, if present, refer to the old cut-off outlet.
        exchange.pop("input", None)
        exchange["comment"] = (
            f"EN15804 native scrap adaptation: signed kg quantity and uncertainty "
            f"retained; native reference output {output} kg. "
            + (exchange.get("comment") or "")
        )
    return len(selected)
