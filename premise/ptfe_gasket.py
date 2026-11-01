"""Select the approved native-resin AEC gasket boundary during source import.

The caller restricts this operation to the additional electrolysis workbook.
Older targets retain its monomer-based legacy reference. Native background
activities and arbitrary user inventories are never passed through this hook.
"""

from copy import deepcopy
import math

from packaging.version import Version

LEGACY_NAME = "polytetrafluoroethylene gasket production, polymerization and forming"
LEGACY_PRODUCT = "PTFE gasket, polymerization and forming"
NATIVE_NAME = "polytetrafluoroethylene gasket production, forming, resin supply proxy"
NATIVE_PRODUCT = "PTFE gasket, unfilled, engineering reference"
STACK_NAME = "electrolyzer production, 1MWe, AEC, Stack"

LEGACY = (LEGACY_NAME, LEGACY_PRODUCT, "GLO", "kilogram")
NATIVE = (NATIVE_NAME, NATIVE_PRODUCT, "GLO", "kilogram")
LEGACY_RECIPE = {
    ("production", *LEGACY): 1,
    (
        "technosphere",
        "market for tetrafluoroethylene",
        "tetrafluoroethylene",
        "GLO",
        "kilogram",
    ): 1
    / 0.95,
    (
        "technosphere",
        "market for heat, district or industrial, natural gas",
        "heat, district or industrial, natural gas",
        "RoW",
        "megajoule",
    ): 10.5,
    (
        "technosphere",
        "market group for electricity, medium voltage",
        "electricity, medium voltage",
        "GLO",
        "kilowatt hour",
    ): 0.7,
    (
        "technosphere",
        "market for waste plastic, mixture",
        "waste plastic, mixture",
        "RoW",
        "kilogram",
    ): -(1 / 0.95 - 1),
}


def _identity(record):
    product = record.get("product", record.get("reference product"))
    if "product" in record and "reference product" in record:
        if record["product"] != record["reference product"]:
            raise ValueError("PTFE gasket supplier product fields disagree")
    return record.get("name"), product, record.get("location"), record.get("unit")


def _amount(exchange, expected):
    amount = exchange.get("amount")
    return (
        type(amount) in (int, float)
        and math.isfinite(amount)
        and math.isclose(amount, expected, rel_tol=1e-12)
        and not exchange.get("formula")
    )


def use_native_ptfe_gasket(importer, version):
    """Rewire one reviewed 144.2 kg purchase and omit its legacy producer.

    Called before migration/preparation of the additional electrolysis source.
    Validate the complete selected scope before replacing the importer's data;
    an inconsistent or expanded consumer graph needs another source review.
    Repeated application to the already selected source is a no-op.
    """
    if Version(str(version)) < Version("3.12"):
        return 0
    data = importer.data
    if any(d.get("name") == NATIVE_NAME for d in data):
        raise ValueError("Native PTFE gasket must come from its separate source")
    producers = [d for d in data if d.get("name") == LEGACY_NAME]
    stacks = [d for d in data if d.get("name") == STACK_NAME]
    if len(stacks) != 1:
        raise ValueError("PTFE gasket conversion requires one AEC stack")
    stack = stacks[0]
    if _identity(stack) != (
        STACK_NAME,
        "electrolyzer, 1MWe, AEC, Stack",
        "RER",
        "unit",
    ):
        raise ValueError("PTFE gasket AEC stack identity changed")
    purchases = []
    for dataset in data:
        for exchange in dataset["exchanges"]:
            if exchange.get("type") != "technosphere":
                continue
            if exchange.get("name") not in {LEGACY_NAME, NATIVE_NAME} and not any(
                exchange.get(field) in {LEGACY_PRODUCT, NATIVE_PRODUCT}
                for field in ("product", "reference product")
            ):
                continue
            key = _identity(exchange)
            if key[0] in {LEGACY_NAME, NATIVE_NAME} or key[1] in {
                LEGACY_PRODUCT,
                NATIVE_PRODUCT,
            }:
                purchases.append((dataset, exchange, key))
    if len(purchases) != 1:
        raise ValueError("PTFE gasket conversion requires one declared purchase")
    consumer, purchase, identity = purchases[0]
    if consumer is not stack or not _amount(purchase, 144.2):
        raise ValueError("PTFE gasket purchase must remain 144.2 kg in the AEC stack")
    if identity == NATIVE and not producers:
        return 0
    if identity != LEGACY or len(producers) != 1:
        raise ValueError("PTFE gasket legacy producer or purchase is inconsistent")
    producer = producers[0]
    if _identity(producer) != LEGACY:
        raise ValueError("PTFE gasket legacy producer identity changed")
    recipe = producer["exchanges"]
    if len(recipe) != len(LEGACY_RECIPE):
        raise ValueError("PTFE gasket legacy manufacturing boundary changed")
    observed = {(e.get("type"), *_identity(e)): e for e in recipe}
    if observed.keys() != LEGACY_RECIPE.keys() or not all(
        _amount(observed[key], expected) for key, expected in LEGACY_RECIPE.items()
    ):
        raise ValueError("PTFE gasket legacy manufacturing recipe changed")
    revised = deepcopy(stack)
    position = next(i for i, e in enumerate(stack["exchanges"]) if e is purchase)
    exchange = revised["exchanges"][position]
    exchange["name"] = NATIVE_NAME
    if "product" in exchange:
        exchange["product"] = NATIVE_PRODUCT
    if "reference product" in exchange:
        exchange["reference product"] = NATIVE_PRODUCT
    for field in ("input", "output", "database", "code"):
        exchange.pop(field, None)
    importer.data = [revised if d is stack else d for d in data if d is not producer]
    return 1
