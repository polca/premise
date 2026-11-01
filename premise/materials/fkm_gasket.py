"""Omit the reviewed legacy FKM chain from the VRFB additional inventory.

The caller scopes this hook to lci-batteries-vanadium-redox-flow.xlsx, before
DefaultInventory preparation. The separate 3.12 workbook supplies the same
gasket identity, so existing purchases require no rewriting.
"""

import hashlib
import json

from packaging.version import Version

GASKET_NAME = "gasket production, FKM, for VRFB system"
HELPER_PRODUCTS = {
    "vinylidene fluoride production, chlorination and fluorination": (
        "vinylidene fluoride, chlorination and fluorination"
    ),
    "hexafluoropropylene production, tetrafluoroethylene pyrolysis": (
        "hexafluoropropylene, tetrafluoroethylene pyrolysis"
    ),
    "fluoroelastomer production, fkm terpolymer, 75/16/9 molar composition": (
        "fluoroelastomer, FKM terpolymer, 75/16/9 molar composition"
    ),
    "fluoroelastomer gasket compound production, fkm, fe5540 type": (
        "fluoroelastomer gasket compound, FKM, FE5540 type"
    ),
}

# ALIGN019: canonical JSON of the five legacy-science.json source records,
# frozen before migration. Runtime must not depend on scripts/data or /tmp.
LEGACY_ACTIVITY_SCIENCE_SHA256 = {
    GASKET_NAME: "759a13f40f851db418f8eda0a1e72130451a9bfba1d80774e70335ff9f6b2ecf",
    "vinylidene fluoride production, chlorination and fluorination": (
        "9ea78e1f469d1699593bbb9ee8a1bcd24eb9925c0c73fde74b2f3df53cf9ad59"
    ),
    "hexafluoropropylene production, tetrafluoroethylene pyrolysis": (
        "97bdeee23e437c6bede3b9ff8241512f761b366cca1316fbddbfa16dabbcfd5a"
    ),
    "fluoroelastomer production, fkm terpolymer, 75/16/9 molar composition": (
        "458c4c1d5b4390b29ddb70ef5b3ab0c87479091deef3521a2b688ecf278c04d0"
    ),
    "fluoroelastomer gasket compound production, fkm, fe5540 type": (
        "0c03128f32727fa2aa52e6b3012702719ec26cc3b78459b00aedf02cfbcd5f75"
    ),
}

_EXCHANGE_METADATA = frozenset(
    {"comment", "source", "database", "code", "input", "output", "worksheet name"}
)


def _science_digest(activity):
    # Dataset descriptive metadata/type labels are outside this source digest.
    # Keep the declared identity and production basis; retain EVERY exchange
    # field except descriptive metadata/link codes, including both product
    # aliases, uncertainty, formulas, replacement fields and unknown additions.
    science = {
        field: activity.get(field)
        for field in (
            "name",
            "reference product",
            "location",
            "unit",
            "production amount",
        )
    }
    if "product" in activity:
        science["product"] = activity["product"]
    science["exchanges"] = [
        {key: value for key, value in exchange.items() if key not in _EXCHANGE_METADATA}
        for exchange in activity["exchanges"]
    ]
    payload = json.dumps(
        science, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def use_native_fkm_gasket(importer, version, other_importers=()):
    """Remove exactly five reviewed source activities, returning 5 or 0.

    Targets below 3.12 are strict no-ops. Validate all selected scientific
    content and incoming private-helper references before changing data. Other
    preloaded importers are read only, and may include ``importer`` itself.
    An already selected source is idempotent only if no helper references remain.
    """
    if Version(str(version)) < Version("3.12"):
        return 0

    data = importer.data
    selected = [d for d in data if d.get("name") in LEGACY_ACTIVITY_SCIENCE_SHA256]
    if selected:
        names = [d["name"] for d in selected]
        if len(names) != 5 or set(names) != LEGACY_ACTIVITY_SCIENCE_SHA256.keys():
            raise ValueError("FKM gasket requires all five unique legacy activities")
        for activity in selected:
            try:
                digest = _science_digest(activity)
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    f"FKM gasket legacy science is invalid: {activity['name']}"
                ) from error
            if digest != LEGACY_ACTIVITY_SCIENCE_SHA256[activity["name"]]:
                raise ValueError(
                    f"FKM gasket legacy science changed: {activity['name']}"
                )

    selected_ids = {id(d) for d in selected}
    helper_products = set(HELPER_PRODUCTS.values())
    helper_links = {
        (d.get("database"), d["code"])
        for d in selected
        if d["name"] in HELPER_PRODUCTS and "code" in d
    }
    retained = [d for d in data if id(d) not in selected_ids]
    for source in (importer, *other_importers):
        for activity in source.data:
            if id(activity) in selected_ids:
                continue
            for exchange in activity.get("exchanges", ()):
                if exchange.get("type") != "technosphere":
                    continue
                link = exchange.get("input")
                if (
                    exchange.get("name") in HELPER_PRODUCTS
                    or any(
                        exchange.get(field) in helper_products
                        for field in ("product", "reference product")
                    )
                    or (
                        isinstance(link, (tuple, list))
                        and len(link) == 2
                        and tuple(link) in helper_links
                    )
                ):
                    raise ValueError(
                        "FKM gasket private helper has an unreviewed consumer: "
                        f"{activity.get('name')} -> {exchange.get('name')}"
                    )

    if not selected:
        return 0
    importer.data = retained
    return 5
