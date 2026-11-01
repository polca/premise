"""Select native finished transformer-core steel for the reviewed 3.12 target.

Only the default electrolysis importer opts into this post-migration hook.
Its unchanged workbook retains the legacy 3.9 recipe for older builds.
"""

from copy import deepcopy
import hashlib
import json

from packaging.version import Version

NAME = "transformer production, electrolysis, 1.1 mva"
NATIVE = (
    "market for grain oriented electrical steel, 0.23 mm coil",
    "grain oriented electrical steel, 0.23 mm coil",
    "RER",
    "kilogram",
)
CORE_MASS = 947.32
TRANSFORMER_CORE_CACHE_REVISION = 1
LEGACY_SCIENCE_SHA256 = (
    "5b25530d2a883abecdec69655648d1fd7792d6ea0bffde2ef795f4a7d1b1728c"
)
NATIVE_SCIENCE_SHA256 = (
    "948b90f2d2f1e15c888681603d753d85ec4811e21874382216a999f3ee4e52ff"
)
CORE_NAMES = frozenset(
    {
        "market for steel, unalloyed",
        "market for silicon, metallurgical grade",
        "market for hot rolling, steel",
        "sheet rolling, steel",
        "market group for electricity, medium voltage",
        "heat production, natural gas, at industrial furnace >100kW",
    }
)
NOTE = (
    "APPROVED NATIVE TRANSFORMER-CORE SUPPLY FOR TARGET 3.12, 2026-10-27. "
    "Purchase 947.32 kg native RER grain-oriented electrical steel, 0.23 mm "
    "finished coil. Replace the six reviewed core alloying, rolling and "
    "annealing inputs together; retain all other transformer inputs, core "
    "mass, quantities and retirement references. Native nominal silicon is "
    "3.2%, replacing the legacy 3% engineering alloy assumption. Regional "
    "procurement and grade/gauge suitability remain explicit proxies. Core "
    "cutting, stacking, insulation and electrical-design coverage remain "
    "unverified. The source workbook remains 3.9; this selection applies "
    "after migration only for 3.12 targets. Earlier core-production notes "
    "below are superseded on this route.\n\n"
)
EXCHANGE_NOTE = (
    "Qualified native RER 0.23 mm GOES coil, nominal 3.2% Si; replaces the "
    "six reviewed core-feed/rolling/annealing inputs together. Core cutting, "
    "stacking and actual grade suitability remain unverified."
)
_METADATA = frozenset(
    {"comment", "source", "database", "code", "input", "output", "worksheet name"}
)


def native_transformer_core_enabled(version):
    """The reviewed choice applies to the 3.12 family, not unreviewed targets."""
    return version is not None and Version(str(version)).release[:2] == (3, 12)


def _science_digest(activity):
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
        {key: value for key, value in exchange.items() if key not in _METADATA}
        for exchange in activity["exchanges"]
    ]
    payload = json.dumps(
        science, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _identity(record):
    product = record.get("reference product", record.get("product"))
    if "product" in record and record["product"] != product:
        raise ValueError("Transformer-core supplier product fields disagree")
    return record.get("name"), product, record.get("location"), record.get("unit")


def use_native_transformer_core(importer, version, database):
    """Replace the complete reviewed core boundary before linking, atomically.

    Validate the whole scientific recipe, including unknown exchange fields,
    rather than silently discarding a later source correction. Background
    activities and all other imported activities are read only. Runtime uses
    no authoring fixtures, licensed inventory copies or external files.
    """
    if not native_transformer_core_enabled(version):
        return 0
    selected = [d for d in importer.data if d.get("name") == NAME]
    if len(selected) != 1:
        raise ValueError("Native transformer core requires one reviewed transformer")
    transformer = selected[0]
    try:
        digest = _science_digest(transformer)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("Transformer-core scientific recipe is invalid") from error
    if digest not in {LEGACY_SCIENCE_SHA256, NATIVE_SCIENCE_SHA256}:
        raise ValueError("Transformer-core scientific recipe changed")
    suppliers = [d for d in database if d.get("name") == NATIVE[0]]
    suppliers = [d for d in suppliers if _identity(d) == NATIVE]
    if len(suppliers) != 1:
        raise ValueError("Native transformer-core supplier must exist uniquely in 3.12")
    if digest == NATIVE_SCIENCE_SHA256:
        return 0

    revised = deepcopy(transformer)
    native = {
        "name": NATIVE[0],
        "reference product": NATIVE[1],
        "product": NATIVE[1],
        "location": NATIVE[2],
        "unit": NATIVE[3],
        "type": "technosphere",
        "amount": CORE_MASS,
        "comment": EXCHANGE_NOTE,
    }
    revised["exchanges"] = [
        revised["exchanges"][0],
        native,
        *[e for e in revised["exchanges"][1:] if e["name"] not in CORE_NAMES],
    ]
    revised["comment"] = NOTE + transformer.get("comment", "")
    if _science_digest(revised) != NATIVE_SCIENCE_SHA256:
        raise ValueError("Native transformer-core replacement boundary is inconsistent")
    importer.data = [revised if d is transformer else d for d in importer.data]
    return 1
