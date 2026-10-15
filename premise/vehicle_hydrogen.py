"""Keep explicitly identified tank-compression electricity coupled to H2 use."""

import json
import math
from collections.abc import Mapping

from .utils import rescale_exchange

COMPRESSION_KEY = "hydrogen tank compression"
COMPRESSION_COMMENT = "Hydrogen tank compression"


def compression_specification(dataset):
    """Read the activity-level contract retained through exchange relinking."""
    specification = dataset.get(COMPRESSION_KEY)
    if specification is None:
        return None
    if isinstance(specification, str):
        specification = json.loads(specification)
    if not isinstance(specification, Mapping):
        raise ValueError("Hydrogen tank compression metadata must be a mapping")
    intensity = specification["electricity kWh per kg H2"]
    if (
        not isinstance(intensity, (int, float))
        or not math.isfinite(intensity)
        or intensity <= 0
    ):
        raise ValueError("Hydrogen compression intensity must be finite and positive")
    return specification


def compression_electricity_product(exchange, specification):
    """Select electricity inputs that use the documented compression product."""
    return (
        exchange.get("type") == "technosphere"
        and exchange.get("product", exchange.get("reference product"))
        == specification["electricity product"]
        and exchange.get("unit") == "kilowatt hour"
    )


def compression_electricity(exchange, specification):
    """Select electricity inputs explicitly documented as tank compression."""
    return compression_electricity_product(exchange, specification) and str(
        exchange.get("comment", "")
    ).startswith(COMPRESSION_COMMENT)


def hydrogen_amount(dataset, specification):
    exchanges = [
        exchange
        for exchange in dataset["exchanges"]
        if exchange.get("type") == "technosphere"
        and exchange.get("product", exchange.get("reference product"))
        == specification["hydrogen product"]
    ]
    if not exchanges or any(
        exchange.get("unit") != "kilogram"
        or not math.isfinite(exchange["amount"])
        or exchange["amount"] < 0
        for exchange in exchanges
    ):
        raise ValueError(
            "Compression requires finite nonnegative hydrogen purchases in kg"
        )
    return sum(exchange["amount"] for exchange in exchanges)


def prepare_hydrogen_compression(dataset):
    """Identify and validate the documented compression exchanges."""
    specification = compression_specification(dataset)
    if specification is None:
        return None
    exchanges = [
        e for e in dataset["exchanges"] if compression_electricity(e, specification)
    ]
    if not exchanges or any(
        not math.isfinite(e["amount"]) or e["amount"] < 0 for e in exchanges
    ):
        raise ValueError("Compression electricity must be finite and nonnegative")
    recorded = sum(e["amount"] for e in exchanges)
    expected = (
        hydrogen_amount(dataset, specification)
        * specification["electricity kWh per kg H2"]
    )
    if not math.isclose(recorded, expected, rel_tol=1e-8, abs_tol=1e-12):
        raise ValueError(
            f"Compression electricity is {recorded} kWh; hydrogen use requires "
            f"{expected} kWh"
        )
    return specification, exchanges


def finish_hydrogen_compression(dataset, prepared):
    """Set compression from final H2 demand without scaling it a second time."""
    if prepared is None:
        return
    specification, exchanges = prepared
    hydrogen = hydrogen_amount(dataset, specification)
    target = hydrogen * specification["electricity kWh per kg H2"]
    old_total = sum(e["amount"] for e in exchanges)
    if old_total == 0 and target > 0:
        raise ValueError("Cannot allocate compression to an empty electricity supply")
    if old_total > 0 and not math.isclose(
        target, old_total, rel_tol=1e-14, abs_tol=1e-16
    ):
        for exchange in exchanges:
            rescale_exchange(exchange, target / old_total, remove_uncertainty=False)
