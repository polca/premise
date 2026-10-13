"""Keep explicitly identified tank-compression electricity coupled to H2 use."""

import json
import math
from collections.abc import Mapping

from .utils import rescale_exchange

COMPRESSION_KEY = "hydrogen tank compression"
BALANCE_KEY = "premise hydrogen compression balance"


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


def compression_electricity(exchange, specification):
    """Select the documented supply product, including market-group relinks."""
    return (
        exchange.get("type") == "technosphere"
        and exchange.get("product", exchange.get("reference product"))
        == specification["electricity product"]
        and exchange.get("unit") == "kilowatt hour"
    )


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
    """Separate the identified compression burden before efficiency scaling.

    Other uses of the same electricity product can be merged by relinking.
    Retain that residual unchanged; the specific compression intensity alone
    determines the amount following H2 consumption. Legacy unmarked inventories
    are not assigned an inferred compression purpose.
    """
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
    total = sum(e["amount"] for e in exchanges)
    compression = (
        hydrogen_amount(dataset, specification)
        * specification["electricity kWh per kg H2"]
    )
    residual = total - compression
    if residual < -max(1e-12, total * 1e-8):
        raise ValueError(
            "Recorded electricity is below the specified hydrogen compression"
        )
    return specification, exchanges, max(0.0, residual)


def finish_hydrogen_compression(dataset, prepared):
    """Set compression from final H2 demand without scaling it a second time."""
    if prepared is None:
        return
    specification, exchanges, residual = prepared
    hydrogen = hydrogen_amount(dataset, specification)
    compression = hydrogen * specification["electricity kWh per kg H2"]
    old_total = sum(e["amount"] for e in exchanges)
    target = compression + residual
    if old_total == 0 and target > 0:
        raise ValueError("Cannot allocate compression to an empty electricity supply")
    if old_total > 0 and not math.isclose(
        target, old_total, rel_tol=1e-14, abs_tol=1e-16
    ):
        for exchange in exchanges:
            rescale_exchange(exchange, target / old_total, remove_uncertainty=False)
    dataset[BALANCE_KEY] = {
        "hydrogen kg": hydrogen,
        "electricity kWh per kg H2": specification["electricity kWh per kg H2"],
        "compression electricity kWh": compression,
        "other electricity kWh": residual,
        "electricity product": specification["electricity product"],
        "boundary": "Vehicle tank compression after delivered hydrogen supply",
    }


def record_hydrogen_compression_balances(database, vehicle_type):
    """Cover retained templates and clear records on forwarding originals."""
    prefix = {
        "car": "transport, passenger car,",
        "truck": "transport, freight, lorry,",
    }[vehicle_type]
    for dataset in database:
        if (
            not dataset.get("name", "").startswith(prefix)
            or COMPRESSION_KEY not in dataset
        ):
            continue
        if dataset.get("emptied", False):
            dataset.pop(BALANCE_KEY, None)
        elif BALANCE_KEY not in dataset:
            finish_hydrogen_compression(dataset, prepare_hydrogen_compression(dataset))
