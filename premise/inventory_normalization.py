"""Dependency-light inventory normalization shared by validation and reporting."""

import math
import numpy as np


def normalize_inventory_numeric_types(database, on_change=None):
    """Apply the historical scalar numeric conversions before validation.

    IAM calculations often produce zero-dimensional or one-element NumPy
    arrays.  Export preparation has always converted these values with
    ``float``.  Keeping that conversion in an explicit normalization function
    lets semantic validation reject genuinely non-scalar arrays without
    mutating the graph it checks.
    """

    def normalize_scalar(value):
        if isinstance(value, np.ndarray):
            if value.size == 1:
                return float(value.reshape(-1)[0])
            return value
        if isinstance(value, np.generic):
            return float(value)
        return value

    def assign_if_converted(mapping, key, value):
        normalized = normalize_scalar(value)
        if normalized is not value:
            mapping[key] = normalized
            return True
        return False

    for dataset in database:
        changed = False
        for value in dataset.values():
            if isinstance(value, dict):
                for key, item in value.items():
                    changed = assign_if_converted(value, key, item) or changed
        for exchange in dataset.get("exchanges", ()):
            amount = exchange.get("amount")
            if (
                isinstance(amount, (int, float, np.number, np.ndarray))
                and not isinstance(amount, (bool, np.bool_))
                and (not isinstance(amount, np.ndarray) or amount.size == 1)
            ):
                normalized_amount = float(np.asarray(amount).reshape(-1)[0])
                if type(amount) is not float:
                    exchange["amount"] = normalized_amount
                    changed = True
            for key, value in tuple(exchange.items()):
                changed = assign_if_converted(exchange, key, value) or changed
        if changed and on_change is not None:
            on_change(dataset, "numeric_types")
    return database


def normalize_inventory_uncertainty(database, on_change=None):
    """Apply the historical uncertainty repairs before read-only checks."""

    for dataset in database:
        changed = False
        for exchange in dataset.get("exchanges", ()):
            try:
                uncertainty_type = int(exchange.get("uncertainty type", 0))
            except (TypeError, ValueError, OverflowError):
                continue
            amount = exchange.get("amount")
            numeric_amount = (
                isinstance(amount, (int, float, np.number))
                and not isinstance(amount, (bool, np.bool_))
                and np.isfinite(amount)
            )
            if uncertainty_type == 2 and numeric_amount:
                if amount == 0:
                    # A lognormal distribution cannot represent zero. Some
                    # transformations legitimately scale an exchange to zero;
                    # make that result deterministic before read-only
                    # certification instead of leaving stale distribution
                    # metadata attached to it.
                    exchange["uncertainty type"] = 0
                    exchange["loc"] = 0.0
                    if "negative" in exchange:
                        exchange.pop("negative")
                    for field in ("scale", "minimum", "maximum", "shape"):
                        exchange.pop(field, None)
                    changed = True
                else:
                    if "loc" not in exchange:
                        exchange["loc"] = float(math.log(abs(float(amount))))
                        changed = True
                    negative = bool(amount < 0)
                    if exchange.get("negative") is not negative:
                        exchange["negative"] = negative
                        changed = True
            elif uncertainty_type == 3 and numeric_amount and "loc" not in exchange:
                exchange["loc"] = float(amount)
                changed = True
            elif uncertainty_type == 5 and numeric_amount:
                if "loc" not in exchange:
                    exchange["loc"] = float(amount)
                    changed = True
                if "minimum" in exchange and exchange["minimum"] > exchange["loc"]:
                    exchange["minimum"] = exchange["loc"]
                    changed = True
                if "maximum" in exchange and exchange["maximum"] < exchange["loc"]:
                    exchange["maximum"] = exchange["loc"]
                    changed = True
        if changed and on_change is not None:
            on_change(dataset, "uncertainty")
    return database


def normalize_exact_deterministic_exchange_duplicates(database, on_change=None):
    """Consolidate only byte-equivalent deterministic technosphere rows.

    The summed amount preserves the deterministic inventory total. Exchanges
    with uncertainty, differing metadata, or merely equal supplier keys are
    deliberately left for validation because combining their distributions or
    semantics would require methodological judgment.
    """

    for dataset in database:
        normalized = []
        exact_rows = {}
        duplicate_found = False
        for exchange in dataset.get("exchanges", ()):
            if exchange.get("type") != "technosphere" or int(
                exchange.get("uncertainty type", 0) or 0
            ) not in {0, 1}:
                normalized.append(exchange)
                continue
            signature = repr(
                sorted((str(key), repr(value)) for key, value in exchange.items())
            )
            existing = exact_rows.get(signature)
            if existing is None:
                exact_rows[signature] = exchange
                normalized.append(exchange)
            else:
                existing["amount"] += exchange["amount"]
                duplicate_found = True
        if duplicate_found:
            dataset["exchanges"] = normalized
            if on_change is not None:
                on_change(dataset, "exact_duplicates")
    return database
