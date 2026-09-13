"""Lossless copying of mutable inventory payloads without scalar dispatch."""

from __future__ import annotations

import copy
from typing import Any

import numpy as np

_ATOMIC_TYPES = frozenset((type(None), bool, int, float, complex, str, bytes))
_NUMPY_ATOMIC_TYPES = frozenset(
    scalar
    for scalar in np.sctypeDict.values()
    if issubclass(scalar, np.generic) and scalar is not np.void
)


def clone_inventory_dataset(dataset: Any, memo: dict[int, Any] | None = None) -> Any:
    """Copy inventory containers, preserving aliases and custom copy protocols.

    Strings and numeric scalars dominate inventory payloads. Handle ordinary
    dictionaries, lists and tuples directly, and delegate uncommon types to
    ``deepcopy``. A shared memo retains cycles and shared mutable metadata.
    NumPy object arrays and structured scalars also use ``deepcopy`` because
    their contents can be mutable.
    """
    return _clone_inventory_value(dataset, {} if memo is None else memo)


def _clone_inventory_value(value: Any, memo: dict[int, Any]) -> Any:
    # A recursive closure would retain its memo in a cycle after every copy.
    atomic_types = _ATOMIC_TYPES
    value_type = type(value)
    if value_type in atomic_types:
        return value
    if value_type in _NUMPY_ATOMIC_TYPES:
        return value
    if (
        value_type is tuple
        and len(value) == 2
        and type(value[0]) in atomic_types
        and type(value[1]) in atomic_types
    ):
        return value

    value_id = id(value)
    if value_id in memo:
        return memo[value_id]

    if value_type is dict:
        duplicate = {}
        memo[value_id] = duplicate
        for key, item in value.items():
            duplicate_key = (
                key if type(key) in atomic_types else _clone_inventory_value(key, memo)
            )
            duplicate[duplicate_key] = (
                item
                if type(item) in atomic_types
                else _clone_inventory_value(item, memo)
            )
        return duplicate

    if value_type is list:
        duplicate = value.copy()
        memo[value_id] = duplicate
        for position, item in enumerate(value):
            if type(item) not in atomic_types:
                duplicate[position] = _clone_inventory_value(item, memo)
        return duplicate

    if value_type is tuple:
        items = [
            item if type(item) in atomic_types else _clone_inventory_value(item, memo)
            for item in value
        ]
        # A mutable item can refer back to this tuple while being copied.
        if value_id in memo:
            return memo[value_id]
        duplicate = value if all(a is b for a, b in zip(value, items)) else tuple(items)
        memo[value_id] = duplicate
        return duplicate

    if value_type is np.ndarray and not value.dtype.hasobject:
        duplicate = value.copy(order="K")
        memo[value_id] = duplicate
        return duplicate

    return copy.deepcopy(value, memo)
