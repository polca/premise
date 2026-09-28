"""Helpers for loading and evaluating IAM heat variables.

Heat mappings differ from most premise mappings because a technology can be a
linear expression (for example, a parent IAM variable minus its explicitly
reported children).  This module keeps that small expression language separate
from the generic market-share loader, which deliberately only supports aliases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import numpy as np
import xarray as xr

from .runtime_cache import load_yaml_cached

HEAT_LAYERS = (
    "buildings_end_use",
    "industrial_end_use",
    "secondary_supply",
)
ABSOLUTE_TOLERANCE = 1e-8
RELATIVE_TOLERANCE = 1e-5


def _as_terms(expression: Any) -> List[Dict[str, Any]]:
    """Return a mapping expression as normalized linear terms."""

    if isinstance(expression, str):
        return [{"variable": expression, "coefficient": 1.0}]
    if isinstance(expression, list):
        if all(isinstance(item, str) for item in expression):
            return [
                {"variable": variable, "coefficient": 1.0} for variable in expression
            ]
        return [
            {
                "variable": item["variable"],
                "coefficient": float(item.get("coefficient", 1.0)),
            }
            for item in expression
        ]
    if isinstance(expression, dict) and "terms" in expression:
        return _as_terms(expression["terms"])
    raise TypeError(f"Unsupported heat IAM expression: {expression!r}")


def load_heat_mapping(filepath: Path, model: str) -> Dict[str, Dict[str, Any]]:
    """Load heat mapping metadata relevant to ``model``."""

    mapping = load_yaml_cached(filepath)

    selected = {}
    for technology, metadata in mapping.items():
        layer = metadata.get("layer")
        aliases = metadata.get("iam_aliases", {})
        if layer not in HEAT_LAYERS or model not in aliases:
            continue
        selected[technology] = {
            **metadata,
            "terms": _as_terms(aliases[model]),
        }
    return selected


def heat_expression_variables(mapping: Mapping[str, Mapping[str, Any]]) -> List[str]:
    """Return the unique raw IAM variables required by heat expressions."""

    return sorted(
        {
            term["variable"]
            for metadata in mapping.values()
            for term in metadata["terms"]
        }
    )


def split_heat_mapping_by_layer(
    mapping: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Group model-specific heat mapping metadata by market layer."""

    return {
        layer: {
            technology: dict(metadata)
            for technology, metadata in mapping.items()
            if metadata["layer"] == layer
        }
        for layer in HEAT_LAYERS
    }


def _expression_reference(
    data: xr.DataArray, terms: Iterable[Mapping[str, Any]]
) -> xr.DataArray:
    """Return a positive scale used by the residual closure tolerance."""

    positive = [
        data.sel(variables=term["variable"]).fillna(0)
        * max(float(term["coefficient"]), 0.0)
        for term in terms
        if float(term["coefficient"]) > 0
    ]
    if not positive:
        return xr.zeros_like(data.isel(variables=0, drop=True))
    reference = positive[0]
    for value in positive[1:]:
        reference = reference + value
    return abs(reference)


def evaluate_heat_layer(
    data: xr.DataArray,
    mapping: Mapping[str, Mapping[str, Any]],
    layer: str,
) -> tuple[xr.DataArray | None, Dict[str, Any]]:
    """Evaluate unnormalised heat volumes for one layer.

    If no required variable is present, the complete layer is considered
    unavailable.  Once any required variable is present, all variables are
    required; this prevents silently normalising a partial technology set.
    """

    layer_mapping = {
        technology: metadata
        for technology, metadata in mapping.items()
        if metadata["layer"] == layer
    }
    diagnostics: Dict[str, Any] = {
        "layer": layer,
        "available": False,
        "tiny negative clips": [],
        "assumptions": {},
        "residual technologies": [],
    }
    if not layer_mapping:
        return None, diagnostics

    required = {
        term["variable"]
        for metadata in layer_mapping.values()
        for term in metadata["terms"]
    }
    available = set(str(v) for v in data.coords["variables"].values)
    present = required & available
    if not present:
        return None, diagnostics
    missing = sorted(required - available)
    if missing:
        raise ValueError(
            f"Heat layer {layer!r} is partially available. Missing required IAM "
            f"variables: {missing}."
        )

    pieces = []
    units = {}
    for technology, metadata in layer_mapping.items():
        expression = None
        for term in metadata["terms"]:
            value = data.sel(variables=term["variable"]).fillna(0)
            value = value * float(term["coefficient"])
            expression = value if expression is None else expression + value

        if metadata.get("residual", False):
            reference = _expression_reference(data, metadata["terms"])
            tolerance = xr.apply_ufunc(
                np.maximum,
                ABSOLUTE_TOLERANCE,
                RELATIVE_TOLERANCE * reference,
            )
            invalid = expression < -tolerance
            diagnostics["residual technologies"].append(technology)
        else:
            invalid = expression < -ABSOLUTE_TOLERANCE

        if bool(invalid.any()):
            minimum = float(expression.min().values)
            raise ValueError(
                f"Heat expression for {technology!r} in {layer!r} is negative "
                f"({minimum} EJ/yr), beyond the accepted closure tolerance."
            )

        tiny_negative = (expression < 0) & ~invalid
        if bool(tiny_negative.any()):
            diagnostics["tiny negative clips"].append(technology)
        expression = expression.clip(min=0)

        if "variables" not in expression.dims:
            expression = expression.expand_dims("variables")
        expression = expression.assign_coords(variables=[technology])
        pieces.append(expression)

        first_variable = metadata["terms"][0]["variable"]
        unit = data.attrs.get("unit", {}).get(first_variable)
        if unit is not None:
            units[technology] = unit
        if metadata.get("assumption"):
            diagnostics["assumptions"][technology] = metadata["assumption"]

    result = xr.concat(pieces, dim="variables")
    result.attrs = {
        "unit": units,
        "energy_basis": {
            technology: metadata["energy_basis"]
            for technology, metadata in layer_mapping.items()
        },
        "conversion": {
            technology: metadata.get("conversion", "none")
            for technology, metadata in layer_mapping.items()
        },
        "residual": {
            technology: bool(metadata.get("residual", False))
            for technology, metadata in layer_mapping.items()
        },
    }
    diagnostics["available"] = True
    diagnostics["technologies"] = list(result.coords["variables"].values)
    return result, diagnostics


def evaluate_heat_layers(
    data: xr.DataArray, mapping: Mapping[str, Mapping[str, Any]]
) -> tuple[Dict[str, xr.DataArray | None], Dict[str, Any]]:
    """Evaluate every heat layer and return raw volumes plus diagnostics."""

    arrays = {}
    diagnostics = {}
    for layer in HEAT_LAYERS:
        arrays[layer], diagnostics[layer] = evaluate_heat_layer(data, mapping, layer)
    return arrays, diagnostics
