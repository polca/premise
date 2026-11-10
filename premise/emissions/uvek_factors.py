"""Review native GAINS reductions without changing an inventory or legacy rules."""

from __future__ import annotations

import math
from numbers import Real

import numpy as np
import xarray as xr


def plan_native_gains_reductions(data, year):
    """Return factors and explicit dispositions relative to the exact 2020 base.

    This is a read-only building block, not an emissions update. It retains
    Premise's reduction-only convention but distinguishes absent data, undefined
    baselines, zero targets and increases instead of treating them as measured
    factors of one. Activity/air-compartment and GAINS-sector binding still need
    independent review. Pollutant labels are never interpreted as size bins here.
    """
    dimensions = {"region", "sector", "year", "pollutant"}
    if set(data.dims) != dimensions:
        raise ValueError("Native GAINS needs region, sector, year and pollutant axes.")
    for dimension in dimensions:
        if len(set(data[dimension].values.tolist())) != data.sizes[dimension]:
            raise ValueError(f"Duplicate native GAINS {dimension} coordinate.")
    if (
        not isinstance(year, Real)
        or isinstance(year, bool)
        or not math.isfinite(year)
        or 2020 not in data.year.values
        or not float(data.year.min()) <= year <= float(data.year.max())
    ):
        raise ValueError("Native GAINS needs the exact 2020 base and an in-range year.")
    if np.isinf(data.values).any() or bool((data < 0).any()):
        raise ValueError("Native GAINS observations cannot be negative or infinite.")

    regional = data.drop_sel(region="World") if "World" in data.region else data
    if not regional.sizes["region"]:
        raise ValueError("World GAINS needs independent regional observations.")
    # The source World aggregate is not counted as another emitting region.
    # Incomplete regional evidence remains incomplete rather than becoming zero.
    world = regional.sum(
        "region", skipna=True, min_count=regional.sizes["region"]
    ).expand_dims(region=["World"])
    reviewed = xr.concat([regional, world], dim="region")
    baseline = reviewed.sel(year=2020, drop=True)
    selected = (
        reviewed.sel(year=year, drop=True)
        if year in reviewed.year.values
        else reviewed.sortby("year").interp(year=year).drop_vars("year")
    )
    baseline, selected = xr.align(baseline, selected, join="exact")
    valid = np.isfinite(baseline) & np.isfinite(selected)
    positive_base = baseline > 0
    ratio = selected / baseline.where(positive_base)
    reduction = valid & positive_base & (selected > 0) & (ratio < 1)
    status = xr.full_like(baseline, "missing_data", dtype=object)
    status = xr.where(valid & ~positive_base, "undefined_zero_baseline", status)
    status = xr.where(
        valid & positive_base & (selected == 0), "zero_projection_not_applied", status
    )
    status = xr.where(valid & positive_base & (ratio == 1), "unchanged", status)
    status = xr.where(
        valid & positive_base & (ratio > 1), "increase_not_applied", status
    )
    status = xr.where(reduction, "reduction", status)
    return {
        "factors": ratio.where(reduction),
        "status": status,
        "diagnostics": {
            "base_year": 2020,
            "selected_year": year,
            "reduction_only": True,
            "source_World_recomputed_from_regions": True,
            "missing_regional_observations_not_zero_filled": True,
            "dispositions": {
                str(value): int((status == value).sum())
                for value in np.unique(status.values)
            },
            "inventory_updated": False,
            "pollutant_bins_inferred": False,
        },
    }
