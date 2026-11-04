"""Pure declarative planner for GAINS hot-pollutant factors."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from ..declarative import (
    ExchangeFactor,
    LatentReference,
    MatrixCoordinate,
    MetadataProposal,
    SectorPatch,
)
from ..inventory.activity_maps import GAINS_MAPPING, get_mapping


def _activity_field(activity, field: str) -> str:
    if field == "reference product":
        return activity.product
    if hasattr(activity, field):
        return str(getattr(activity, field) or "")
    return str(activity.metadata.get(field, ""))


def _normalise_filters(filters: Any, *, default_field: str = "name") -> dict:
    if isinstance(filters, str):
        return {default_field: [filters]}
    if isinstance(filters, list):
        return {default_field: filters}
    return {
        field: values if isinstance(values, list) else [values]
        for field, values in (filters or {}).items()
    }


def compile_activity_sector_mapping(reference: LatentReference) -> dict[str, str]:
    """Apply the legacy GAINS contains/mask rules to the complete latent set."""

    reverse = {}
    mappings = get_mapping(filepath=GAINS_MAPPING, var="ecoinvent_aliases")
    for sector, specification in mappings.items():
        filters = _normalise_filters(specification.get("fltr"))
        masks = _normalise_filters(specification.get("mask"))
        for activity in reference.activities:
            selected = all(
                any(str(term) in _activity_field(activity, field) for term in terms)
                for field, terms in filters.items()
            )
            excluded = any(
                str(term) in _activity_field(activity, field)
                for field, terms in masks.items()
                for term in terms
            )
            if selected and not excluded:
                reverse[activity.name] = sector
    return reverse


@dataclass(frozen=True, slots=True)
class EmissionsPatchPlanner:
    """Plan emissions changes without reading another sector's output."""

    pollutant_mapping: Mapping[str, str]
    activity_sector_mapping: Mapping[str, str]
    location_mapping: Mapping[str, str]
    gains_factors: Mapping[tuple[str, str, str], float]
    gains_regions: frozenset[str]

    def __post_init__(self) -> None:
        for name in (
            "pollutant_mapping",
            "activity_sector_mapping",
            "location_mapping",
            "gains_factors",
        ):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))
        object.__setattr__(self, "gains_regions", frozenset(self.gains_regions))

    @classmethod
    def from_updater(
        cls, updater: Any, reference: LatentReference
    ) -> EmissionsPatchPlanner:
        """Freeze all mutable updater/IAM state needed by the pure planner."""

        if not hasattr(updater, "_gains_factor_values"):
            updater._compile_gains_factor_lookup()
        location_mapping = dict(updater.ecoinvent_to_iam_loc)
        for activity in reference.activities:
            location_mapping.setdefault(
                activity.location,
                updater.geo.ecoinvent_to_iam_location(activity.location),
            )
        gains_factors = {
            (str(region), str(pollutant), str(sector)): float(
                updater._gains_factor_values[
                    region_position,
                    pollutant_position,
                    sector_position,
                ]
            )
            for region, region_position in updater._gains_region_index.items()
            for pollutant, pollutant_position in updater._gains_pollutant_index.items()
            for sector, sector_position in updater._gains_sector_index.items()
        }
        return cls(
            pollutant_mapping=updater.ei_pollutants,
            activity_sector_mapping=compile_activity_sector_mapping(reference),
            location_mapping=location_mapping,
            gains_factors=gains_factors,
            gains_regions=frozenset(
                str(value) for value in updater._gains_region_index
            ),
        )

    def _factor(self, *, region: str, pollutant: str, sector: str) -> float:
        value = float(self.gains_factors.get((region, pollutant, sector), 1.0))
        return 1.0 if not math.isfinite(value) or value == 0.0 else value

    def plan(self, reference: LatentReference) -> SectorPatch:
        factors: dict[MatrixCoordinate, float] = {}
        metadata = []
        for activity in reference.activities:
            gains_sector = self.activity_sector_mapping.get(activity.name)
            region = (
                activity.location
                if activity.location in self.gains_regions
                else self.location_mapping.get(activity.location)
            )
            if gains_sector is None or region not in self.gains_regions:
                continue
            log_parameters = dict(activity.metadata.get("log parameters", {}))
            previously_processed = frozenset(log_parameters)
            changed = False
            for exchange in activity.exchanges:
                if exchange.type != "biosphere":
                    continue
                pollutant = self.pollutant_mapping.get(exchange.name)
                if pollutant is None or pollutant in previously_processed:
                    continue
                factor = self._factor(
                    region=region,
                    pollutant=pollutant,
                    sector=gains_sector,
                )
                if not 1.0 > factor > 0.0:
                    continue
                coordinate = MatrixCoordinate(
                    "biosphere", exchange.input_identity, activity.identity
                )
                previous = factors.get(coordinate)
                if previous is not None and previous != factor:
                    raise ValueError(
                        f"One biosphere coordinate received two GAINS factors: "
                        f"{coordinate!r}: {previous} != {factor}"
                    )
                factors[coordinate] = factor
                log_parameters.setdefault("GAINS sector", gains_sector)
                log_parameters[pollutant] = factor
                changed = True
            if changed:
                metadata.append(
                    MetadataProposal.from_value(
                        activity.identity,
                        "log parameters",
                        log_parameters,
                    )
                )

        return SectorPatch(
            sector="emissions",
            model=reference.model,
            pathway=reference.pathway,
            year=reference.year,
            reference_digest=reference.digest,
            factors=tuple(
                ExchangeFactor(coordinate, factor)
                for coordinate, factor in factors.items()
            ),
            metadata=tuple(metadata),
        )


__all__ = ["EmissionsPatchPlanner", "compile_activity_sector_mapping"]
