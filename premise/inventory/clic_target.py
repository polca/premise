"""Explicit destination contexts for CLIC inventory preparation."""

from __future__ import annotations


def resolve_inventory_target(version, *, family="ecoinvent", system_model="cutoff"):
    """Resolve the supported technosphere and biosphere as separate axes.

    UVEK 2026 preparation uses the ecoinvent 3.10 biosphere. This context
    describes inventory conversion and linking, not sector-transformation
    compatibility. EN15804 retains its existing ecoinvent cut-off bridge.
    """
    from brightpath import BackgroundContext, BiosphereProfile, TechnosphereProfile

    family = str(family).strip().casefold()
    if family not in {"ecoinvent", "uvek"}:
        raise ValueError(f"Unsupported inventory target family {family!r}.")
    if family == "uvek":
        if str(version) != "2026":
            raise ValueError("The supported UVEK inventory target is release 2026.")
        if system_model != "cutoff":
            raise ValueError(
                "UVEK inventory preparation requires system_model='cutoff'."
            )
        return BackgroundContext(
            TechnosphereProfile("uvek", "2026", "cutoff"),
            BiosphereProfile("ecoinvent", "3.10"),
        )

    from ..core.utils import check_ecoinvent_target_version

    version = check_ecoinvent_target_version(version)
    if system_model not in {"cutoff", "consequential", "EN15804"}:
        raise ValueError(f"Unsupported inventory target system model {system_model!r}.")
    return BackgroundContext(
        TechnosphereProfile(
            "ecoinvent",
            version,
            "cutoff" if system_model == "EN15804" else system_model,
        ),
        BiosphereProfile("ecoinvent", version),
    )
