"""Background axes and faithful extraction for the UVEK electricity pilot."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import math
import re


@dataclass(frozen=True)
class BackgroundSpec:
    family: str
    version: str
    system_model: str
    biosphere_family: str
    biosphere_version: str

    def to_dict(self):
        return asdict(self)


def resolve_source_background(version, *, family="ecoinvent", system_model="cutoff"):
    """Keep a publisher release distinct from its canonical biosphere version."""
    family = str(family).strip().casefold()
    if family == "ecoinvent":
        from ..core.utils import check_ecoinvent_target_version

        version = check_ecoinvent_target_version(version)
        return BackgroundSpec(family, version, system_model, "ecoinvent", version)
    if family != "uvek":
        raise ValueError(f"Unsupported source_family {family!r}.")
    if str(version) != "2026" or system_model != "cutoff":
        raise ValueError(
            "The UVEK pilot requires release 2026 and system_model='cutoff'."
        )
    return BackgroundSpec("uvek", "2026", "cutoff", "ecoinvent", "3.10")


def validate_uvek_scope(sectors):
    """Refuse unverified sectors before inventory preparation or scenario mutation."""
    if sectors is None:
        return ["biomass", "electricity", "heat"]
    selected = [sectors] if isinstance(sectors, str) else sectors
    if not isinstance(selected, list) or not all(isinstance(s, str) for s in selected):
        raise TypeError("sectors must be a string or a list of strings.")
    unsupported = sorted(
        set(selected)
        - {"biomass", "electricity", "heat", "cement", "steel", "mining", "renewable"}
    )
    if unsupported:
        raise ValueError(
            "UVEK 2026 supports biomass, electricity, heat, cement, steel, mining and renewable; "
            f"unsupported sectors: {unsupported}."
        )
    return list(selected)


def extract_uvek_source(source, biosphere_name, *, keep_uncertainty=True):
    """Read a linked Brightway source without ecoinvent normalization strategies.

    Labels needed by Wurst come from the linked suppliers, never a version-based
    fallback table. Copies retain original coefficients, formulae and metadata.
    Background axes come from NewDatabase arguments, not custom Brightway
    metadata labels. The installed databases are read only.
    """
    import bw2data as bd

    if biosphere_name not in bd.databases:
        raise ValueError(
            f"The selected UVEK biosphere {biosphere_name!r} is not installed."
        )
    native = deepcopy(bd.Database(source).load())
    biosphere = bd.Database(biosphere_name).load()
    if not native:
        raise ValueError(f"The UVEK source {source!r} is empty.")
    records = list(native.values())
    # The BAFU ecospold export shortens long labels but retains the exact name
    # in its comment. Restore only that documented export convention, not fuzzy
    # matches between technologies. Preserve the imported labels for provenance.
    for activity in records:
        shortened = re.fullmatch(r"(.+) \[[0-9a-f]{10}\]", activity["name"])
        first_line = (activity.get("comment") or "").split("\n", 1)[0]
        if shortened and first_line.startswith("Full activity name: "):
            full_name = first_line.removeprefix("Full activity name: ").strip()
            if full_name.startswith(shortened[1]):
                activity["premise original source labels"] = {
                    key: activity[key] for key in ("name", "reference product")
                }
                if activity["reference product"] == activity["name"]:
                    activity["reference product"] = full_name
                activity["name"] = full_name
    for activity in records:
        for edge in activity["exchanges"]:
            reference = tuple(edge.get("input", ()))
            target = biosphere if edge["type"] == "biosphere" else native
            if reference not in target:
                raise ValueError(
                    f"UVEK exchange has no supplier in the declared destination: {reference!r}."
                )
            if not math.isfinite(float(edge["amount"])):
                raise ValueError(f"Nonfinite UVEK exchange in {activity['code']!r}.")
            supplier = target[reference]
            edge.update(name=supplier["name"], unit=supplier["unit"])
            if edge["type"] == "biosphere":
                edge["categories"] = tuple(supplier["categories"])
            else:
                edge.update(
                    product=supplier["reference product"], location=supplier["location"]
                )
            edge.pop("output", None)
    if not keep_uncertainty:
        from ..inventory.clean_datasets import remove_uncertainty

        remove_uncertainty(records)
    return records
