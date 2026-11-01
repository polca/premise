"""Unweighted inventory indicators and the separately licensed RELICS mapping."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from scipy.sparse import csr_matrix
import yaml


@dataclass(frozen=True)
class Indicator:
    """A linear elementary-flow inventory indicator, not a damage score."""

    key: str
    unit: str
    family: str
    description: str
    rules: tuple[tuple[str, tuple[str, ...], float], ...]
    flow_unit: str

    def factor(self, flow):
        name, categories, unit = flow
        if unit != self.flow_unit:
            return 0.0
        matches = [
            amount
            for pattern, compartment, amount in self.rules
            if re.match(pattern, name, flags=re.I)
            and categories[: len(compartment)] == compartment
        ]
        if matches and any(x != matches[0] for x in matches):
            raise ValueError(f"Conflicting factors for {self.key}: {flow}")
        return matches[0] if matches else 0.0

    def to_dict(self):
        return dict(
            key=self.key,
            unit=self.unit,
            family=self.family,
            description=self.description,
            flow_unit=self.flow_unit,
            rules=[
                dict(name_pattern=p, categories=list(c), factor=f)
                for p, c, f in self.rules
            ],
        )


def default_indicators():
    """Return physical carbon/water/land and all 65 RELICS 0.0.6 indicators.

    Cooling and turbine water are deliberately separate. Withdrawal/return
    and land occupation/transformation are never combined as one net quantity.
    RELICS categories can overlap and must not be summed as total metal mass.
    """
    definitions = [
        (
            "resources.crude_oil",
            "kilogram",
            "fossil_resources",
            r"^Oil, crude(?:,.*)?$",
            ("natural resource", "in ground"),
            "Crude-oil resource input",
        ),
        (
            "resources.natural_gas",
            "cubic meter",
            "fossil_resources",
            r"^Gas, natural(?:,.*)?$",
            ("natural resource", "in ground"),
            "Natural-gas resource input",
        ),
        (
            "resources.coal",
            "kilogram",
            "fossil_resources",
            r"^(?:Coal,|Lignite).*$",
            ("natural resource", "in ground"),
            "Coal/lignite resource input",
        ),
        (
            "resources.uranium",
            "kilogram",
            "energy_resources",
            r"^Uranium(?:,.*)?$",
            ("natural resource", "in ground"),
            "Uranium resource input",
        ),
        (
            "co2.fossil",
            "kilogram",
            "carbon",
            r"^Carbon dioxide, fossil$",
            ("air",),
            "Signed fossil CO2 to air",
        ),
        (
            "co2.non_fossil",
            "kilogram",
            "carbon",
            r"^Carbon dioxide, non-fossil$",
            ("air",),
            "Signed non-fossil CO2 to air",
        ),
        (
            "co2.stock",
            "kilogram",
            "carbon",
            r"^Carbon dioxide, (?:to|from) soil or biomass stock$",
            ("air",),
            "Aggregate signed soil/biomass-stock inventory flows; not net atmospheric CO2",
        ),
        (
            "co2.atmospheric_uptake",
            "kilogram",
            "carbon",
            r"^Carbon dioxide, in air$",
            ("natural resource",),
            "Atmospheric CO2 resource uptake",
        ),
        (
            "co2.geological",
            "kilogram",
            "carbon",
            r"^Carbon dioxide, in ground$",
            ("natural resource",),
            "Geological CO2 resource input",
        ),
        (
            "carbon.monoxide",
            "kilogram",
            "carbon",
            r"^Carbon monoxide(?:,.*)?$",
            ("air",),
            "Carbon monoxide to air; multiply by 12/28 for kg C",
        ),
        (
            "carbon.methane",
            "kilogram",
            "carbon",
            r"^Methane(?:,.*)?$",
            ("air",),
            "Methane to air; multiply by 12/16 for kg C",
        ),
        (
            "water.freshwater",
            "cubic meter",
            "water",
            r"^Water, (?:river|lake|well, in ground|unspecified natural origin)$",
            ("natural resource",),
            "Freshwater withdrawals; cooling/turbine flows reported separately",
        ),
        (
            "water.surface",
            "cubic meter",
            "water",
            r"^Water, (?:river|lake)$",
            ("natural resource",),
            "River and lake withdrawals",
        ),
        (
            "water.groundwater",
            "cubic meter",
            "water",
            r"^Water, well, in ground$",
            ("natural resource",),
            "Groundwater withdrawals",
        ),
        (
            "water.saltwater",
            "cubic meter",
            "water",
            r"^Water, (?:ocean|salt|sole).*$",
            ("natural resource",),
            "Saltwater withdrawals",
        ),
        (
            "water.cooling",
            "cubic meter",
            "water",
            r"^Water, cooling.*$",
            ("natural resource",),
            "Cooling water resource input",
        ),
        (
            "water.turbine",
            "cubic meter",
            "water",
            r"^Water, turbine.*$",
            ("natural resource",),
            "Turbine water resource input",
        ),
        (
            "water.to_air",
            "cubic meter",
            "water",
            r"^Water$",
            ("air",),
            "Water to air; not independently a freshwater-consumption balance",
        ),
        (
            "water.return",
            "cubic meter",
            "water",
            r"^Water$",
            ("water",),
            "Water returned to water compartments",
        ),
        (
            "land.occupation",
            "square meter-year",
            "land",
            r"^Occupation, (?!.*(?:seabed|lake|river|inland waterbody)).*$",
            ("natural resource",),
            "Land occupation excluding flooding and seabed",
        ),
        (
            "land.flooding",
            "square meter-year",
            "land",
            r"^Occupation, (?:lake|river|inland waterbody).*$",
            ("natural resource",),
            "Inland-water occupation",
        ),
        (
            "land.seabed",
            "square meter-year",
            "land",
            r"^Occupation, seabed.*$",
            ("natural resource",),
            "Seabed occupation",
        ),
        (
            "land.transformation_from",
            "square meter",
            "land",
            r"^Transformation, from .*$",
            ("natural resource",),
            "Transformation from original land type",
        ),
        (
            "land.transformation_to",
            "square meter",
            "land",
            r"^Transformation, to .*$",
            ("natural resource",),
            "Transformation to destination land type",
        ),
    ]
    result = [
        Indicator(k, u, family, description, ((pattern, cat, 1.0),), u)
        for k, u, family, pattern, cat, description in definitions
    ]
    mapping = Path(__file__).resolve().parents[1] / "data/utils/audit/relics.yaml"
    for row in yaml.safe_load(mapping.read_text(encoding="utf-8")):
        result.append(
            Indicator(
                "metals." + row["name"],
                "kilogram",
                "metals",
                f"RELICS 0.0.6 attributed {row['name']} extraction; not scarcity weighted",
                tuple(
                    (
                        "^" + re.escape(rule["name"]),
                        tuple(rule["categories"].split("::")),
                        float(rule["amount"]),
                    )
                    for rule in row["environmental flow"]
                ),
                "kilogram",
            )
        )
    return tuple(result)


def characterize(graph, indicators):
    from .graph import flow_identity

    rows, cols, values, coverage = [], [], [], []
    catalog = tuple({flow_identity(n) for n in graph.flow_catalog.values()})
    for j, indicator in enumerate(indicators):
        matched = []
        for i, flow in enumerate(graph.flows):
            value = indicator.factor(flow)
            if value:
                rows.append(j)
                cols.append(i)
                values.append(value)
                matched.append(i)
        coverage.append(
            dict(
                indicator=indicator.key,
                unit=indicator.unit,
                matched_inventory_flows=len(matched),
                exchange_rows=int(sum(graph.B.getrow(i).nnz for i in matched)),
                catalog_matches=(
                    sum(bool(indicator.factor(f)) for f in catalog) if catalog else None
                ),
                status="represented" if matched else "no_matching_inventory_flow",
                matched_flows=[
                    dict(
                        name=graph.flows[i][0],
                        categories=list(graph.flows[i][1]),
                        unit=graph.flows[i][2],
                    )
                    for i in matched
                ],
            )
        )
    CF = csr_matrix(
        (values, (rows, cols)), shape=(len(indicators), len(graph.flows)), dtype=float
    )
    return CF, (CF @ graph.B).tocsr(), coverage
