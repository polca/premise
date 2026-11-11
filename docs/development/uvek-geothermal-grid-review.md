# Deep-geothermal heat: electricity prerequisites

The retained `lci-geothermal.xlsx` workbook defines delivered useful heat, not an electric heat pump. Its original delivery efficiency is already part of the inventory; do not apply another 0.8 multiplier. Its 48 selected records use ecoinvent 3.6 cut-off dependencies.

Reviewed 24 high- and medium-voltage electricity correspondences against actual ecoinvent 3.6 and native UVEK 2025/2026 references. Each has a positive 1 kWh reference and the same declared voltage and geography. Use the complete native grid as a screening proxy, preserving signed caller quantities, uncertainty and formulas. Mixes, losses, infrastructure, allocation, vintage and aggregate territory boundaries can differ; score equivalence is not established. Do not add another loss factor.

Brightpath commit `24a72d3e35438b02095711c6135a5cf6fe4cace8` packages the reference evidence. One rule needed an exact 3.6 positive-reference sign profile; its selected 3.9 behaviour is unchanged. Of 714 previous Premise approval fingerprints, 713 are unchanged and one is rebound to the expanded rule without changing its selected scientific behaviour. The new total is 738 exact scopes. No consequential route is approved.

The Brightpath change also removes three already-unreferenced historical review records, preserves their history in Git, and updates stale resource-storage assertions. A maintainer replay script and tests preserve the new exact profile during regeneration. Neither library runtime Python modules nor Premise sector algorithms change.

Verification: 173 focused Brightpath tests, built-wheel evidence identity, and 48 signed coordinate conversions. Preparation accepts 28 correspondences (24 new and four previously approved), but still refuses 11 non-electricity correspondences. These include cement, chemicals, drilling waste, inert waste, diesel service and wastewater. In particular, the current ordinary Portland cement to slag-cement heuristic must be corrected rather than approved.

This is a prerequisite milestone, not qualification of geothermal heat or the entire heat sector. No new heat supplier alias or public sector gate is added. See [verification receipt](uvek-geothermal-grid-verification.json) for the exact remaining identities.
