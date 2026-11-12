# Steel dependencies: regional gas and heat portfolios

## Regional high-pressure gas

The exact ecoinvent 3.9 cut-off market group for high-pressure natural gas in
Europe without Switzerland has a +1 m³ reference, twenty national market
inputs and no direct biosphere exchanges. It declares no calorific property.
Use the existing regional complete native RER consumer-supply recipe at nominal
36 MJ/m³. This is the reviewed Premise screening reference, not measured or
source-verified energy content, density or standard-state equivalence.

The recipe already exists for a separately inspected 3.12 correspondence; it
is reused without adding a redundant helper recipe. The source country weights
are replaced by native RER supply. Trade routes, composition, compression,
network losses/leakage, geography, allocation, vintage, internal uncertainty
and native biosphere exclusions can differ; score effects are unquantified.
Do not add end-use combustion, another pipeline, extra loss/pressure/density
correction or alter separate caller stack emissions. Preserve signed m³ demand,
uncertainty and formulas.

Only the exact inspected 3.9 cut-off profile is added. Complete selected 3.6 and
3.12 resources are identical before/after. Their approval fingerprints are
refreshed with explicit retention receipts because the complete shared rule
now includes another profile. No reverse or cross-system-model approval.

Frozen review `export/uvek_steel_39_regional_gas_review.json`, SHA256
`f496fa6f29308681a12f8de6ec9609d9c72f3304a8e761a4ea0bbdc29264c07c`.
Brightpath commit `df87bba15dfc911722c499d9268472eb0e9a283f` is pushed.
Ten Brightpath checks pass (19.71 seconds): signed demand, normal/lognormal
uncertainty, formulas, input immutability, exact scope, helper quantity,
historical selections and atomic rule/recipe drift refusal. The strict Premise
fresh/cache/approval-withdrawal test passes (17.85 seconds). Black and Ruff pass.
The complete 36-MJ service passes two finite signed composite LCIs with exact
sign reversal and unchanged source database; see
`uvek-steel-regional-gas-composite-lci-verification.json`.

Actual encrypted REMIND SSP2-NPi 2050 diagnostic steel preparation now has
**17 pending correspondences, down from 18**. Strict migration validation stayed
enabled; only the still-closed steel support guard was bypassed. No steel
transformation/export is claimed. Private audit receipt:
`75c2a651555deabfb2264d85e81ad68ffb166e087441d07fe8220f23e72604f2.audit.json.gz`.
Runtime Python modules remain unchanged; Python edits are maintainer replay,
generator hook and tests.

## Three still-unapproved heat dependencies

These are dependencies in imported steel inventories, not a failure of the
previously verified public heat-sector update. Inspecting the actual ecoinvent
3.9 market graphs gives:

| Source portfolio | Market nodes | Producer references | Largest technology share |
| --- | ---: | ---: | --- |
| Global natural-gas district/industrial heat | 6 | 104 | 57.79% conventional gas cogeneration |
| Global district/industrial heat other than gas | 6 | 327 | 69.34% hard-coal industrial furnace |
| RoW central/small-scale heat other than gas | 1 | 52 | 71.49% anthracite stove |

All inspected heat references are +1 MJ. Each expanded portfolio totals one MJ
within numerical precision; no cycles, direct market biosphere exchanges or
other market inputs were found. See `uvek-steel-heat-portfolio-preflight.json`.
This is primary evidence, not approval of the old suggested supplier mappings.

The existing native suggestion is complete delivered district heat from a gas
furnace. Its actual vector buys 1.11 MJ furnace heat plus 1 MJ district transport
per MJ reference. This adds a delivery/network boundary absent from the traced
market layers and replaces technology/allocation portfolios, including non-gas
fuels. Retain the literal vector as evidence; do not infer a different loss
formula from the prose. Review production-side suppliers and each fuel/CHP
allocation boundary before constructing replacement mixes. Do not approve a
single gas furnace as if it preserved those portfolios.

Licensed inventories and detailed traversal witnesses remain private. Published
preflight data contain aggregate technology weights and reference facts only.
