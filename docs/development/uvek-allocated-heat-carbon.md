# Allocated native heat and electricity: fuel-carbon origin

This YAML-only extension makes 51 additional native operating inventories use
the final delivered fuel's carbon origin through the existing shared recorded-
exhaust update. It changes no Python module, migration resource, allocation
algorithm or user-facing argument.

## Scope

The inspected records comprise 48 natural-gas and three diesel inventories:
30 provide one MJ of heat and 21 provide one kWh of electricity. They cover
allocated CHP, gas fuel cells and gas-driven heat pumps. Source products,
positive unit references, actual suppliers and complete auxiliary inventories
were inspected before adding exact names, reference products and units to
`uvek_recorded_exhaust_consumers` in `fuels.yaml`.

Keep each source's energy, exergy, price or single-output allocation as written.
Retain fuel demand, equipment, maintenance, auxiliary electricity, water, oil,
urea, waste treatment, signed exchanges, uncertainty and non-CO2 emissions.
Keep recorded positive CO2 totals and refresh only their fossil/non-fossil
split from the final linked fuel. No new efficiency, heating value, fuel
quantity, reallocation, avoided-emission credit or combined CHP dataset is
introduced.

**Screening warning:** the fuel-dominant whole-exhaust convention can also
reclassify minor fossil carbon from lubricants or urea. Those contributions
are not separately established; score effects are unquantified. No guessed
correction is applied. Technology, capacity, source composition, geography,
vintage and allocation limitations remain. The standing approval for similar
cases applies; warnings are recorded in the declarations and verification
receipt for later review.

Four ignition-biogas operating records are outside these selectors. Their
separate biogas and ignition-fuel contributions still need a mixed-fuel review.
Geological and industrial process CO2 must not be reclassified wholesale.

## Verification

The REMIND SSP2-NPi 2050 five-sector diagnostic exported 14,808 activities to
`BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-194820`.
All 51 operating inventories retain their other exchanges and original positive
CO2 totals, have the expected final fuel-origin split and give finite LCIs.
The four excluded ignition-biogas inventories and 14 geothermal records with
geological CO2 retain their complete exchange inventories.

No additional same-name regional copies were present in this build. The
inspection also checked new heat/electricity records for direct native fuel
links; geothermal records buy separate burner services and keep their own
process CO2. This is not proof of every future clone or technology combination.

All 141,426 technosphere links are internal and all 311,656 biosphere links
use the requested ecoinvent 3.10 database. Eleven validation phases report no
errors and 14 warnings. The focused suite passes 144 tests. YAML parsing,
whitespace and generated-reference checks pass. Structural comparison shows
that ecoinvent and all other mapping fields are unchanged. A separate real
ecoinvent regression was not repeated for this mapping-only extension.

This local diagnostic retains the six unrelated pre-existing edits; it is
not a new clean-checkout verification. The unchanged shared code previously
passed the isolated vehicle-blend build. Aggregate evidence is stored in
`uvek-allocated-heat-carbon-verification.json`; raw inspection stays private.

## Remaining work

The native route audit leaves 247 unreviewed process candidates and 64 delivery
carriers. These counts describe a review queue, not confirmed errors. Mixed
industrial carbon, ignition-biogas units and loss-flow origins remain pending.
This does not add IAM heat-role bindings: the eleven unqualified roles and
other documented heat limitations remain. Public fuels scope and all remaining
registered sector requirements stay unchanged.
