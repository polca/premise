# Gas properties through native low-pressure delivery

The 64 native `Natural gas, low pressure, at consumer` records all have a positive 1 MJ reference. Each purchases high-pressure gas and pipeline infrastructure; 31 also purchase a separate turbine operating service. Their network already connects to the updated IAM gas mix. The missing element was outgoing fuel-property metadata, so the existing carbon reader could not identify that composition in downstream combustion services.

YAML now declares the exact carrier, its incoming gas, permitted pipeline/turbine auxiliaries and outgoing reference energy. The existing gas-property annotation routine accepts these optional declarations. The output carbon properties are calculated independently for fossil, non-fossil and unresolved oxidation carbon:

`outgoing carbon = outgoing reference MJ × sum(input quantity × incoming carbon per unit) / sum(input quantity × incoming MJ per unit)`

This represents the delivered reference energy, rather than passing the network's extra gas draw to the end consumer as additional fuel carbon. The original gas draw, infrastructure, turbine service, emissions, signed quantities and uncertainty remain in the inventory. Auxiliary operating services are excluded from the composition of the physical gas output. No density conversion, measured-loss claim, extra multiplier, supplier replacement or new dataset is introduced.

The source-family callback activates declarations only when their required IAM gas array has positive data. Default CNG annotation and its existing reference basis remain unchanged. Unit mismatches, unknown inputs, missing upstream properties, negative gas supplies and cycles produce explicit errors; unresolved carbon origins remain unresolved. Repeated annotations refresh properties from the actual suppliers. The carbon reader recognizes the declared canonical market label without renaming or redirecting the native node.

Mappings alone could not propagate properties through these native MJ delivery records: the old routine recognized only canonical kg CNG and m3 gas inputs. The three existing Python modules receive a generic extension for declaration loading, gas-carrier annotation and reading the resulting label. No UVEK sector engine or NewDatabase argument is added.

## Downstream scope and decisions

Seventeen exact low-pressure combustion-service names now use the existing recorded-exhaust-origin refinement. Their positive exhaust totals remain unchanged; the fossil/non-fossil split follows the actual linked gas properties. This brings the checked delivered-gas services to 137. Fourteen production-specific raw-gas records retain their original inventories and emissions.

Four added CHP services consume lubricating oil without separately identified direct lubricant CO2. Their gas-dominant whole-exhaust split remains a screening proxy: some fossil lubricant CO2 may also be reclassified. Its score effect is unquantified. The standing approval for similar cases with warnings applies. Oil and waste inputs are retained, with no invented correction or credit. This limitation is stored in carrier metadata and emitted once per activated declaration, not once per country.

The annotation does not update low-pressure network/process emissions or resolve mixed industrial process carbon. Native vehicle-gas kg reference/blend handling also remains unfinished. These limitations are separate from the verified carrier-property transfer.

## Evidence

The real REMIND SSP2-NPi 2050 five-sector export contains 14,795 activities. All 607 exchange rows across the 64 delivery records match the previous scenario on the normalized comparison basis. Outgoing carbon properties agree with incoming carbon per MJ. All 137 selected positive exhaust balances are verified; exactly 17 change in this step. The 14 raw-production gas records remain unchanged. Other exchange fields, fuel demand, uncertainty, signed credits and emissions are unchanged outside the selected positive CO2 rows. Exported database/code tuples and exchange ordering are excluded from comparisons.

All internal technosphere links resolve and 17 representative downstream service LCIs are finite. Eleven pre-export validation phases contain no errors; the 14 existing electricity warnings remain. The carrier declaration also emits its explicit modelling warning. The companion JSON receipt records exact names and scopes.

262 focused fuel, energy, carbon, source-axis and carrier regressions pass. Tests cover the outgoing energy basis, retained upstream draw, unchanged network recipe, repeated updates, unknown origins, invalid inputs, cycles, absent/zero IAM gas and one warning per declaration. Black, Ruff and the generated-documentation reference check pass for the changed files.

A separate real ecoinvent 3.12 cut-off biomass/electricity/heat/cement/fuels update completes with 31,205 activities and 11 validation phases, with no errors and 85 existing warnings. That regression does not create a Brightway export. Public UVEK fuels support remains gated; this milestone does not qualify all remaining heat roles, other electricity technologies or other sectors.

A clean staged checkout independently exports `BAFU:2026-remind-SSP2-NPi-2050-five-sector-clean-low-pressure-review-20261111-184429` and passes the same 64-carrier/137-exhaust audit and 17 finite LCIs. It uses the committed GAINS resources, excluding the pre-existing uncommitted edits in the main checkout. All 311,483 exported biosphere rows resolve in the requested ecoinvent 3.10 database. Runtime module/YAML hashes match the staged feature. The temporary checkout (86,894,371 bytes) was removed after verification; the exported database and audit receipt are retained.
