# Native vehicle-gas reference review

Status: native station routing and generic vehicle-blend replacement implemented
and verified in diagnostic builds. Not qualified for public fuels updates;
other fuel and sector coverage remains unfinished.

## Evidence

The inspection used project `bafu-2026-ecospold2-biosphere-310` and exported
database
`BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-181632`.
The source station references were also checked in the original BAFU 2026
ecoSpold archive. Both stations provide one kilogram; the inspected source
records do not declare a delivered lower heating value.

The consumer audit resolves exchange input keys to their actual suppliers,
rather than matching exchange labels alone. It found:

| Route | Consumer records | Distinct consumer names |
| --- | ---: | ---: |
| Historical `Fuel supply for gas vehicles` | 86 | 58 |
| Vehicles explicitly labelled `100% biomethane` | 86 | 58 |

There are 177 distinct consuming records across the two station products and
the historical blend, including supply-chain records. The table counts
activity records, not distinct technologies. There are no direct transport
consumers of the natural-gas station outside the generic blend in this export.

The Swiss generic blend contains historical biomethane. The European blend's
input masses do not sum to exactly one kilogram. Neither feature should be
silently normalized away or added on top of a future IAM fuel mix.

## Required boundaries

1. Generic vehicle gas must receive the future IAM natural-gas blend through
   the shared fuel transformation machinery. Dedicated biomethane vehicles
   must retain their fuel identity; a name containing “compressed gas” is
   insufficient to select a generic-blend consumer.
2. Retain native station electricity, infrastructure and loss-related
   exchanges. The station's incoming gas energy is not evidence of delivered
   heating value. A demand-based calibration must be explicitly described as
   such, rather than labelled a measured fuel-energy conversion.
3. Keep source vehicle kilogram demands and uncertainty. Any reference-basis
   conversion belongs at the declared supply interface.
4. Do not relink every high-pressure gas producer to the future mix: regional
   fossil inventories remain ingredients of that mix. Doing so would create
   recursion or replace a fossil ingredient with the blend itself.
5. Check mixed petrol/biomethane vehicle inputs before revising exhaust
   carbon. An explicit biomethane label does not prove that all fossil CO2
   is erroneous or that a vehicle has no other fuel inputs.

## Reproduced mapping limitations

Before this adaptation, shared CNG regionalization selected ecoinvent's vehicle-grade
product and production/market names exactly. It did not select the native
BAFU station or blend. Adding a BAFU station to the fossil-gas ingredient map
would incorrectly turn a delivered kilogram product into a gas-market
ingredient whose canonical reference is cubic metres.

The existing vehicle-blend handover calibrates against the canonical fuel's
unit-based energy. Natural gas currently has a cubic-metre basis, whereas the
native station supplies kilograms. A YAML alias alone cannot establish the
missing delivered heating value. Any shared-code adaptation needs focused
regressions for the unchanged ecoinvent CNG path and must expose the adopted
reference assumption in provenance.

## Verified station routing

`uvek_vehicle_gas_suppliers` now declares the exact native station identity and
its upstream supply conversion. The existing shared CNG regionalization reads
these declarations alongside its unchanged canonical ecoinvent identities.
This preserves converted helpers that retain those identities in a UVEK build.
There is no additional transformation engine or constructor argument.

The existing regional-copy callback replaces only the declared gas purchase,
using the already-reviewed 36 MJ/m3 supply basis. Source quantities must be
positive and finite, and the destination regional market must exist. Geographic
relinking can split a purchase across several suppliers; each resulting input
is converted with its uncertainty, rather than assuming exactly one input or
collapsing their distributions. Original station records forward one kg to
their normal IAM region. Repeated updates do not duplicate regional stations.

This small Python adaptation is necessary because the former exact-name loop
could not be extended through YAML. The physical identities, conversion and
limitations remain in the mapping file. Station provenance stores the selected
declaration and its limitations.

The real five-sector diagnostic exported 14,808 activities to
`BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-192239`.
All 13 regional station copies have finite LCIs. Compared with the preceding
export, their gas-energy demand, station electricity, non-CO2 biosphere
exchanges and total CO2 are retained. The normal shared gas update reclassifies
the fossil/non-fossil CO2 split; it does not add an oxidation emission to the
station. Methane and other non-CO2 loss-flow origins remain unchanged, a
remaining screening limitation.

Validation: 127 focused fuel tests pass; changed Python files pass Black and
Ruff. Generated documentation references are current. The real ecoinvent 3.12
five-sector regression also completes with 31,205 activities and no validation
errors. It is an update regression, not an additional Brightway export.

## Generic blend replacement and reference decision

The follow-up consumer audit found no additional petrol or diesel inputs in
either the 86 generic gas vehicles or the 86 dedicated biomethane vehicles.
The exact 58 generic vehicle names are now declared as recorded-exhaust
consumers. Dedicated biomethane identities are excluded.

The adopted screening proxy gives both historical station-gas ingredients and
the destination the same nominal Premise CNG reference, 46.5 MJ/kg. Consequently
the handover preserves the sum of historical station-gas masses. It does not
normalize those masses to one, preserve historical upstream gas-energy demand,
or claim a measured BAFU delivered heating value. The nominal reference cancels
from that mass-based handover and from the vehicle exhaust-origin ratio.

Replacing the generic historical blend with the complete regional station
removes its fixed historical biomethane contribution. The IAM mix now determines
gas origin. This also replaces the old mixture of two compression services with
the complete native natural-gas station: compression, composition, pressure,
upstream gas demand and loss-model differences are explicit screening
limitations, with unquantified score effects. The separate dedicated biomethane
supplier and its vehicle inventories are retained.

Vehicle fuel demands and uncertainty remain on their original kg basis.
Recorded positive CO2 totals are retained; only their fossil/non-fossil split
changes. The gas-dominant whole-exhaust convention can also reclassify minor
other fossil carbon. No measured heating value, lubricant correction, extra
generator, avoided emission or recovery credit is inferred.

This decision uses the standing approval for analogous screening proxies with
warnings. The common reference, changed compression boundary and whole-exhaust
limitations are declared in YAML, retained in handover/carrier provenance and
reported by the carrier warning.

## Why the small shared-code changes are needed

The normal handover runs while fuel markets are created. A complete CNG station
only has final properties after regionalization and carrier annotation, so the
shared update now permits a declared handover at that later point. It still
uses the existing handover and forwarding routine. Missing destination
properties fail explicitly.

The handover accepts an explicit reference energy instead of applying the
natural-gas cubic-metre reference implicitly to a kilogram station. The carrier
reader accepts an explicit list of gas-input identities, covering both regional
station recipes and original kg forwarders. Finally, native handovers retain
their carrier's canonical fuel meaning without renaming source products; the
carbon reader uses that meaning. No alternative UVEK transformation engine or
new user-facing argument is introduced.

The diagnostic export
`BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-193124`
contains 14,808 activities. Both historical blend records preserve their total
station-gas mass. All 86 generic gas vehicles retain their recorded CO2 totals
and other exchanges, have the final linked gas-origin split, and give finite
LCIs. All 86 dedicated biomethane vehicle inventories are unchanged. Every
technosphere link is internal and all biosphere links use the requested
ecoinvent 3.10 database. Eleven validation phases report no errors and 14
warnings. The focused fuel suite passes 133 tests. The real ecoinvent 3.12
five-sector update regression completes with 31,205 activities and no errors.

An isolated staged checkout, excluding the four unrelated local GAINS edits,
also exported 14,808 activities to
`BAFU:2026-remind-SSP2-NPi-2050-five-sector-clean-vehicle-blend-review-20261111-193404`.
The same 86 vehicle LCIs and inventory/origin checks pass, with no validation
errors. Runtime module/YAML hashes match the staged change. Evidence is in
`uvek-vehicle-blend-verification.json`. Private export artifacts and receipts
were retained; the temporary checkout was removed (125,124,878 bytes).

Black and the generated-reference check pass. Ruff introduces no new findings;
the six unused-import findings in `markets.py` are also present in the preceding
commit and are not changed by this work. Other reviewed files pass Ruff.

## Remaining work

Keep the public fuels gate unchanged. Network/process loss-flow origins,
mixed industrial fuel-carbon coverage and the other documented fuel roles
still need review. Other registered sectors and the remaining electricity and
heat coverage also remain part of the active integration goal. Passing this
vehicle-gas check does not certify full fuel or sector coverage or LCIA
equivalence.

The full exchange-level inspection remains private; this document contains
identity-level findings and aggregate counts only. No source inventory vector
is redistributed.
