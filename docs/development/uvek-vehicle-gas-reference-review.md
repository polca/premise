# Native vehicle-gas reference review

Status: inspected, not yet implemented or qualified for public fuels updates.

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

The existing shared CNG regionalization selects ecoinvent's vehicle-grade
product and production/market names exactly. It does not select the native
BAFU station or blend. Adding a BAFU station to the fossil-gas ingredient map
would incorrectly turn a delivered kilogram product into a gas-market
ingredient whose canonical reference is cubic metres.

The existing vehicle-blend handover calibrates against the canonical fuel's
unit-based energy. Natural gas currently has a cubic-metre basis, whereas the
native station supplies kilograms. A YAML alias alone cannot establish the
missing delivered heating value. Any shared-code adaptation needs focused
regressions for the unchanged ecoinvent CNG path and must expose the adopted
reference assumption in provenance.

## Next verification

Use exact source-family declarations with the existing regionalization and
handover machinery. Verify source quantities and uncertainty, station losses,
generic versus dedicated-biomethane routing, final exhaust carbon origins,
absence of supply loops, successful export and finite LCIs. Keep the public
fuels gate unchanged until this and the other documented fuels gaps pass.

The full exchange-level inspection remains private; this document contains
identity-level findings and aggregate counts only. No source inventory vector
is redistributed.
