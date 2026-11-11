# Additional native burner carbon-origin mappings

This change extends only `premise/iam_variables_mapping/fuels.yaml`. It uses
the existing shared recorded-exhaust update; no Python module, migration
resource, source database or user-facing API is changed.

## Reviewed services

Twenty-five native burner records, covering 24 distinct names, directly buy
delivered fuels with final IAM mixture properties:

| Fuel | Records | Scope |
| --- | ---: | --- |
| Diesel | 20 | Machinery, agricultural combustion and CHP |
| Petrol | 1 | Building machinery |
| Natural gas | 4 | Older native CHP combustion services |

Source records were inspected for reference product, unit, positive unit
production, actual fuel supplier and complete auxiliary inventory. Selectors
are exact and bounded by reviewed names, reference products and units. Both
native MJ and kg references remain unchanged. Material production, reforming,
source-gas production and delivery losses are outside these new selectors.

## Decision and warning

Use the already-reviewed fuel-dominant whole-exhaust convention: retain each
record's positive CO2 total and refresh only its fossil/non-fossil origin from
the actual linked fuel properties. Do not infer a new heating value or
efficiency, rescale fuel demand, alter allocation or add an avoided-emission
credit. Retain all other emissions, signed inputs, uncertainty, infrastructure,
lubricants, urea and waste treatment.

**Screening warning:** some direct fossil carbon from lubricants or urea may
also be reclassified. Its contribution is not separately established and the
score effect is unquantified. No guessed correction is introduced. Native
technology, capacity, reference-energy, geography, vintage and allocation
limitations remain. This follows the standing approval for similar cases;
the warning is recorded here, in the YAML declarations and verification receipt.

This does not resolve methane or carbon-monoxide origins, gas-production loss
flows, mixed industrial process carbon or every remaining heat/fuel role.
It does not establish LCIA equivalence.

## Verification

The REMIND SSP2-NPi 2050 five-sector diagnostic exported 14,808 activities to
`BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-194104`.
Compared with the preceding vehicle-blend export, every reviewed burner retains
its non-CO2 inventory and recorded positive CO2 total. All 25 final origin
ratios match the linked fuel properties and all 25 LCIs are finite. All
141,426 technosphere links are internal and all 311,605 biosphere links use
the requested ecoinvent 3.10 database. Eleven validation phases report no errors
and 14 warnings.

The focused suite passes 144 tests; YAML parsing, whitespace checks and generated
documentation references pass. A structural comparison confirms that every
ecoinvent alias, fuel property and mapping field outside
`uvek_recorded_exhaust_consumers` is unchanged. No new Python lint finding can
be introduced by this YAML-only change. A separate real ecoinvent run was not
repeated for this extension.

The local diagnostic retains the six pre-existing unrelated edits. This is
not a new clean-checkout verification; the preceding vehicle-blend milestone
separately verified the shared code from a clean staged checkout. Aggregate
evidence is in `uvek-additional-burner-verification.json`; raw inventory
inspection remains private.

The expanded route audit leaves 298 unreviewed native process candidates and
64 delivery carriers. These are review candidates, not confirmed errors or
permission to reclassify their emissions wholesale. Public fuels support stays
disabled, and all remaining sector work stays in the active goal.
