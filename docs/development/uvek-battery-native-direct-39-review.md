# Fourteen native supply correspondences — 16 November 2026

The actual required battery import uses fourteen compatible native supply or
processing functions whose correspondences had not been approved for source
ecoinvent 3.9 cut-off. These decisions reuse the existing direct rules. No
migration rule, helper inventory or production Python module changes.

The group covers hot rolling, tin metal, process-specific landfill operation,
dry hydrated lime, deionised water, lithium carbonate, alumina, AlMg3 alloy,
chromium metal, PTFE film on glass, gas-turbine fuel combustion, activated
bentonite, lubricating oil and unspecified organic solvents. Both source and
native references are positive one kg, except the combustion service per MJ.
Source definitions and both native releases were inspected; hashes and the
functional decisions are in Brightpath's packaged
`uvek_battery_native_direct_39_review.json`.

These are complete native inventory screening proxies. They preserve signed
caller quantities, formulas and supported uncertainty. They replace the source
supply's internal production, delivery, allocation and emissions model once;
they do not preserve source producer shares or imply equal impact scores.
Warnings explicitly cover those changes and incomplete native documentation.

Particular boundaries matter:

- Hot rolling excludes the separately purchased steel. Landfill operation
  excludes composition-dependent waste treatment and emissions.
- Native bentonite already has the same 38/61/1 activated-product mixture.
  No extra activation inventory is needed.
- Gas-turbine service references burned fuel, not electricity or useful heat.
  No efficiency or heating-value conversion is introduced.
- Native chromium is metal, not ferrochromium. AlMg3 does not replace AlLi.
- Native lithium carbonate replaces the source brine/spodumene/recovered mix
  with a historical Chilean brine model. Purity, grade, route and delivery
  differences remain unquantified; no guessed purity multiplier is added.
- Generic solvent is only an unspecified mixture. It cannot replace a named
  reagent such as tetraethyl orthosilicate.

Twelve Brightpath tests and four Premise harmonizer/cache/withdrawal tests pass.
Twenty-eight signed native-target inventory solves are finite without a new
Brightway database. The actual first source-3.9 battery cohort falls from 27
pending correspondences to 13; final energy falls from 29 to 21. Trucks' first
source-3.8 cohort remains at 85. These counts cover only the first encountered
cohort, not all future source versions or the entire remaining goal.

The much broader pre-existing approval test file was stopped after 96 passing
cases to avoid repeating unrelated correspondence checks. This is not a claim
that the full suite passed. Focused new-decision tests completed successfully.
See [verification](uvek-battery-native-direct-39-verification.json).
