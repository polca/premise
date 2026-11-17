# Complete Na-NiCl battery mapping for the legacy workbook

The ordinary battery preparation still imports a legacy capacity workbook.
Its NaCl battery market previously pointed to Battery BoP, while the source
manufacturer pointed to a LiFePO4 battery. Neither is a complete Na-NiCl
high-temperature battery. Those two exact source-3.10 cut-off correspondences
now retain the inspected manufacturer and its delivered market using two
mapping recipes. No Python module or test is changed.

All thirteen manufacturing entries and five market entries remain. Seventeen
complete native processes supply aluminium oxide, control electronics, helium,
metal working, pig iron, HDPE, silicone, powdered NaCl, stainless steel,
low-alloyed steel, copper, nickel, electricity and freight. No deeper upstream
production is reconstructed. The original source represents a sealed battery
with cells and supporting parts together; no second BoP or cell burden is added.
The documented source density of 116 Wh/kg is evidence, not a new conversion
factor: both helpers output one kg. Existing Premise capacity/mass logic remains.

Native production mixes, grade, geography, delivery, vintage, allocation and
internal uncertainty replace source input models, with unquantified score
differences. Native 99.5% nickel is a purity proxy for source class-1 nickel,
without metal-content scaling. Swiss low-voltage electricity replaces the
inspected NEU supply at the Swiss producer. Every freight quantity remains;
European rail/road, Swiss light-commercial and transoceanic ship fleets are
explicit geographic/technology proxies. No stoichiometric, yield, payload,
distance, lifetime, efficiency or density multiplier is introduced. No source
position is omitted and no recovery credit is added. The source immediate
manufacturer has no separately listed biosphere exchanges. Documented BAFU
biosphere exclusions remain. Inventory/impact equivalence is not established.

Checks passed: nine Brightpath resource tests, 92 existing Premise battery
tests, sixteen signed uncertainty checks, fresh/warm public harmonization,
signed finite in-memory LCIs for both consumers and helpers with independent
flattened-native comparisons, and byte equality of all three generator outputs.
Temporary outputs were removed; no Brightway database was created. Two
historical standalone replay test failures remain separately disclosed.

The ordinary preparation diagnostic now imports all 951 records. The current
battery code then fails twenty validation checks because positive native
treatment purchases are counted as manufacture or expected to be negative
waste inputs. Mapping signs must remain physically correct. A separate
unapplied generic source-context and mass-accounting proposal addresses this
code incompatibility. Public battery admission is also still gated in Python.
Fifteen of nineteen sectors remain qualified; this mapping does not claim a
complete battery update/export.
