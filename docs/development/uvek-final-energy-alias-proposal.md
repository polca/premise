# Approved generic final-energy classifier fix

Final-energy preparation succeeds with 1,621 imported records. The unchanged
shared update then cannot recognize native operating energy in a diesel-electric
train. The native diesel label is already declared in the existing exact-label
YAML, but the two classifiers in `vehicle_energy_use.py` do not consult it.

The approved one-file patch calls the existing `canonical_vehicle_energy_product`
lookup from `carrier` and `fuel_property_key`. Supplier labels, quantities,
units, LHVs, carbon factors, transformation formulas and public signatures remain
unchanged. It introduces no UVEK sector engine or new conversion strategy.
A mapping-only change cannot wire an existing function into these classifiers;
adding physical supply helpers solely to disguise labels would be redundant.

The YAML gains exact medium-/high-voltage grids, delivered ethanol and E5 labels,
with their units. E5 retains the original nominal petrol accounting policy;
no blend shares or carbon split are recalculated. The lookup remains strict by
label and unit, and does not classify battery storage as purchased electricity.

The interpreter-only proposal passes 81 existing vehicle/energy tests, six native
classification cases, six wrong-unit refusals and three equipment/production
countercases. An operating-source census found 15/16 source pairs recognized
before the last E5 alias was added; the shared workflow preview is recorded
separately. These checks do not establish public sector support or an export.

The user approved this exact fix on 18 November 2026 as an explicit exception
to the no-Python instruction. The source now matches the reviewed proposal
exactly, and all 81 existing vehicle/energy tests pass against the applied fix.
No other Python changes are authorized by this approval. The separate four-file
battery proposal remains unchanged and unapplied. The original reviewed patch
is retained at `proposed-final-energy-alias-compatibility.patch`.


Latest full preview: the alias proposal and exact E5 mapping allow inventory
preparation (1,621 records) and the shared update to finish. It selects 170
nonempty roles containing 2,418 activity assignments. Exhaustive graph validation
then rejects one newly introduced technosphere cycle, including native
HFC-152a and 1-butanol suppliers. This is an additional operational blocker;
classification success does not establish a valid final-energy database.
The preview took about 152 seconds, left source files unchanged and performed
no export. See `uvek-final-energy-alias-workflow-preview-verification.json`.
