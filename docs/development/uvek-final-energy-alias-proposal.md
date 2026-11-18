# Proposed generic final-energy classifier fix — not applied

Final-energy preparation succeeds with 1,621 imported records. The unchanged
shared update then cannot recognize native operating energy in a diesel-electric
train. The native diesel label is already declared in the existing exact-label
YAML, but the two classifiers in `vehicle_energy_use.py` do not consult it.

The proposed one-file patch calls the existing `canonical_vehicle_energy_product`
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

The patch remains unapplied because the latest instruction prohibits Python
source edits. Approval is needed for this concrete exception. The separate
four-file battery proposal remains unchanged and unapplied. See
`proposed-final-energy-alias-compatibility.patch` and its verification JSON.
