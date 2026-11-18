# Reuse reviewed WEEE and dry-paint models for final energy

Two exact source-3.12 cut-off correspondences use existing reviewed models.
No upstream manufacturing reconstruction, new carrier-removal decision,
foreground omission or Python module/test change is introduced.

WEEE collection uses its already generated source-3.12 road/rail-plus-shredding
recipe. The inspected source has a negative one-kg waste reference, one negative
shredding input and two positive freight entries consistent with that model
within local floating-point storage precision. Factor -1 converts negative
waste demand to positive native service demand. Complete Swiss shredding and
both freight modes remain; no recovery credit or burden-free handover is added.
Global mix, fleet and native treatment/allocation differences remain explicit.

The European alkyd-paint market delegates to the already reviewed source-3.9
dry white solvent-based paint model through one explicit inventory-profile
alias. The source product is named without solvent in 60% solution state; its
immediate producer has the same kg reference and principal resin/pigment basis.
The previous copied-native carrier removals, processing solvent, nitrogen-purge
difference, full inventory, model freight and uncertainty remain as reviewed.
No extra concentration factor is applied. Source-3.12 internal manufacture,
delivery, uncertainty and vintage are replaced as a screening proxy, not claimed
preserved. Existing model limits and unquantified score effects remain visible.

Checks: 16 signed uncertainty checks, fresh/warm public harmonization, signed
finite inventories, two independent flattened-native comparisons, the new alias
inventory, unchanged earlier source-3.9 selection, nine review/manifest and ten
Premise approval tests. All three normal generator outputs match packaged bytes;
113 MB of temporary generator output was removed. No database was written.
Preparation now clears source 3.12 and reaches one source-3.10 storage-tank scope.
Final-energy transformation, coverage and export remain unqualified.
