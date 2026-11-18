# Canonical air compartments in the retained final-energy workbook

The actual source-3.10 workbook conversion stopped on 20 selected emissions
labelled “air / unspecified”. The exact ecoinvent 3.10 catalog represents that
same unspecified air compartment as “air”. The complete workbook contains
39 such labels across six flow names. Each corrected name/unit/category is
verified against the canonical catalog, using the existing name correspondence
for the refrigerant where needed. No receiving compartment is reclassified.

Only those 39 text cells change. All 8,029 numeric cells, 459 formulas, cell
styles, tabs, comments, uncertainty fields and unrelated workbook XML members
remain exact. The worksheet's existing XML retains the Artifact-authored text
changes without a broad workbook rewrite. The reviewed hybrid registry changes
only the workbook SHA256; no Python module or test changes.

All 39 existing hybrid/profile tests pass. The actual preparation retry clears
source validation and next reports 52 unapproved source-3.10 correspondences in
the retained workbook. Those are remaining mapping work, not permission to omit
inputs or disable validation. No Brightway database is written and the sector
remains unqualified.
