# Retained workbook biosphere names

The steel prerequisite diagnostic passed its first four correspondence reviews,
then stopped because 141 biosphere exchanges used 13 legacy names. The existing
Premise correspondence YAML already supplies every required name. Each canonical
identity exists in the exact ecoinvent 3.9 source catalog and its publisher UUID
agrees with Premise's 3.9 biosphere table. Parent-only air/water compartments are
valid; no new subcompartment or oxidation-state guess is introduced.

The retained-workbook path needs to apply those existing correspondences before
strict source validation, as ordinary Premise imports already do. Extending the
YAML again cannot execute that missing preparation step. A small generic adapter
in `premise/inventory/clic_hybrid.py` therefore canonicalizes a copied exchange
only when its original identity is absent and the declared replacement exactly
matches the source catalog, unit and categories. Already valid names remain
unchanged, including when the bidirectional table also contains another spelling.
Unknown names, wrong units/compartments and unavailable catalogs remain subject
to the ordinary strict refusal. Application/custom catalog resolution is retained.

Only the biosphere name changes. All source quantities, signs, uncertainty,
formulae, categories, units, comments, links, production and other inventory
fields remain as supplied. Source profiles stay ecoinvent 3.9; they are not
relabelled as an older biosphere to avoid validation. The original workbook is
unchanged. Per-exchange information records retain old/new identities, source
profile, canonical flow UUID, catalog digest and correspondence digest. These
records participate in cache identity and remain in harmonization diagnostics.
Sector transformation algorithms and the ordinary ecoinvent import route are
unchanged. No new correspondence or model proxy is added by this adapter.

An independent observer around the actual encrypted REMIND SSP2-NPi 2050 steel
preparation verifies 141 renames across 379 biosphere exchanges. Across all 803
exchanges inspected at this normalization step, every other field is exactly
preserved; non-biosphere exchanges are unchanged. The resulting strict source
validation resolves all 379 biosphere and 395 technosphere links at full coverage.
This establishes source preparation, not a completed steel transformation/export.

Twenty focused tests pass, including all 13 legacy names and UUIDs, unchanged
physical fields, idempotence, exact catalog/unit/compartment guards, existing valid
names, unknown/missing-catalog handling, application-provider selection, copied
source/provenance/cache identity, and strict migration through the target
biosphere with a warm-cache repeat. Existing hybrid and user-inventory regressions are included.
Black, Ruff and documentation-reference checks pass. Separately, the previous
four-prerequisite approval batch's broader suite passed 834 compatibility/approval
and fuel-carbon tests; that suite does not replace the new adapter tests.

The next retained-workbook cohort has 52 unreviewed technosphere correspondences.
They include clear product/mode mismatches and functional reference/grade checks,
fuel/heat boundaries, residue allocation/treatment and other material/equipment
references. Refusals remain active, no physical exchange is omitted, and steel
support remains gated. See `uvek-steel-retained-technosphere-frontier.json` and
`uvek-retained-biosphere-name-verification.json`. Later cohorts and the full steel
update remain unfinished.
