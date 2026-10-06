# Fresh legacy versus Brightpath SimaPro comparison

Both CSVs were regenerated on 2026-09-25 from the same prepared REMIND
SSP1-PkBudg1000 2050 snapshot: ecoinvent 3.12 cut-off, 42,770 processes and
1,386,053 exchanges. The snapshot includes the 21 reviewed municipal-waste CPC
corrections. Current legacy mappings and current Brightpath behavior were used,
including the reviewed waste-reference corrections, full supplier names, ISIC
folders, Geography and Premise provenance.

Premise revision: `34a734dc59f40932db1abb60040737df9f42bdb0`.
Brightpath revision: `5c9a0d8b62e6ea7e0395da4d77e107efce7be350`.
The Brightpath worktree has unrelated packaging/documentation/test edits; its
export implementation has no uncommitted changes.

## Main findings

| Check | Legacy | Brightpath |
| --- | ---: | ---: |
| Processes and reference outputs | 42,770 | 42,770 |
| Duplicate supplier labels | 0 | 0 |
| Unmatched technosphere rows | 0 | 0 |
| Ambiguous technosphere rows | 0 | 0 |
| Waste-treatment processes | 5,245 | 5,000 |
| Technosphere rows | 491,505 | 490,895 |
| Biosphere rows | 847,427 | 832,759 |
| Nonzero biosphere exchanges excluded by blacklist | 0 | 5,881 |
| Dedicated process identifiers | 0 | 42,770 |
| Input parameter rows | 0 | 782 |
| Exchange rows with defined uncertainty | 0 | 14,962 |

These checks parse the actual CSVs. Every exported process was also matched to
its source identity before comparison. All supplier labels resolve uniquely
within their respective CSV.

The 610 fewer technosphere rows are zero-amount exchanges. The 14,668 fewer
biosphere rows comprise 8,787 zero-amount exchanges and 5,881 nonzero blacklisted
exchanges. These counts reconcile exactly against the prepared input. The
blacklisted rows produce 5,881 warnings. They include Oxygen, radionuclides,
traffic-area occupation, turbine water and reservoir occupation; their treatment
remains a deferred review item, not an accepted claim of equivalence.

Both routes omit 4,351 inventory-indicator exchanges: the legacy writer reports
unsupported categories, and the comparison adapter explicitly excludes them
before Brightpath rendering.

## Amounts and classifications

The comparison aggregates exchanges by emitted flow identity within each process,
resolves technosphere labels to source supplier identities, and uses a 0.051%
relative tolerance plus 1e-15 absolute tolerance for legacy rounding.

- **1,075,340 grouped amounts differ only within the rounding tolerance.**
- **515 grouped amounts differ beyond that tolerance.** All were attributed to
  the source input and current mappings:
  - 240 technosphere groups are sign reflections caused by differing supplier
    waste classifications. All suppliers have an explicit resolved classification
    disagreement; 212 groups concern the electric-arc-furnace market. These are
    consequences of the previously reviewed classification choices.
  - 182 biosphere groups concern the differing Formate/Formic acid/thallium-salt
    correspondence: 91 under each emitted label.
  - 89 groups concern Brightpath mapping annual-crop transformation and
    unspecified arable-land transformation to the same emitted label.
  - 4 groups concern legacy mapping unspecified seabed occupation to the
    infrastructure label, while Brightpath retains separate labels.
- The 275 biosphere totals were reconstructed from source exchanges under each
  mapping and checked against both CSVs. This explains the differences; it does
  not establish which correspondence is correct.
- **259 processes differ in product versus waste-treatment classification:**
  252 legacy waste processes become products and 7 products become waste treatment.
- **21 reference quantities differ:** Brightpath preserves the positive non-unit
  source quantity, whereas legacy writes 1. All 21 Brightpath quantities were
  checked against the prepared source.

Thus the combined production-section/amount count is 280. The historical count
of 311 used an older legacy baseline and earlier reference handling; it should
not be interpreted as 311 remaining defects.

There are also **13,033 differing `Category type` fields**. Of these, 12,774 are
changes among non-waste types, separate from the 259 waste-status disagreements.
Classification inference usually supplies `material` for ordinary products,
so many legacy `energy`, `processing`, `transport` and `use` values become
`material`. The ISIC folder hierarchy is a separate field and does not preserve
those category types automatically.

## Other serialized differences

- 47,836 grouped exchange-section differences, including the separate
  Electricity/heat section (48,011 rows in Brightpath).
- 14,230 matched groups differ in uncertainty fields; Brightpath preserves source
  uncertainty that legacy omits.
- 197 matched groups differ in row multiplicity.
- 185,572 Brightpath-only and 201,401 legacy-only emitted flow labels within
  processes. These counts include renamed flows and exclusions; they are not
  counts of missing source exchanges.
- Geography matches source location for all processes in both files. Brightpath's
  Generator, System description and External documents were checked for all
  processes and contain the intended Premise/scenario provenance.
- All Brightpath process identifiers are nonempty and unique. No product folder
  is blank. Brightpath uses the shared ISIC hierarchy.
- 84 process display names differ; the inspected examples are Unicode
  transliteration differences, such as an en dash becoming `-` in Brightpath
  versus `?` in legacy.

Difference categories can overlap and must not be summed as an error count.

## Scope and reproduction

This is still the documented adapted comparison route, not a native-default
Brightpath export or a replacement of `NewDatabase.write_db_to_simapro()`.
The adapter adds missing unit labels, fills 913 empty comments, maps fossil-well
resource compartments to blank, normalizes parameter representation, and uses
explicit legacy categories for 1,961 unresolved waste classifications. Brightpath's
native biosphere mappings and blacklist remain active. No database build,
background migration or source-inventory modification was performed.

No native SimaPro application import or LCIA comparison was performed. Supplier
link integrity passes, but the deferred biosphere correspondence/exclusion review
and category-type choices still matter before claiming equivalent results.

Local artifacts are in `export/simapro-comparison/latest-20260925/`:

- `legacy/simapro_export_remind_SSP1-PkBudg1000_2050.csv`
- `brightpath-compatibility/simapro_export_remind_SSP1-PkBudg1000_2050.csv`
- `comparison-summary.json`, `csv-differences.csv`, `amount-differences.csv`
- `csv-verification.json`, `metadata-verification.json`, `difference-attribution.json`
- `source.json`, classification and compatibility sidecars, export logs, `run.json`
- `reproduce/run_comparison.py`, exporter scripts, CSV audit and attribution scripts

`source.json` records the immutable snapshot path and SHA-256; the comparison
summary records both CSV hashes. The export/comparison runner uses
`/private/tmp/brightpath-flow-venv/bin/python`. The supplemental audit and
attribution scripts were run with the Premise environment (Python 3.11).
The snapshot, generated CSVs and detailed inventory reports remain local and
Git-ignored. No commit or push was made.
