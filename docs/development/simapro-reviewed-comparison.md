# SimaPro comparison after reviewed waste corrections

Both CSVs were freshly regenerated from the same prepared REMIND SSP1-PkBudg1000,
2050, ecoinvent 3.12 cut-off inventory (42,770 processes). This run does not reuse
the old legacy CSV. It invokes Premise's `Export.export_db_to_simapro()` and
Brightpath's SimaPro renderer with `category_mode="infer_classifications"`.

The original prepared snapshot predates the legacy writer's in-place mutations.
Exactly 21 CPC fields were refreshed from Premise's corrected classifications file
before saving a new shared input snapshot. All other input data was retained.
Each exporter loaded that snapshot separately. The current legacy mapping includes
the confirmed nitrogen-trifluoride effluent treatment activity.

## Reviewed cases

All 22 are verified in the actual CSVs: both exporters put them in **Waste treatment**
with a **+1 reference amount**. The 21 incineration datasets now have CPC 39990,
but still use explicit legacy categories in the Brightpath comparison harness:
the generic inference rule intentionally leaves their positive reference amounts
unresolved. The effluent dataset uses Brightpath's reviewed identity-specific exception.
See `reviewed-22-checks.json` for every checked process.

## Remaining differences

- Independently resolved categories: 40,809.
- Explicit legacy-category fallback: 1,961.
- Resolved classification disagreements: 259.
  - recovered_fertiliser_product: 199.
  - non_waste_sector: 27.
  - recovered_energy_product: 26.
  - waste_sector_negative_reference: 7.

| Section | Legacy rows | Brightpath rows |
| --- | ---: | ---: |
| Electricity/heat | 0 | 48,011 |
| Emissions to air | 397,536 | 394,251 |
| Emissions to soil | 91,991 | 91,979 |
| Emissions to water | 277,326 | 276,884 |
| Materials/fuels | 409,093 | 360,747 |
| Products | 37,555 | 37,800 |
| Resources | 80,574 | 69,645 |
| Waste to treatment | 82,412 | 82,137 |
| Waste treatment | 5,215 | 4,970 |

CSV differences after grouping emitted identities and normalizing uncertainty fields:

- rounding_only: 1,036,343.
- legacy_only_label: 242,697.
- brightpath_only_label: 225,759.
- section: 29,794.
- production_section_or_amount: 310.
- uncertainty_fields: 13,173.
- amount_beyond_legacy_rounding: 515.
- multiplicity: 197.

The classification disagreements are 199 recovered-fertiliser cases, 26 recovered-energy
cases, 27 ordinary non-waste-sector cases and 7 negative waste-reference cases. They
need semantic review; disagreement with legacy alone is not proof of an error.

Other differences include energy input sections, biosphere names and blacklisting,
uncertainty preservation, reference amounts and numeric precision. Inferred folders
use `Classified`, so folder metadata differs too. Product-label collisions remain.
Unmatched labels can be renamings; they are not all missing flows. The comparison
uses a 0.051% tolerance for legacy numeric rounding.

The Brightpath harness still supplies missing unit labels, fills empty comments,
omits inventory indicators as legacy does, and maps fossil-well compartments.
It also explicitly supplies legacy categories for unresolved datasets. This is
not an unadapted default Brightpath export. No native SimaPro import or LCIA comparison
was performed. The legacy writer reports 1,519 unmatched flow categories; Brightpath
reports 5,881 unused-exchange warnings. Those diagnostics remain unresolved.

## Artifacts

All inventory artifacts are local and excluded from Git under
`export/simapro-comparison/reviewed/`:

- `legacy/simapro_export_remind_SSP1-PkBudg1000_2050.csv`
- `brightpath-compatibility/simapro_export_remind_SSP1-PkBudg1000_2050.csv`
- `comparison-summary.json` with hashes, `csv-differences.csv`
- `classification-audit.json`, `classification-changes.csv`, `classification-unresolved.csv`
- `classification-corrections.json`, `reviewed-22-checks.json`
- `reproduce/` scripts (require local prepared snapshot and both checkouts)

No exporter code was changed and no commit or push was made during this rerun.
