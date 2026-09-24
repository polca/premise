# SimaPro classification inference: implementation and comparison

> Historical first implementation. Superseded by [the refined identification report](simapro-classification-refined.md).

Brightpath now supports `category_mode="infer_classifications"` on SimaPro render,
validation and CSV writing. It preserves explicit production categories, resolves
included suppliers once, and uses their categories for both reference flows and links.
Waste references use the absolute source amount; links to waste treatment negate the
source amount, including uncertainty transformations. Default behavior is unchanged.

The classifier is in `brightpath/profiles/simapro_waste.py`. It interprets ISIC revision 4
and CPC, with reference signs and activity/product roles as supporting evidence.
Negative outputs in waste treatment/recovery sectors support waste status. Positive
recovered electricity/heat is treated as an ordinary product. Waste treatment producing
fertiliser can still be waste treatment. Positive material recovery, ambiguous waste
markets, missing classifications and conflicting evidence require explicit review.
External suppliers require an explicit category when absent from the inventory.

## Same-scenario comparison

Source: REMIND SSP1-PkBudg1000, 2050, ecoinvent 3.12 cut-off, 42,770 processes.
Reused the prepared inventory captured before the legacy writer mutated it, and the
previous legacy CSV. Regenerated the Brightpath CSV through its renderer and encoding.

- 40,988 processes resolved independently (95.83%).
- 1,782 unresolved processes explicitly retain legacy categories in the
  comparison harness only. The production library does not silently apply this fallback.
- 194 resolved classifications disagree with the legacy lookup:
  120 change from ordinary product to waste treatment, and 74 change the other way.
- Legacy waste processes: 5,214; regenerated export: 5,260.

Examples requiring review: some sewage-sludge landfarming/fertiliser datasets become
waste treatment, while recovered electricity from waste treatment and fly-ash brick
production become ordinary products. These are disagreements, not proof the legacy
lookup is wrong. The strongest remaining limitation is unresolved activity roles.

| Unresolved rule | Processes |
| --- | ---: |
| incomplete_isic | 446 |
| other_waste_activity_role | 327 |
| missing_activity_classification | 309 |
| negative_reference_outside_waste_sector | 214 |
| positive_waste_market | 188 |
| positive_material_recovery | 156 |
| recovery_product_role_unresolved | 123 |
| conflicting_service_and_sector | 18 |
| multiple_classifications | 1 |

## CSV results

| Section | Legacy rows | Brightpath rows |
| --- | ---: | ---: |
| Electricity/heat | 0 | 48,011 |
| Emissions to air | 397,536 | 394,251 |
| Emissions to soil | 91,991 | 91,979 |
| Emissions to water | 277,326 | 276,884 |
| Materials/fuels | 409,094 | 360,711 |
| Products | 37,556 | 37,510 |
| Resources | 80,574 | 69,645 |
| Waste to treatment | 82,411 | 82,173 |
| Waste treatment | 5,214 | 5,260 |

Normalized difference counts (grouped by emitted flow identity):

- rounding_only: 1,036,306
- section: 29,831
- brightpath_only_label: 225,759
- legacy_only_label: 242,697
- production_section_or_amount: 245
- uncertainty_fields: 13,173
- amount_beyond_legacy_rounding: 552
- multiplicity: 197

Other differences remain from the earlier comparison: energy input sections, biosphere
mapping/blacklisting, uncertainty retention, numeric rounding, and product-label
collisions. Inferred categories use a generic `Classified` folder; the many folder
changes do not represent additional waste-classification changes. Numeric comparisons
use a 0.051% tolerance for legacy rounding. Unmatched biosphere labels may be renamings.

The comparison retains the previously recorded compatibility adjustments: missing unit
labels, empty comments, inventory-indicator omission, and fossil-well compartment handling.
It is not a native-default Brightpath export. No SimaPro application import or LCIA
comparison was performed. Classifications cannot yet fully replace explicit categories.

## Validation and artifacts

1,037 tests passed; documentation build, Ruff, import sorting and formatting checks passed.
No commit was made.

Local artifacts (licensed inventory content; excluded from Git):
`export/simapro-comparison/classified/`. Includes regenerated CSV under
`brightpath-compatibility/`, per-process `classification-audit.json`,
`classification-changes.csv`, `classification-unresolved.csv`,
`csv-differences.csv`, and `comparison-summary.json` with file SHA-256 hashes.
Reproduction scripts are in its `reproduce/` folder and require the existing local
prepared snapshot and both checkouts. The legacy CSV is linked to the original export.
