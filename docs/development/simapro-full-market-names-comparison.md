# Full-market-name CSV comparison against original legacy export

Fresh Brightpath CSV generated with full market activity names and the export-time
supplier-label collision guard. Baseline is the **original** legacy CSV in
`export/simapro-comparison/legacy/`, not the later reviewed legacy regeneration.
Both contain 42,770 processes.

Input is the prepared snapshot with the 21 reviewed municipal-waste CPC corrections.
Brightpath also uses the confirmed nitrogen-trifluoride effluent waste-treatment
exception; the original legacy baseline predates that correction.

## Supplier naming verified in the CSV

- product_rows: 42,770.
- duplicate_product_labels: 0.
- ambiguous_exchange_rows: 0.
- unmatched_exchange_rows: 0.

All 77 remaining market-label collisions are eliminated. Supplier labels now retain
wind-turbine technology, battery chemistry/scenario, electricity period, heat source
and vehicle-size distinctions. These checks parse the actual generated CSV, rather
than only exercising the naming helper. They do not substitute for a native SimaPro
import or LCIA comparison, neither of which was performed.

## Other differences from the original legacy CSV

- rounding_only: 1,075,339.
- section: 47,837.
- brightpath_only_label: 185,572.
- legacy_only_label: 201,401.
- production_section_or_amount: 311.
- uncertainty_fields: 14,230.
- multiplicity: 197.
- amount_beyond_legacy_rounding: 516.

| Section | Original legacy | Brightpath |
| --- | ---: | ---: |
| Electricity/heat | 0 | 48,011 |
| Emissions to air | 397,536 | 394,251 |
| Emissions to soil | 91,991 | 91,979 |
| Emissions to water | 277,326 | 276,884 |
| Materials/fuels | 409,094 | 360,747 |
| Products | 37,556 | 37,800 |
| Resources | 80,574 | 69,645 |
| Waste to treatment | 82,411 | 82,137 |
| Waste treatment | 5,214 | 4,970 |

Classification audit:

- resolved: 40809.
- explicit_legacy_fallback: 1961.
- resolved_disagreements_with_original_legacy: 260.

The comparison remains adapted: missing unit labels are supplied, empty comments
filled, inventory indicators omitted, fossil-well compartments mapped, and unresolved
categories supplied explicitly from legacy mappings. Inferred folders use `Classified`.
Biosphere naming and exclusions, uncertainty retention and numeric precision differ.
Grouped amount differences include sign and biosphere mapping effects. Unmatched
labels are not necessarily missing flows. Legacy rounding tolerance is 0.051%.

The 260 resolved classification disagreements include the 259 previously reported
cases and the now-confirmed effluent treatment (the original baseline classified it
as material). Comparing against the reviewed legacy export would exclude that last
classification disagreement. This report does not claim all remaining disagreements
are errors in either exporter.

## Files

Local artifacts: `export/simapro-comparison/full-market-names/`.

- `brightpath-compatibility/simapro_export_remind_SSP1-PkBudg1000_2050.csv`
- `legacy/simapro_export_remind_SSP1-PkBudg1000_2050.csv` (link to original baseline)
- `comparison-summary.json` with SHA-256 hashes and verification results
- `csv-differences.csv`, `product-label-collisions.csv`, `process-identities.csv`
- `classification-audit.json`, compatibility sidecars, and `reproduce/` scripts

The scripts require both local checkouts and the corrected prepared inventory snapshot.
Licensed inventory data remains excluded from Git. No commit or push was made.
