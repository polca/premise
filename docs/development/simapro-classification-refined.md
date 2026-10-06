# Refined SimaPro waste identification

Supersedes the initial classification implementation and its inference conclusions.
The previous comparison remains available as a historical artifact.

## Revised behavior

Opt-in `category_mode="infer_classifications"` preserves explicit production
SimaPro categories. Otherwise, the reference product, unit and amount constrain
ISIC/CPC inference. Dataset and production reference identities must agree.

- Recovered electricity/heat, fertiliser, compost, biogas and dried poultry manure
  are ordinary products only when their names, CPC codes and unit dimensions agree
  and their reference amounts are positive. These are conservative reviewed product
  rules, not a comprehensive recovered-product vocabulary.
- Energy CPC codes attached to kilograms of municipal waste now require review.
  They cannot classify the dataset as energy merely because its industry recovers energy.
- Negative recovered products require review. A negative reference quantity supported
  by a waste-sector classification continues to identify waste treatment.
- A treatment activity name no longer turns positive allocated outputs into waste
  treatment. Unrecognized positive treatment references, including service references,
  require explicit review. Unlisted recovery products also remain unresolved.
- Existing included-supplier resolution and sign handling remain: reference waste
  amounts become positive with magnitude preserved; links to waste suppliers negate
  their amounts and transform uncertainty consistently. Recovered-product links retain
  their source signs. Explicit reviewed categories remain authoritative.

The distinction is based on [ecoinvent's reference-product documentation](https://support.ecoinvent.org/activities-products).
It is not an official CPC/ISIC-to-SimaPro mapping. Name patterns are only supporting
product evidence, not a replacement for explicit supplier metadata.

## Regenerated comparison

Same prepared REMIND SSP1-PkBudg1000 2050, ecoinvent 3.12 cut-off inventory;
42,770 processes. Legacy CSV reused; Brightpath CSV regenerated in a new output folder.

Of the initial 194 disagreements:

- 112 recovered-product cases now agree with legacy.
- 22 now require review (21 municipal-waste energy-CPC conflicts and one positive effluent reference).
- 60 still disagree.

Across the complete inventory, applying the recovered-fertiliser rule consistently
reveals another 199 disagreements with legacy. These are the same positive-product
semantics as the 100 fertiliser cases corrected above, rather than a separate rule.
They remain candidates for semantic review, not certified corrections.

| Result | Processes |
| --- | ---: |
| Independently resolved | 40,808 (95.41%) |
| Unresolved, explicit legacy fallback in harness | 1,962 |
| Waste under new rule, ordinary under legacy | 7 |
| Ordinary under new rule, waste under legacy | 252 |
| Total resolved disagreements | 259 |

The 252 consist of 199 recovered-fertiliser, 26 recovered-energy and 27 non-waste-sector
cases. Legacy exports 5,214 waste processes; the refined hybrid export exports 4,969.
A lower disagreement count is not the acceptance criterion: inferred orientation must
match the specific reference flow. No SimaPro application import or LCIA validation
was performed. Existing naming collisions and other comparison limitations remain.

The comparison retains explicit compatibility adjustments for missing unit labels,
empty comments, inventory indicators and fossil-well compartments. Unresolved
cases retain legacy categories only in the comparison harness; the library refuses
to export them without explicit categories. Inferred folders remain `Classified`.

## Validation and artifacts

1,057 tests passed. Documentation build, Ruff, import sorting, formatting and
whitespace checks passed. Regression tests cover recovered-product input signs,
negative recovered products, inconsistent reference identities, CPC/unit/product
conflicts, explicit overrides, non-mutation and waste supplier/reference signs.

Artifacts: `export/simapro-comparison/classified-refined/`, including:

- `brightpath-compatibility/simapro_export_remind_SSP1-PkBudg1000_2050.csv`
- `classification-audit.json`, `classification-changes.csv`, `classification-unresolved.csv`
- `comparison-summary.json` with hashes and aggregate CSV metrics
- `csv-differences.csv`, `product-label-collisions.csv`
- `reproduce/` scripts requiring both local checkouts and the prepared snapshot

Licensed inventory artifacts remain excluded from Git. No commit or push was made.
