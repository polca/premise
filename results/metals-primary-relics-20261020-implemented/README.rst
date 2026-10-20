Refined-metal RELICS recalculation
=================================

Completed: 2026-10-20T09:35:16.576338+00:00.

Rebuilt from ecoinvent 3.12 cut-off using the current checkout,
encrypted IMAGE SSP2-M 2020 input, and a newly written isolated database:
``metals-primary-relics-20261020-implemented``.

There are 284 individual primary producers across 38
elements. 270 producers are within +/-5% of one kg/kg;
274 are within +/-10%. All 65 matched
RELICS indicators were recalculated for every producer, as well as original
ecoinvent counterparts where present.

Scope
-----

The complete metals update was applied, including material overlays, metal-market remapping and resource correction. Other sector updates were excluded.

Functional units are one kg from individual primary production datasets,
including primary co-products. Markets, secondary outputs, concentrates,
compounds, alloys, anodes and unrefined gold are excluded as functional units.
Pig iron and sponge iron remain without assuming pure Fe content. Ordinary
upstream market mixes remain in the supply chains.

The own-element score is kg target element extracted per kg reference product,
including the full upstream supply chain. One is a screening benchmark for
resource attribution, not an independent proof of physical extraction or
recovery efficiency. Attribution/characterization gaps remain for several
routes; a low score does not demonstrate negligible physical extraction.

Files
-----

* ``refined_metals_relics.xlsx``: one row per producer, all indicators, element
  summaries and explicit scope, in separate sheets.
* ``report.html``: standalone searchable report and plot.
* ``refined_producers.csv``: 284 own-element results, direct/upstream split.
* ``refined_producers_all_indicators.csv``: one row per producer, one column
  per indicator (all values in kg extracted per kg reference product).
* ``scores.csv`` and ``all_indicators.csv``: both source and corrected results.
* ``summary.csv``: minimum, median and maximum per element.
* ``comparison_with_previous.csv``: own-element score changes from the previous audit, when supplied.
* ``numerical_verification.csv``: independent LCIA and solve checks for every
  producer. ``coverage.csv`` records matched RELICS factors.
* ``metadata.json``: database fingerprints, code/configuration hashes, numerical
  checks and comparison against the previous audit.

Reproduction
------------

Use the premise environment with ``PREMISE_KEY`` set. A unique new database
name is required; the calculation script refuses to overwrite a database::

    python dev/audit_refined_metals_relics.py --database UNIQUE_NEW_NAME \
      --output results/NEW_DIRECTORY
    python dev/diagnose_refined_metals_relics.py results/NEW_DIRECTORY
    python dev/package_refined_metals_relics.py results/NEW_DIRECTORY
    python dev/report_refined_metals_relics.py results/NEW_DIRECTORY

Implemented corrections and verification
----------------------------------------

This is a fresh complete metals update with the approved A–G resource rules
implemented in premise, followed by a new Brightway export and all 18,460
producer/indicator scores. The targeted implementation and guard commits are listed
in validation.json. Source doses, recovery assumptions and consequential resource
inventories retain the documented scope.

All 122 direct resource assignments match the isolated assessment on the real
30,443-dataset inventory. Other exchanges are unchanged and correction is
idempotent. The 367 metals/mapping/reporting regressions pass. Test fixtures
exercise all 11 supported version labels; full real-source integration results
are recorded separately, including unavailable or initialization-blocked releases.

The fresh scores reproduce the isolated assessment to a maximum absolute
1.82e-12 kg/kg difference after accounting for Brightway's processed-matrix
amount precision. Every proposed assignment was verified at that precision.
The largest difference from the unquantized sensitivity calculation is 1.07e-7;
implementation_comparison.json records the numerical evidence.

The remaining arsenic zinc-proxy burden and legacy antimony recipe limitation
are retained explicitly. Larger values caused by the selected reduction and
purification losses have not been normalized away.

relics_heatmap.png shows all 284-by-65 scores rounded to one decimal. Only the
matching element/indicator cells are colored; other numeric cells are white.
Scores above 1.1 use purple. reproduction_scripts.zip preserves the exact
analysis, checking and plotting scripts under their dev/ paths.

Final guard verification and full-row review
-------------------------------------------

The fresh export pins the core implementation commit in metadata. A subsequent
shared-consumer guard refinement is independently verified by 22 negative cases
across every release label, real-source 2020/2050 builds, and the unchanged
122-assignment inventory result. validation.json records this final commit and
its code hashes separately, without rewriting the export's original provenance.

before_all_indicators.csv retains the complete pre-fix 284-by-65 matrix.
all_indicator_changes.csv records all 5,112 before/after changed pairs, including
off-diagonal changes. These are also included as spreadsheet tabs. The full
isolated A–G comparison is in ../metals-resource-fix-assessment-20261020.
The annotated heatmap retains numeric values in every box and colors only the
matching metal/indicator cells.
