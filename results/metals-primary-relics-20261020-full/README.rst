Refined-metal RELICS recalculation
=================================

Completed: 2026-10-20T07:44:57.781511+00:00.

Rebuilt from ecoinvent 3.12 cut-off using the current checkout,
encrypted IMAGE SSP2-M 2020 input, and a newly written isolated database:
``metals-primary-relics-20261020-full``.

There are 284 individual primary producers across 38
elements. 261 producers are within +/-5% of one kg/kg;
262 are within +/-10%. All 65 matched
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

Comparison and remaining departures
-----------------------------------

All 128 producers from the previous resource-correction-only audit are retained.
On this common set, 111 are within +/-5% (previously 104), and 112 are within
+/-10% (previously 107). The full update adds 156 datasets, including many
country-specific variants. Dataset counts are not counts of independent
production technologies. The five additional elements are dysprosium,
holmium, neodymium, praseodymium and terbium.

Of the 284 datasets, 22 fall outside +/-10%. Low own-element scores occur for
lithium, arsenic, silicon, the five additional rare earths, one antimony route,
and copper/silver co-products of zinc refining. Scores above 1.1 occur for
7N gallium, two iron routes and two nickel routes. Refer to the detailed scores
and attribution diagnostics before interpreting these as physical mass balances.
The resource-attribution convention directly sets many resource exchanges, so
agreement with one is not an independent validation of extraction or allocation.

All 284 numerical checks passed: maximum absolute technosphere solve residual
2.33e-10; maximum independent LCIA difference 1.34e-15. All reference production
exchanges are one kg. The full-update scope applies only to metals;
other scenario sectors remain outside this run.

The exact analysis scripts are retained in ``reproduction_scripts.zip`` with
``dev/`` paths; extract at the repository root before using the commands above.
RELICS 0.0.6 must be importable. This run read a relocated local RELICS checkout;
its mapping SHA-256 exactly matches the previous audit. No deleted checkout was
restored and no characterization factors were changed for this rerun.

Annotated heatmap
-----------------

``relics_heatmap.png`` contains all 284 datasets (rows) and all 65 RELICS
indicators (columns), with each of the 18,460 scores printed to one decimal.
The 5295 by 8188 pixel PNG colors only the 284 cells where the indicator
matches the dataset's target metal. All other cells retain their numeric
annotations on a white background. Colored cells use unrounded values for the
linear red-to-green scale from zero to one; values from 1.0 to 1.1 remain green,
and scores above 1.1 are purple. Each colored cell has a black outline. See ``relics_heatmap_metadata.json`` for
input hashes and rendering details. Reproduce with::

    python dev/heatmap_primary_metals_relics.py results/metals-primary-relics-20261020-full
