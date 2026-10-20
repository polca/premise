Refined-metal RELICS recalculation
=================================

Completed: 2026-10-19T15:01:18.383373+00:00.

Rebuilt from ecoinvent 3.12 cut-off using the current resource correction,
encrypted IMAGE SSP2-M 2020 input, and a newly written isolated database:
``metals-refined-relics-20261019-recalculated``.

There are 128 individual primary producers across 33
elements. 104 producers are within +/-5% of one kg/kg;
107 are within +/-10%. All 65 matched
RELICS indicators were recalculated for every producer, as well as original
ecoinvent counterparts where present.

Scope
-----

Only ``Metals.post_allocation_correction`` was applied. Material overlays and
metal-market remapping were excluded. Full metals update remains blocked by a missing conversion for polyethylene terephthalate adhesive film production, for fuel cells, which the fuel-cell equipment mapping selects. The traction-motor mapping blocker has been fixed.

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
* ``refined_producers.csv``: 128 own-element results, direct/upstream split.
* ``refined_producers_all_indicators.csv``: one row per producer, one column
  per indicator (all values in kg extracted per kg reference product).
* ``scores.csv`` and ``all_indicators.csv``: both source and corrected results.
* ``summary.csv``: minimum, median and maximum per element.
* ``numerical_verification.csv``: independent LCIA and solve checks for every
  producer. ``coverage.csv`` records matched RELICS factors.
* ``metadata.json``: database fingerprints, code/configuration hashes, numerical
  checks and comparison against the previous audit.

Reproduction
------------

Use the premise environment with ``PREMISE_KEY`` set. A unique new database
name is required; the calculation script refuses to overwrite a database::

    python dev/audit_refined_metals_relics.py --database UNIQUE_NEW_NAME \
      --correction-only --output results/NEW_DIRECTORY \
      --full-metals-update-limitation 'KNOWN_FULL_UPDATE_LIMITATION'
    python dev/diagnose_refined_metals_relics.py results/NEW_DIRECTORY
    python dev/package_refined_metals_relics.py results/NEW_DIRECTORY
    python dev/report_refined_metals_relics.py results/NEW_DIRECTORY
