Cobalt and zinc extraction-attribution repair
=============================================

The repair restores the primary resource additions omitted by the upstream
checks in the ``e54ac3e2`` build. It preserves the post-allocation convention:
one kilogram of the target resource per kilogram of pure primary metal, with
the corresponding generic carrier extraction attributed downstream.

Historical audit: ``../conservative-defaults/README.rst`` documents the later
restriction of this convention to attributional databases. Consequential source
resource flows and marginal markets are now preserved.

Implementation
--------------

Material-path matching now requires kilogram inputs and uses the reference
product. The supplier name is a fallback only when the product is absent, so
an identifiable broken link remains unresolved. This excludes electricity
whose industry label happens to contain "cobalt". Carrier links still require
unique suppliers before their downstream-attribution policy is applied.

Primary zinc production from concentrate has an explicit attribution boundary
for its zinc output. The concentrate feed must be at least 1 kg per kg output,
other target-bearing feeds must total less than 1 kg, and their suppliers must
resolve uniquely. These mass checks bound applicability; they do not infer
grades or yields. Regional boundaries are planned together, including when
Canada already has extraction, so the small zinc/zinc-oxide production loops
cannot suppress the primary additions elsewhere. A failed boundary check is
preserved with a diagnostic and cannot fall back to a generic resource addition.

Recycling exclusions, unresolved-link handling, and preservation of cycles
without an explicit attribution boundary remain in force. The production
inputs themselves are retained, including their upstream burdens.

Validation scope
----------------

The comparison uses ecoinvent 3.12 cutoff and consequential, IMAGE SSP2-M 2050,
metals-only updates, encrypted IAM data and the compact inventory backend.
``Before`` is ``metals-audit-final-{system}``, the build with missing additions.
``After`` is ``metals-audit-resource-fix-{system}``. These are new isolated audit
databases; the previous databases and historical results are retained.

The full inventory comparison finds exactly 46 changed resource amounts in
each system: 17 cobalt and 29 zinc additions. All 32,840 cutoff and 27,576
consequential dataset identities, units, production exchanges, technosphere
exchanges and other biosphere exchanges are unchanged. A further no-write
rebuild after tightening the failed-feed guard has complete exchange equivalence
with the scored exports. Its final module hashes are recorded separately in
``validation.json``.

The 146 passing tests cover metals transformation, change reports, sector
validation and LCIA regression utilities. New cases include electricity loops,
regional zinc loops, existing Canadian extraction, reversed dataset order,
repeat corrections, secondary supply, downstream refining, missing and ambiguous
links, missing auxiliary suppliers, and insufficient or competing full-metal
feeds.

All 65 registered RELICS indicators are calculated for each of the 102 available
markets, before and after, in both system models (26,520 scores). The 6,630
pre-fix scores in each system reproduce the previous market audit. Registered
factors and processed-database fingerprints are checked; direct Brightway LCIA
cross-checks also run. ``graphite ore, mined`` remains unavailable. Missing
dedicated indicators are reported explicitly rather than as zero.

``report.html`` provides a searchable comparison and ``comparison.csv`` provides
both system models side by side. Each system directory contains complete
indicator matrices, product-associated indicator comparisons, exact market
identities, resource-exchange differences, and calculation/validation metadata.

Interpretation and limits
-------------------------

The recovered scores repair accounting omissions. They do not indicate an
increase in future physical resource demand, establish real recovery yields,
or validate every other metal's extraction attribution. The separate
consequential sulfur coverage issue remains unresolved. Generic material cycles
without declared boundaries also remain unresolved; the fix does not treat all
cycles as proof of missing extraction.

Reproduction
------------

Use the Brightway/RELICS environment containing the existing isolated audit
projects. Supply ``PREMISE_KEY`` through the environment. For each system model,
build a fresh database using the current implementation; the build script
refuses to overwrite an existing database:

.. code-block:: sh

   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
   python dev/validate_metals_correctness.py --repo . \
     --project metals-correctness-3.12-cutoff --source ecoinvent-3.12-cutoff \
     --biosphere ecoinvent-3.12-biosphere --system-model cutoff \
     --label resource-fix-cutoff --output /private/tmp/premise-metals-audit --write

   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
   python dev/validate_metals_resource_fix.py --system cutoff

Repeat with ``consequential`` replacing ``cutoff``. To regenerate the table:

.. code-block:: sh

   python dev/report_metals_markets.py \
     --results results/metals-correctness/resource-fix --resource-fix

The additional final-code equivalence check uses no-write build labels
``resource-fix-verified-cutoff`` and ``resource-fix-verified-consequential``;
run the comparison script with ``--reuse-scores --verified-label
resource-fix-verified`` to record it without repeating the LCAs. Raw licensed
inventories stay in private temporary storage, outside the repository.

.. list-table:: Verified scores, kg extracted element per kg market product
   :header-rows: 1

   * - Market
     - System model
     - Before repair
     - After repair
   * - Cobalt
     - Cutoff
     - 0.00004066
     - 0.99432933
   * - Cobalt
     - Consequential
     - -0.000000006
     - 0.99999904
   * - Zinc
     - Cutoff
     - 0.04701744
     - 1.00290303
   * - Zinc
     - Consequential
     - 0.04888402
     - 1.04431846

Across the full market comparison, only the Cobalt and Zinc indicators change
beyond numerical tolerance (relative 1e-9, absolute 1e-12). Other resource
indicators remain consistent with the pre-fix build.

The automated tests were run with:

.. code-block:: sh

   python -m pytest -q tests/test_metals.py tests/test_metals_change_report.py \
     tests/test_sector_validation.py tests/test_lcia_regression.py
