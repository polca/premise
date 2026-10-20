Metals mapping validation across ecoinvent releases
===================================================

All 282 regression tests passed. The mapping-selection, co-product exclusion,
and material-plan conversion tests are parameterized over all 11 supported
version labels. These are fixture-based tests, independent of local inventory
availability.

The broader inventory/import/metals suite had 394 passing tests and 21 failures. The identical 21 failures reproduce with the original workbooks and HEAD tests; there are no newly failing tests. Two new market-supplier regressions passed. See regression_comparison.json.

Twenty-one supplier links in three workbooks use native markets. All 718 numeric/formula cells in changed sheets, including cached formula values, are retained. See workbook_preservation.json.

source_market_availability.csv records native market names and geographies for all tested models. Market availability across system models does not imply availability in older releases: the requested lithium iron phosphate market is absent in 3.8 cutoff and consequential, and present in 3.9 onward.

Complete metals-update integrations
----------------------------------

13 version/system-model combinations passed, covering
26 complete scenario updates. 5 combinations
failed, and 1 could not be run because the source was unavailable.
Failures are recorded explicitly rather than counted as successful tests.

Each integration uses the actual release database, standard source cleaning
and default inventory imports, compact inventory backend, encrypted IMAGE
SSP2-M data, and years 2020 and 2050. The only sector transformation requested
is the complete ``metals`` update. Other sector updates, LCA scoring and
scenario exports are outside this test's scope.

Passing cases require nonempty intended mappings for EV, PEMFC, AEL, HTEL and
c-Si, exclusion of the six known unrelated component/process names, execution
of every compiled material-rule pair, and no unsuppressed validation errors.
For the excluded component datasets present in each build, technosphere input
quantities grouped by product and unit must remain unchanged after the update.
Existing material/resource preservation warnings are retained and counted in
each result; passing does not mean every material input was overwritten.

The tested models are cutoff for every supported release, consequential for
the versions accepted by premise's consequential API (3.8 onward), and 3.12
EN15804. As in the repository's EN15804 integration test, EN15804 uses its own
source database and premise's cutoff transformation rules. APOS and consequential
versions before 3.8 are outside the supported NewDatabase configuration.

Results
-------

* 3.5 cutoff: not_available — No local cutoff source; official ecoinvent catalogue exposes only apos for 3.5 with the stored account.
* 3.6 cutoff: failed — lci-PV-2026.xlsx, lci-PV-perovskite.xlsx, lci-ammonia.xlsx, lci-batteries-ASSB.xlsx, lci-batteries-LiO2.xlsx, lci-batteries-LiS.xlsx, lci-batteries-NMC622-NMC532.xlsx, lci-batteries-NMC955-LTO.xlsx, lci-batteries-SIB.xlsx, lci-batteries-organic-and-hybrid-redox-flow.xlsx, lci-batteries-vanadium.xlsx, lci-battery-capacity.xlsx, lci-biofuels.xlsx, lci-carbon-capture.xlsx, lci-cobalt.xlsx, lci-hydrogen-electrolysis.xlsx, lci-nuclear_EPR.xlsx, lci-steel.xlsx, lci-synfuels-from-FT.xlsx, lci-synfuels-from-methanol.xlsx, lci-syngas.xlsx: Fix the unlinked exchanges before proceeding
* 3.7 cutoff: failed — lci-ammonia.xlsx, lci-batteries-ASSB.xlsx, lci-batteries-NMC622-NMC532.xlsx, lci-batteries-NMC955-LTO.xlsx, lci-batteries-SIB.xlsx, lci-batteries-organic-and-hybrid-redox-flow.xlsx, lci-batteries-vanadium.xlsx, lci-battery-capacity.xlsx, lci-carbon-capture.xlsx, lci-synfuels-from-FT.xlsx, lci-synfuels-from-methanol.xlsx, lci-syngas.xlsx: Fix the unlinked exchanges before proceeding
* 3.7.1 cutoff: failed — lci-ammonia.xlsx, lci-batteries-ASSB.xlsx, lci-batteries-NMC622-NMC532.xlsx, lci-batteries-NMC955-LTO.xlsx, lci-batteries-SIB.xlsx, lci-batteries-organic-and-hybrid-redox-flow.xlsx, lci-batteries-vanadium.xlsx, lci-battery-capacity.xlsx, lci-carbon-capture.xlsx, lci-synfuels-from-FT.xlsx, lci-synfuels-from-methanol.xlsx, lci-syngas.xlsx: Fix the unlinked exchanges before proceeding
* 3.8 cutoff: failed — lci-batteries-ASSB.xlsx: Fix the unlinked exchanges before proceeding
* 3.8 consequential: failed — lci-batteries-ASSB.xlsx: Fix the unlinked exchanges before proceeding
* 3.9 cutoff: passed (2020, 2050)
* 3.9 consequential: passed (2020, 2050)
* 3.9.1 cutoff: passed (2020, 2050)
* 3.9.1 consequential: passed (2020, 2050)
* 3.10 cutoff: passed (2020, 2050)
* 3.10 consequential: passed (2020, 2050)
* 3.10.1 cutoff: passed (2020, 2050)
* 3.10.1 consequential: passed (2020, 2050)
* 3.11 cutoff: passed (2020, 2050)
* 3.11 consequential: passed (2020, 2050)
* 3.12 cutoff: passed (2020, 2050)
* 3.12 consequential: passed (2020, 2050)
* 3.12 EN15804: passed (2020, 2050)

Files and reproduction
----------------------

``matrix.csv`` contains the compact status table. ``summary.json`` contains
all case details, input identities, configuration hashes, mapping counts,
per-scenario execution metrics, validation warning counts and preservation
checks. Individual case JSON files hold the same detailed results.
``initialization_unlinked_exchanges.csv`` identifies the unlinked exchanges and
their workbooks for failed initialization cases. Raw import
and build logs remain private under ``/private/tmp``; no licensed inventories
are exported into this report or repository. Existing source databases are
read; missing sources are imported into dedicated validation projects.

Run in the premise environment with access to local Brightway projects::

    python -m pytest tests/test_metals.py tests/test_activity_maps.py tests/test_metals_change_report.py -q
    python dev/test_metals_all_versions.py
    python dev/report_metals_version_tests.py

Use ``--retry-failed`` to rerun failed integrations after fixing their blockers.
Remove a chosen case's result JSON to repeat a passed case. Missing-version
imports require stored ecoinvent credentials; the import helper uses dedicated
projects and does not overwrite existing releases.
