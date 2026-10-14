Metals correctness audit — 11 October 2026
================================================

This audit compares the unchanged ``fe02694ac5`` baseline with the metals
correctness changes in ``3002c98f`` on ``fix/metals-correctness``. It tests
implementation contracts and reports their numerical consequences. It does not establish
the physical plausibility of the prospective metal supply assumptions.

The subsequent all-market RELICS comparison in ``markets/README.rst`` and
``markets/report.html`` exposes missing cobalt and zinc extraction attribution
that the selected-product audit below did not detect. Those historical score
reductions were accounting gaps. ``cobalt-zinc-diagnosis/README.rst`` traces their
causes; ``resource-fix/README.rst`` records the follow-up correction and fresh
RELICS results. The original software-test results remain valid.

``conservative-defaults/README.rst`` records the subsequent safeguards for
component scope, temporal support, transport coverage and system-model
consistency. It supersedes the use of economic-allocation corrections in
consequential inventories and contains the current paired scores.

Design and reproducibility
--------------------------

The paired builds use ecoinvent 3.12 cutoff and consequential, IMAGE SSP2-M,
2050, encrypted IAM files, identical source inventories and scenario settings,
``PYTHONHASHSEED=0``, and the compact backend. There are separate pairs for
``update(["metals"])`` and the complete default update sequence. The changed
metals implementation and corrected configuration are the comparison treatment.
The baseline was extracted with ``git archive`` before changing the code. Raw inventory
snapshots remain outside the repository. Brightway exports use separate audit
projects and fresh database names; source databases were not overwritten.

``builds.json`` records source fingerprints, configuration and module hashes,
scenario settings, dataset counts, timings and diagnostic counts. Timings
were collected with concurrent jobs and warm caches; they are not controlled
performance benchmarks. ``inventory_summary.json``,
``selected_material_changes.csv`` and ``market_transport.csv`` summarize
inventory differences without redistributing the databases.
``baseline_builds.json`` retains the paired baseline build metadata, and
``transport_coverage.csv`` gives each market's primary share with and without
transport data. Module hashes identify the audited files; later docstring and
diagnostic-wording edits do not change their numerical behavior.

Each of ``cutoff``, ``consequential``, ``full-cutoff`` and
``full-consequential`` contains:

* ``activities.csv``: exact activities, locations, units and functional units;
* ``scores.csv``: source, baseline and corrected scores;
* ``differences.csv``: absolute and relative baseline-to-corrected changes;
* ``relics_coverage.csv``: characterization-factor mapping coverage;
* ``metadata.json``: methods, package versions and processed-database hashes.

RELICS 0.0.6 was installed from the local ``GitHub/RELICS`` repository in an
isolated environment. Its mapping-file SHA256 is
``c2cde55d0468c6153937b3707ec692b83cda426d0028837fc211550d9c4e0af0``.
The audit registers its mappings against the explicitly selected
``ecoinvent-3.12-biosphere``. All 65 resource indicators have matching flows.
Four alternative flow-name patterns (``Cu,``, ``Ni,``, ``Pt,``, ``Rh,``) are
absent; the corresponding full element names are covered. This checks mapping
coverage, not completeness of resource accounting in the inventories.

Climate uses ecoinvent 3.12 IPCC 2013 GWP100. The eight functional units are
one kilogram each of electric passenger-car motor, scooter motor, permanent
magnet, diesel car, petrol/natural-gas car, copper cathode and gold, plus one
kWh of Swiss low-voltage electricity. All locations and exact identities are
in the CSV files. Reference source/baseline scores were reused for the final
resource-only correction after verifying their unchanged databases; corrected
scores were recalculated. ``reused_stages`` identifies this in the metadata.

Implementation checks
---------------------

The complete non-slow suite passed: **1,047 passed, 2 skipped, 17 deselected**.
After the final diagnostic-wording edit, all 112 focused metals/report tests
passed again. Tests cover invalid/mismatched intensity units, missing versus
zero intensities, overlapping writers, shared-engine preservation, recycling,
non-metal flows, uncertainty rescaling, unresolved/cyclic upstream chains,
mine reagents, mine-to-refinery double counting, every transport mode,
transport units, small producer shares, unavailable suppliers, geographical
fallbacks, applied-share validation and compact metadata round trips.

All 15 final build checks completed with zero validation errors:

* 3.12 cutoff and consequential: metals-only and complete default updates;
* 3.12 cutoff: retained source/import uncertainty and the legacy backend;
* metals-only compatibility: 3.8 cutoff/consequential, 3.9 cutoff,
  3.9.1 consequential, 3.10 cutoff/consequential, 3.11 cutoff/consequential,
  and 3.12 EN15804 (with Premise's cutoff transformation setting).

The 3.12 cutoff compact and legacy builds have identical activity identities
and exchange lists (``backend_comparison.json``). The uncertainty build is a
construction/validation check, not Monte Carlo validation of prospective
uncertainty. Older-version compatibility checks do not recertify all of their
full-scenario LCIA references.

All four resource/climate comparisons completed: 8 functional units x 66
indicators x 3 stages x 4 build pairs = 6,336 finite scores. This includes
2,112 baseline-to-corrected comparisons, reported separately by indicator.

Canonical climate references
--------------------------------

Five full scenarios were also built from both the untouched and corrected
code: REMIND SSP3-rollBack 2050 and IMAGE SSP2-VLHO 2050 for 3.12 cutoff,
plus those two and TIAM-UCL SSP2-RCP19 2050 for 3.12 consequential. All 25
baseline scores satisfy the existing references. Three corrected scores exceed
the unchanged 3% tolerance: cutoff REMIND steel, cutoff IMAGE electricity and
consequential TIAM-UCL steel. Only these three references were refreshed after
the paired review. All 25 recorded corrected scores satisfy the updated file.

``regression_comparison.csv`` includes the baseline, corrected, old-reference
and current-reference values. ``reference_check.json`` records the reference
file hash and comparison result; this is a recheck of recorded scores, not
another set of LCA solves. The raw ``regression-*.json`` files preserve their
original reference comparisons. The detailed reference review is in
``docs/development/lcia-regression-reference-review.md``.

The cutoff climate run preceded the final resource-only mine/reagent guard
and metadata fix. Those changes leave its technosphere and IPCC scores
unchanged; the final RELICS comparisons include them. The generic git revision
in a raw regression report records the checkout's HEAD at collection time;
it does not identify uncommitted edits. The paired labels and the implementation
revision above identify this comparison.

Selected results
----------------

These are **metals-only, cutoff** results. Resource results are kilograms of
the specified resource per functional unit; electricity climate results are
kg CO2-eq/kWh. Comparisons should retain these units and the absolute values.

.. list-table:: Baseline and corrected scores
   :header-rows: 1
   :widths: 35 20 20 25

   * - Functional unit / indicator
     - Baseline
     - Corrected
     - Change
   * - Passenger-car motor / aluminium
     - 0.177858
     - 0.263497
     - +48.15%
   * - Passenger-car motor / copper
     - 0.144008
     - 0.121999
     - -15.28%
   * - Copper cathode / copper
     - 0.975848
     - 0.763197
     - -21.79%
   * - Gold / gold
     - 1.000008
     - 0.996240
     - -0.38%
   * - Permanent magnet / neodymium
     - 8.86082e-7
     - 1.21620e-4
     - +1.20734e-4 kg
   * - Swiss electricity / climate
     - 0.0316264
     - 0.0329372
     - +4.14%

These differences combine corrected direct intensities, supply shares,
transport and resource attribution. They cannot be attributed to one mechanism
from the totals alone. In particular, changing the motor conversion from
0.1 MW/53 kg to one removes a unit error, but does not validate the underlying
2050 intensity assumptions. The 2050 neodymium input is much smaller than the
2020 input in the supplied CSV. Its large relative change from the old code's
tiny baseline should not be described as a forecast increase in metal demand.

The new transport checks found an incorrect air provider: the old selector
could choose aircraft manufacture, measured in aircraft units, instead of a
freight service measured in tonne-kilometres. Retaining all transport modes
made this error visible in LCIA. The provider selector now checks service names
and units. Resource comparisons also exposed primary copper mines losing their
extraction because of trace copper-sulphate reagents; primary extraction now
remains at the mine. Both cases have regression tests.

Coverage is explicit: each 3.12 metals-only build records 926 decisions
(692 applied, 201 source-preserved, 23 missing-intensity preservations and
10 zero-intensity removals). Cutoff builds 102 markets and preserves one for
a missing supplier route; consequential builds 72 and preserves 31.
There are respectively 521 and 307 missing transport-coverage diagnostics,
and 32 geographical proxy fallbacks in each system model. Missing transport
is omitted with a diagnostic; it remains a data gap. Of the constructed markets,
27 cutoff and 21 consequential markets lack transport data for their entire
primary share. Both builds report 46
unresolved upstream-resource checks. Zero validation errors must not be read
as zero coverage gaps; ``builds.json`` retains the diagnostic counts.

Remaining methodological limits
-------------------------------

* Direct material overrides are not complete component compositions. For
  example, setting the cast-aluminium input does not reconcile an existing
  wrought-aluminium input or the metals embodied in other components.
  The corrected EV unit conversion also exposes a scope concern in the supplied
  data: its lithium rule adds 0.62835 kg of battery-grade lithium carbonate per
  kg of motor. That component assignment needs a separate source-data review;
  unit consistency alone does not make it physically appropriate. In the full
  cutoff build, the motor's climate score rises from 4.90272 to 15.14798 kg
  CO2-eq/kg. This increase must not be presented as a validated future motor
  design. Separating vehicle, battery and motor material scopes remains necessary.
* Petrol and diesel vehicles share one engine provider but imply different
  Pt/Pd component conversion factors. The engine is explicitly preserved.
  Separate component inventories are needed to apply those assumptions.
* Magnet conversions use the 53/1.98 reference motor-to-magnet mass ratio;
  this is not the scooter's magnet fraction. Technology-specific composition
  evidence is still needed.
* Resource attribution is a documented allocation convention. Upstream
  traversal and feed-mass screening do not solve mixed-feed mass balances,
  recovery yields or missing ore-grade data. Unresolved cases remain visible.
  For example, the consequential copper market still scores 3.00488 kg of
  copper extraction per kg of cathode (baseline: 3.31378). The audit does not
  establish a complete physical resource balance for that system model.
* Cutoff secondary supply is inherited from source markets. There is no
  dynamic scrap-stock model. Consequential country shares are not a model of
  marginal suppliers. Missing configured routes cause whole markets to be
  preserved, so coverage differs between source system models.
* RELICS measures characterized resource extraction. It does not measure
  scarcity, supply risk, installed material content or scenario plausibility.
  Source uncertainty is propagated for positive corrected flows; uncertainty
  about the attribution convention and future assumptions is not estimated.

Reproduction commands
---------------------

Use a Python 3.12 environment with premise, Brightway and RELICS installed.
Supply the encrypted IAM key through ``PREMISE_KEY``. The source project and
databases must already exist. Choose fresh labels for written outputs.
Limit numerical-library threads if sparse solves oversubscribe the machine.

.. code-block:: sh

   PYTHONHASHSEED=0 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
   python dev/validate_metals_correctness.py \
     --repo . --project ecoinvent-3.12-cutoff \
     --source ecoinvent-3.12-cutoff --biosphere ecoinvent-3.12-biosphere \
     --version 3.12 --system-model cutoff --label review-after-cutoff \
     --output /private/tmp/metals-review --write

   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
   python dev/score_metals_relics.py \
     --project metals-correctness-3.12-cutoff \
     --biosphere ecoinvent-3.12-biosphere --source ecoinvent-3.12-cutoff \
     --before metals-audit-review-before-cutoff \
     --after metals-audit-review-after-cutoff \
     --output results/metals-review/cutoff

Generate the corresponding baseline with the same build command, changing
``--repo`` to an untouched baseline checkout and ``--label`` to
``review-before-cutoff``. Add ``--full`` for the full update, or substitute
the consequential source/project/system model for that pair.
``--uncertainty`` retains source and imported uncertainty;
``--backend legacy`` checks the alternative backend without changing assumptions.
The summary utility expects the audit's ``before-*`` and ``final-*`` labels:

.. code-block:: sh

   python dev/summarize_metals_audit.py \
     --snapshots /private/tmp/premise-metals-audit \
     --output results/metals-correctness
