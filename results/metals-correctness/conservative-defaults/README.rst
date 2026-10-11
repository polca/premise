Conservative metals defaults: verification
====================================================

This audit follows commit ``1c382077``. It tests the user's chosen policy:
preserve source inventories when the evidence needed for an override is absent,
and report the gap. It supersedes the earlier assumption that economic-allocation
resource corrections should also be applied to consequential inventories.
Numerical checks do not establish the physical feasibility of a future scenario.

Implemented safeguards
----------------------

* Twenty-one EV rules have explicit component-scope preservation reasons.
  Whole-vehicle observations are not assigned to motors; shared magnet suppliers
  are not overwritten using the mass fraction of one reference motor. The
  original source observations remain available for a future component review.
* Material series retain their individual source-year ranges. Interpolation
  inside those ranges is supported; outside them, the source input is preserved.
  Missing series remain distinct from explicit zero intensities.
* Consequential markets retain their marginal suppliers and resource exchanges,
  including the burdens underlying substitution credits. Historical average
  shares and economic-allocation corrections no longer replace these conventions.
* Attributional market replacement requires supported years, available positive
  routes and transport coverage for all positive origins. Otherwise the source
  market is preserved before proxies are created. Missing transport fractions
  are reported; observed zero distances are accepted.
* Per-rule decisions and scenario methodology metadata record the gaps. Successful
  zinc attribution is no longer incorrectly reported as a preserved correction.

Evidence and interpretation
---------------------------

The EV workbook's lithium row comes from Cullbrand and Magnusson, Table 8,
``The Use of Potentially Critical Materials in Passenger Cars`` (2012,
https://odr.chalmers.se/items/0926d689-2b39-4acf-950d-673f8d6660b7).
Its hybrid-car total, 6256.55 g, divided by 53 kg reproduces the runtime motor
intensity. The primary source identifies the battery as the main lithium use.
The full-sector cutoff and consequential builds now contain no direct
battery-grade lithium carbonate in the reference electric passenger-car motor.

The consequential copper anomaly was a model mismatch. Previously, direct
extraction was added at electrorefining while coproduct resource burdens were
removed from molybdenite mining. The source model resolves multifunctionality
by substitution; see https://support.ecoinvent.org/system-models.
Preserving that convention restores the market result to approximately the
source score. The source sulfur market supplies stockpiled sulfur, and already
has a near-zero net sulfur extraction score. No 1 kg/kg target is imposed.

Selected RELICS results, in kg resource per kg market product:

================= ================== ============= =============
System model      Market             Previous      Conservative
================= ================== ============= =============
cutoff            cobalt             0.994329      0.994393
cutoff            zinc               1.002903      1.002707
cutoff            copper, cathode    0.763197      0.767821
cutoff            sulfur             1.000096      0.933812
consequential     cobalt             0.999999      0.812798
consequential     zinc               1.044318      1.126238
consequential     copper, cathode    3.004876      0.907230
consequential     sulfur             -1.1429e-6    -6.0277e-7
================= ================== ============= =============

The source consequential copper result is 0.907229709; the conservative result
is 0.907229739. Small differences include inventory precision and background
material updates. Consequential cobalt and zinc are no longer calibrated to
approximately one: they retain the source model's resource accounting. These
are model comparisons, not estimates of improvement in accuracy.

Verification
------------

* 1,078 non-slow tests passed; 2 skipped; 17 slow tests deselected. The test
  environment used the existing local Brightpath 1.0.0a6 checkout for exporter
  tests. The initial run without that optional dependency failed in exporter
  tests; no exporter code was changed.
* Six build configurations passed without validation errors: metals-only
  cutoff/consequential 2050; cutoff 2020; full-sector cutoff/consequential 2050;
  and legacy-backend cutoff 2050 with uncertainty retained. All use ecoinvent
  3.12 and IMAGE SSP2-M. Earlier-version compatibility was not rerun here.
* Every in-ground resource exchange was preserved across 26,458 consequential
  datasets, compared with the existing pre-correction snapshot.
* Both models reproduced all 6,630 previous RELICS results exactly using the
  same scoring workflow. Each new build has 95 available global markets with
  65 indicators, giving 6,175 finite scores per model. Direct LCIA cross-checks,
  database fingerprints and RELICS mapping factors were verified.
* Eight configured products have no matching global source market: copper-cobalt
  ore, germanium concentrate, mined graphite ore, iridium, platinum group metal
  concentrate, rhenium, ruthenium and vanadium-bearing magnetite. These are
  reported as unavailable, not zero. Earlier builds constructed global markets
  for seven of these; conservative 2050 builds do not invent them.
* At 2050, all 103 configured markets preserve source supply. Each build applies
  55 material rules; 566 are preserved for unsupported years, 78 for component
  scope, 201 under existing policies, 23 for missing intensities and 3 for missing
  providers. At 2020, 51 cutoff markets are rebuilt and 52 preserve source supply
  for incomplete transport coverage.

``build_checks.json`` records build coverage and exact code/configuration hashes.
``score_checks.json`` records numerical reproduction and unavailable markets.
Per-model files contain complete paired indicators, selected own-resource scores,
methodology metadata, decisions, validation findings, and copper/sulfur source
comparisons with additive contributions and residuals. Raw licensed inventories
remain in private temporary storage.

Reproduction
-------------

Use the existing Brightway audit projects and matching ecoinvent 3.12 source
databases. Supply ``PREMISE_KEY`` through the environment. Build first without
``--write``; use new audit database names when writing a fresh result::

    python dev/validate_metals_correctness.py \
      --repo . --project metals-correctness-3.12-cutoff \
      --source ecoinvent-3.12-cutoff --biosphere ecoinvent-3.12-biosphere \
      --system-model cutoff --year 2050 --label conservative-cutoff \
      --output /private/tmp/premise-metals-audit

Repeat for consequential. After checking the no-write results, repeat with
``--write`` to create the isolated databases. The driver refuses to overwrite
existing databases. Full-sector checks use ``--full``; the legacy check uses
``--backend legacy --uncertainty``. The paired scorer requires the previous
resource-fix databases and the previously captured private pre-correction
snapshot described in ``../cobalt-zinc-diagnosis/README.rst``::

    python dev/validate_metals_conservative.py --system cutoff
    python dev/validate_metals_conservative.py --system consequential

Use single-thread numerical libraries for reproducible audit runs, as in the
earlier resource-fix audit. No code here supplies future mining capacities,
ore grades, recovery yields, dynamic scrap availability or validated marginal
supplier projections. Those remain data and modelling requirements.
