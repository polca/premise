RELICS comparison of all configured metal markets
=================================================

The complete table is in ``report.html`` (searchable by market and resource)
and ``comparison.csv`` (both system models side by side). This extends the
earlier eight-product audit, which only included copper and gold as standalone
metal markets. No transformation code was changed for this comparison.

These are the historical results before the cobalt/zinc repair. The subsequent
fix and all-market verification are in ``../resource-fix/README.rst``.

Scope
-----

The comparison uses the existing isolated ecoinvent 3.12 audit databases:
IMAGE SSP2-M, 2050, metals-only update, cutoff and consequential. ``Before``
means the untouched ``fe02694ac5`` implementation; ``after`` includes the
``3002c98f`` corrections. It is a code-change comparison within the same
scenario, not a comparison between present and future demand.

All 103 markets configured as active in the mining-share workbook are checked.
102 have a matching global market in each build. ``graphite ore, mined`` has
none in either build and is reported as unavailable. Selection uses the exact
name and reference product, requires a kilogram unit, and prefers World,
then GLO, then RoW. A change of location is recorded, including the preserved
source markets used when a configured production route was unavailable.

The functional unit is **1 kg of market product**. RELICS scores are kilograms
of the named extracted resource across its supply chain. Compounds and ores
are not converted to one kilogram of contained metal. The selected indicators
are explicit in each ``metadata.json``. Mixed concentrates have several rows;
they are not summed. Spodumene and dolomite use mineral indicators.

RELICS 0.0.6 supplies 65 indicators in this mapping. There is no dedicated
indicator for bentonite, sand/quartz, uranium or lutetium. All 65 indicators
are still calculated for those available markets, including their background
resource requirements, in each system's ``all_indicators.csv``. An unavailable
indicator or market is not a zero score.

Checks and interpretation
-------------------------

Registered characterization factors and processed-database fingerprints were
verified against the previous audit. Each available market was solved once
per build, and all 65 characterization vectors were applied to its aggregated
inventory. Direct Brightway LCIA and the previous copper/gold audit scores
agree to within 2.3e-15 across the eight cross-checks. There are 26,520 computed
scores: 102 markets x 65 indicators x 2 stages x 2 system models.

The follow-up investigation in ``../cobalt-zinc-diagnosis/README.rst`` confirms
the exact causes: a cobalt-industry electricity loop is misclassified as a metal
path, while small zinc-bearing process inputs create a primary-production loop.
Both prevent the planned downstream extraction additions.

The wider coverage exposes material attribution gaps missed by the earlier
selected-product audit:

* Cobalt extraction falls from approximately 1 to 0.00004066 kg/kg in cutoff
  and to approximately zero in consequential. Yet primary supply represents
  99.43% and 100% of those markets respectively.
* Zinc extraction falls from 1.03586 to 0.04702 kg/kg in cutoff and from
  1.04427 to 0.04888 in consequential. Both constructed zinc markets have
  a 100% primary share.
* The economic-allocation cobalt electrolysis routes in CN/CD and primary
  zinc production from concentrate in CN/RoW no longer receive the previous
  1 kg resource additions. Their diagnostics are ``upstream_unresolved``
  followed by ``preserved_extraction``. The checks avoid making an unsupported
  addition, but these routes currently lack replacement extraction attribution.
  The resulting score reductions must not be interpreted as resource savings.
* Consequential sulfur also falls from about 1 to approximately zero, while
  the selected market changes from World to the retained GLO market. Its
  resource coverage needs separate review.

Some decreases have a different interpretation. The cutoff lead, palladium
and platinum scores are close to their retained primary shares (0.45084,
0.84451 and 0.95540 respectively), consistent with removing virgin-resource
additions from secondary supply. Other changes combine supplier shares,
transport and resource-attribution changes. No complete causal decomposition
has been performed for every market.

Small negative values can occur in consequential results. Percent changes
use ``100 * (after - before) / abs(before)`` and are undefined at a zero
baseline. Very small denominators can produce misleadingly large percentages;
retain the absolute scores when interpreting them. Passing software tests and
link checks does not establish a complete physical metal-resource balance.

Reproduction
------------

The audit databases and RELICS methods must already exist. No IAM rebuild or
method registration is performed by these commands. Use the environment with
Brightway and RELICS installed:

.. code-block:: sh

   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
   python dev/score_metals_markets.py \
     --audit results/metals-correctness/cutoff \
     --output results/metals-correctness/markets/cutoff

   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
   python dev/score_metals_markets.py \
     --audit results/metals-correctness/consequential \
     --output results/metals-correctness/markets/consequential

   python dev/report_metals_markets.py --results results/metals-correctness/markets
