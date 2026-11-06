Physical inventory probing campaign
====================================

Status: implementation reference, 2026-11-01. The reusable automated core,
Brightway adapter, matched campaigns, reports and mutation tests are implemented
in ``premise.audit``. See :doc:`/user_guide/physical-audits` for the supported API.
Human scientific review and additional stress-case execution remain explicit
campaign work; a queued review is not a completed scientific assessment.

Purpose and scope
-----------------

Find scientifically implausible inventories, missing elementary flows, wrong
carbon origins, incorrect supplier boundaries, and unit or sign errors in
generated premise databases. Numerical agreement between two calculations is
one check; physical correctness and inventory coverage require separate checks.

The campaign combines a whole-database screen with detailed automated probing
of 5,000 activities, independent physical expectations, controlled fault
injection, and human review of selected cases. It does not certify every
activity against measured industrial data. Every conclusion identifies its
evidence and applicable system boundary.

Scope of the first implementation is deterministic auditing. Scenario generation
uses existing NewDatabase arguments. No new NewDatabase argument is proposed.
Audit runs are read-only after writing a separately named scenario database.
No inventory coefficients, reference scores or tolerances are automatically
changed to make an audit pass.

Existing assets and their limits
--------------------------------

Reuse these components after checking their interfaces against the selected
commit:

* ``premise/validation/validation_framework.py`` provides read-only graph validation,
  issue records, provenance and scoped applicability. Reuse its established
  structural rules rather than reimplementing provider-existence checks.
* ``premise/validation/validation.py`` checks fuel-market composition and contains
  car/truck carbon checks. Some checks explicitly verify total CO2 without
  inferring its generic-market carbon split. Broaden the audit to aircraft,
  shipping, stationary combustion and industrial fuel users.
* ``premise/materials/metals_resource_boundaries.py`` documents existing attribution
  conventions. Read those conventions as model contracts; independently check
  their consequences and upstream coverage. Do not obtain every expectation
  by calling the correction function being audited.
* The paired GWP and resource audits under ``results/`` provide an existing
  2,000-activity diagnostic panel, native database identities, RELICS mappings,
  contribution traces and numerical checks. Keep those results frozen.
* Local ``dev/compare_premise_resources.py`` and its report/diagnostic helpers
  demonstrate reuse of a numerical factorization. Extract suitable mechanics
  into a maintained audit tool rather than requiring ignored local scripts.

1. Freeze the experiment
------------------------

Use a clean checkout of the selected premise commit, an explicit dependency
environment, and a private Brightway data root. The first new campaign must
include the committed hydrogen and iridium corrections. Do not reuse a graph
that predates either correction without labelling it as a historical control.

Record:

* Source database version and system model, biosphere identity and method keys.
* Source-code revision, dependency versions, build arguments and random seeds.
* Encrypted IAM file identity and provenance; never record its decryption key.
* Import selection, source inventory identities and relevant assumptions.
* Reference-product quantity, unit, sign and production coefficient for every
  demand. Use explicit product-row mappings, not matrix-diagonal assumptions.
* Database counts, unmatched/ambiguous identities, existing validator results,
  method coverage and build/export warnings.

Runtime provenance fingerprints may identify inputs and cached artifacts. Test
expectations use explicit physical quantities and semantic identities, not
workbook hashes or frozen recipe fingerprints.

Retain a final pre-export inventory checkpoint and the written Brightway graph.
For new findings, compare source workbook, prepared inventory, post-sector
inventory, final checkpoint and written exchanges. Add targeted intermediate
checkpoints only where needed to locate the first defective transformation.

2. Build the scenario panel in two stages
-----------------------------------------

Core panel, all based on ecoinvent 3.12 cut-off:

* Native ecoinvent source as an inherited-data control.
* Current premise, REMIND SSP2-PkBudg1000, 2020.
* Current premise, REMIND SSP2-PkBudg1000, 2030.
* Current premise, REMIND SSP2-PkBudg1000, 2050, the primary discovery graph.

The native source does not contain every supplemental activity. Audit matches
where they exist and explicitly report unsupported comparisons. A 2020 premise
build also includes imported inventories and transformations; it is not assumed
to reproduce the native database.

Stress panel, after the core detector is working:

* REMIND SSP2-NPi, 2050, cut-off, as a pathway contrast.
* IMAGE SSP2-VLHO, 2050, cut-off, after confirming the encrypted input is
  available, as an independent IAM contrast.
* REMIND SSP2-PkBudg1000, 2050, consequential.
* REMIND SSP2-PkBudg1000, 2050, EN15804.

Use corresponding native consequential/EN15804 sources for inherited-data
comparisons. Do not interpret a difference between system models as a defect
by itself. Audit 1,000 stratified activities plus every implicated supply
chain in each stress graph. Score-screen the complete graph where feasible.
Expand to 3.8 and 3.11 only for findings sensitive to version migration,
resource-flow naming or supplier availability. This first campaign does not
promise a complete version-by-model matrix.

3. Select 5,000 activities with explicit sampling reasons
---------------------------------------------------------

Draw from all eligible activities in the primary discovery graph, including
new imports and activities absent from premise 2.5.5. Establish unique semantic
identities from name, reference product, geography and unit; retain node IDs
internally and fail on unresolved ambiguity.

Use three disjoint panels:

* 3,000 activities selected uniformly without replacement from the entire
  eligible graph. Use this panel for reporting random-sample flag rates.
* 1,500 activities selected randomly from the remaining graph within declared
  strata: fuel production/markets/combustion; mining and primary metals;
  recycling and treatment; batteries and electrolysers; chemicals and water
  supply; agriculture/forestry/biomass; electricity/heat and infrastructure;
  transport and other manufacturing. Balance geography, physical reference
  units, process/market roles, and native/imported/transformed provenance.
* 500 activities selected from remaining high-risk candidates: zero resource
  scores despite primary-material output, large fossil/biogenic split changes,
  changed suppliers, small positive scores hidden by credits, native-to-custom
  substitutions and scores with unusual dimensional ratios.

If a stratum is exhausted, redistribute its quota using a predeclared rule.
Record the seed, candidate universe, quotas, inclusion reason and inclusion
probability where defined. Do not combine biased panels into a database-wide
error-rate estimate.

Keep the old 2,000-activity sample and known defect sentinels as separate
companion panels. Report any overlap explicitly; do not call them additional
independent random draws. Reuse matched identities in other core graphs and
report missing/new identities separately.

Sampling excludes only activities for which no unique functional product can
be resolved. Waste-treatment, negative-output, zero-score and credit-bearing
activities remain eligible, with their conventions recorded.

4. Register physical indicators and coverage
--------------------------------------------

Carbon
~~~~~~

Register separate inventories for fossil CO2 to air, non-fossil CO2 to air,
CO2 associated with land/biomass-stock changes, atmospheric CO2 uptake, and
geological CO2 resource inputs where present. Keep air subcompartments,
including high-altitude aircraft emissions, visible in diagnostics.

Report signed totals and, for detailed probes, positive and negative
contributions separately. An unchanged sum of fossil and non-fossil CO2 must
not conceal a classification error. Include methane, carbon monoxide and
carbon retained in products in applicable process carbon ledgers, with the
appropriate conversion to kg C. Unspecified carbon origin remains unresolved.

EF 3.1 climate change is a secondary diagnostic alongside these physical
inventories. It helps expose the consequence of carbon reclassification, but
does not supply the expected physical split.

Metals and minerals
~~~~~~~~~~~~~~~~~~~

Use all 65 RELICS 0.0.6 extraction mappings, preserving each indicator's
identity and conversion factors. Include additional raw elementary-resource
flows where a relevant material has no usable RELICS coverage.

Explicit priority elements are Ir, Pt, Pd, Ru, Li, Ge, Ga, As, Cu, Ni, Co,
Mn, Al, Fe, Sb, V, Nb, and rare earths. Priority is not exclusion: every
supported indicator is scored.

Report direct extraction, upstream extraction and negative credit contributions
separately when diagnosing an activity. Keep elemental and mineral indicators
separate; do not sum overlapping categories into a purported total metals mass.
Document where an extraction indicator represents an attribution convention
rather than reconstructed physical ore throughput or recovery losses.

Water
~~~~~

Use native physical inventory indicators for freshwater extraction, surface
water, groundwater, total water extraction, saltwater and undefined origin.
Audit their actual CF flow lists and units in the selected version.

Report cooling and turbine water separately from the standard extraction
indicators, whose coverage can exclude those flows. Keep water emitted to
water and air visible as separate ledgers. Do not label withdrawals as net
consumption or infer a complete water balance from an evaporation score.

Purchased tap/deionised/process water is a technosphere input. Extraction may
be correctly represented at its supplier instead of in the consumer. Trace
the supplier before declaring a missing water resource, and avoid adding the
same withdrawal at both levels. Process-level closure requires documented
product moisture, carrier water, evaporation, discharge and chemical reaction
water, where applicable.

Land
~~~~

Use land occupation in m2 year, flooding occupation, seabed occupation and
total surface occupation as distinct indicators. Inspect transformation-from
and transformation-to flows separately in m2. Never add transformation in m2
to occupation in m2 year, or interpret matched from/to transformation areas
as an absence of land conversion.

Coverage records
~~~~~~~~~~~~~~~~

For every indicator report the intended flow scope, matched biosphere codes,
units, compartments, characterized rows present in the graph, rows actually
used, and flows that remain unmatched. Independently audit the mappings before
trusting a zero result.

Distinguish absent inventory, incorrect flow identity/compartment/unit,
uncharacterized inventory, disconnected suppliers, genuine zero demand,
and cancellation by negative contributions. Report indicators with zero
coverage or no sampled use explicitly. Coverage counts do not establish
correctness or unknown impact magnitude.

5. Compute efficiently and verify the arithmetic
------------------------------------------------

Load and factorize each graph's technosphere matrix once per deterministic
scoring session. Share this numerical factorization across all indicators and
functional units. Never reuse a factorization after its matrix changes.

For each linear indicator, obtain a characterized direct vector from the
biosphere matrix and solve the transposed technosphere system. This gives
signed scores for all reference-product rows, enabling a whole-database
outlier screen without thousands of separate factorizations.

For all 5,000 detailed demands, perform forward solves using that same
factorization. Compare scores with the adjoint results, obtain inventories,
and separate contribution signs where needed. Gross positive/negative
contribution diagnostics require the solved contribution structure; do not
assume they are equivalent to a single net linear indicator.

For at least 250 demands per core generated graph, selected across sectors,
zero/negative scores and outliers, cross-check with standard Brightway LCI/LCIA
and direct aggregation of the selected raw elementary flows. Include all
critical numerical anomalies even if this exceeds 250.

Check finite values, scaled equation residuals, normalization, production
signs and product/activity/biosphere index mappings. Start numerical score
comparison with rtol 1e-7 and documented unit-scaled absolute tolerances;
evaluate tiny results against numerical error separately. Record maxima and
failed identities. Scientific tolerances are separately documented by rule.

Build and score graphs sequentially to control memory. Measure a small pilot
before estimating campaign runtime. Keep full inventories, factorization
artifacts and matrices private. No Monte Carlo is needed to establish that a
resource is absent or carbon is misclassified. Any later uncertainty study
must refactor when sampled technosphere coefficients change.

6. Apply independent physical checks
------------------------------------

Carbon origin and conservation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For every applicable combustion process:

* Identify the actual fuel input and follow its linked market and production
  suppliers. Check delivered mass/energy basis and carbon content. Derive
  origin-weighted carbon shares using carbon quantities, not GWP contributions.
* Distinguish petroleum/coal/natural-gas carbon, biogenic carbon, atmospheric
  captured carbon and reused fossil CO2. Synthetic fuel is not automatically
  assigned biogenic origin. Flag any classification or credit that conflicts
  with the documented accounting convention.
* Independently estimate oxidized carbon using documented composition,
  oxidation fractions, residual products, capture and pollutant carbon.
  Compare expected fossil/non-fossil emissions separately with written flows.
* For CCS/CCU, account for captured/transferred carbon and residual emissions
  once. Preserve capture services and negative exchanges as separate evidence.
* Detect positive petroleum-kerosene demand paired with almost entirely
  non-fossil combustion CO2, even when total CO2 remains correct.
* Check carrier/biomass-stock uptake and release for duplicated or missing
  entries, with explicit temporal and boundary qualifications.

Do not call the same reclassification function, or merely trust its stored
fuel-property labels, to calculate the expected answer. Use those labels as
one evidence source alongside supplier identities, source recipes and
independently recorded carbon-content contracts. If a branch is unresolved,
report the unresolved carbon share rather than treating it as non-fossil.

Avoid universal mass-balance assertions on already allocated multi-output
recipes. Reconstruct the appropriate joint-process or declared attribution
boundary, or mark the physical ledger incomplete.

Metal and resource completeness
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For applicable primary producers, check that their output element is supplied
by direct extraction or an upstream primary-material route. Trace the complete
relevant branch, including cycles, instead of accepting any tiny extraction
from unrelated reagents or capital equipment as sufficient coverage.

Compare known metal content with the declared attribution contract. Examples:
1 kg Ir per kg supplied in the current custom proxy; 0.187852 kg Li per kg
primary Li2CO3 at the approved carbonate boundary; stoichiometric elemental
fractions for supported pure oxides. Distinguish these content contracts from
measurements of ore consumption or recovery efficiency.

Detect missing, duplicated and displaced extraction, wrong element mappings,
metal/compound/mineral confusion, and quantities accidentally multiplied by
economic allocation factors. Check primary/secondary market fractions and
whether primary producer reachability is consistent with those fractions.

An absent direct extraction flow on a downstream metal user is normal when
the supplier already represents it. Recycled supply can legitimately carry
little virgin extraction under its system model. Do not impose a universal
1 kg extraction/kg output rule on all materials, alloys, compounds, markets,
co-products or consequential credited systems.

Water checks
~~~~~~~~~~~~

Check m3/kg conversions, moisture and concentration bases, omitted supplier
withdrawals, inconsistent evaporation/discharge, and duplicate water inputs
relative to an explicit process budget. Check whether reported electricity,
hydrogen, chemicals or metal proxies omit the production stage that would
carry their water extraction.

Use strict balance checks only for inventories with a sufficiently complete
water ledger. Incomplete returns, unspecified origin or unmodelled reaction
water receive an evidence-gap result, not an invented closure correction.
High water withdrawal per kg product is a plausibility trigger, not an error
without contextual evidence.

Land checks
~~~~~~~~~~~

Check land-dependent agriculture, forestry, mining and energy infrastructure
for plausible land routes under the stated boundary. Review gross and net
scores separately where credits cancel occupation. Use reference area,
yield, service lifetime and output to verify dimensional relationships only
when those inputs are documented.

Prioritize m2 versus m2 year mistakes, ha-to-m2 conversion, duplicated
infrastructure, missing lifetime normalization and material-volume mistakes
that produce large upstream forestry burdens. Missing direct land on a
consumer is not an error if it is carried by infrastructure or upstream
suppliers. A changed land-use burden from a different fuel blend is a real
model-boundary change requiring explanation, not automatic evidence of a bug.

Cross-cutting checks
~~~~~~~~~~~~~~~~~~~~

Check kg/g/mg, MJ/kWh, m3/kg, kW/MW, nominal capacity versus lifetime service,
production coefficients, dry/wet mass, component duplication and treatment
signs. Preserve native product and reference-output conventions.

For each physical rule define applicability, independent expectation, unit,
boundary, tolerance, provenance, exceptions and unresolved dependencies before
ranking failures. Record checked/applicable/not-applicable/unresolved counts.
An applicability rate of zero is not a successful audit.

7. Interpret scenario and comparison results
--------------------------------------------

Compare the primary graph with the native source, current 2020/2030 graphs,
pathway contrasts, matched technologies/geographies and documented source
contracts. Reuse the released 2.5.5 comparison only as historical diagnostic
evidence. Neither the old release nor native ecoinvent is universal ground
truth for a new custom recipe.

Use relative changes above 20%, order-of-magnitude changes, new zeros,
zero-to-nonzero results, sign changes and unusually flat trajectories as
investigation triggers. Also examine absolute changes within comparable
reference units. Do not divide by zero, aggregate heterogeneous functional
units, or rank only the largest percentages of trace quantities.

There is no requirement that decarbonization reduce land, water or metal use,
nor that every indicator decrease monotonically with year. Changes in biomass,
electrolysis, storage or infrastructure can legitimately increase resource
demand. Explain the supplier/quantity mechanism before assigning a verdict.

8. Verify detector sensitivity with deliberate faults
------------------------------------------------------

Create private in-memory or disposable copies containing faults. Never alter
the reference graph or workbook during these experiments. Rebuild the affected
matrix and its factorization when the perturbation changes the matrix.

Required faults:

* Move petroleum kerosene's fossil CO2 to non-fossil CO2 while preserving total
  emissions. The carbon-origin detector must flag the case independently of GWP.
* Remove the custom iridium supplier's extraction flow while retaining its
  production, energy and chemical inputs. The primary-resource detector must
  flag missing coverage even when GWP is unchanged.
* Duplicate extraction under an explicit one-kg content attribution contract.
* Substitute an unrecognized flow code, wrong compartment or wrong unit while
  leaving the human-readable name plausible.
* Disconnect a primary supplier while retaining an unused mining dataset in
  the database. Flow presence alone must not pass the reachability check.
* Introduce a 1,000-fold water conversion error and a land unit/lifetime error
  in inventories with explicit reference budgets.
* Insert a duplicate component or a treatment-sign error in an applicable
  documented inventory.
* Reintroduce a static native hydrogen market in a market-average recipe that
  explicitly adopts regional scenario supply.

Also run negative controls: a fully biogenic fuel, legitimately upstream-only
resource supply, a secondary-metal route, a negative-output treatment process,
a documented co-product allocation, and a proxy explicitly missing a complete
water ledger. These should remain valid or unresolved according to their
contract; they must not be falsely forced into primary-resource or closed-
balance assumptions.

All historical target faults and deterministic contract faults must be caught.
Record detection rates separately from the yield of real-world flags. Broad
plausibility heuristics are evaluated for useful precision, not labelled
complete because a few synthetic mutations pass.

9. Triage findings and inspect unflagged controls
-------------------------------------------------

Each finding contains the activity identity, functional unit, graph/version,
indicator, observed value, expected range or qualitative expectation, rule,
applicability, evidence, relevant supplier path, uncertainty and confidence.

Use separate categories:

* Confirmed implementation/import/export defect.
* Missing inventory or characterization coverage.
* Scientifically questionable assumption or proxy.
* Documented attribution, allocation or boundary change.
* Numerical issue.
* Unresolved because the evidence is insufficient.

Rank by confidence, normalized consequence within a comparable product family,
and downstream reach. A missing resource can be high priority even when it
contributes nothing to GWP. Do not label every large relative change a bug.

Trace each high-priority result to the first explanatory supplier or process,
then to its workbook and transformation step. Display selected contributions,
their accounted fraction and the residual; disclose traversal limits and
unexplored branches. Cluster downstream flags by common upstream cause, so
1,886 affected activities do not become 1,886 independent scientific cases.

Manually examine every confirmed high-severity defect and leading unresolved
cases from each indicator family. Independently review at least 100 randomly
selected unflagged activities, balanced across the main sectors, using source
recipes and supply chains. This is a check on blind spots in the detector;
it does not establish a zero false-negative rate. Add newly discovered failure
types to the rules and mutation panel.

Bring corrections that change scientific assumptions, recovery yields,
allocation or proxy choices for review with their supporting evidence. This
plan authorizes no automatic rewriting of inventory values or LCIA references.

10. Implementation and outputs
-------------------------------

Proposed maintained files:

* ``benchmarks/physical_inventory_audit/run.py``: orchestration, scenario
  manifest, numerical checks and resumable scoring.
* ``benchmarks/physical_inventory_audit/sampling.py``: sampling panels and
  reproducible identity matching.
* ``benchmarks/physical_inventory_audit/indicators.py``: explicit indicator
  definitions, method inventory and coverage.
* ``benchmarks/physical_inventory_audit/carbon.py`` and ``resources.py``:
  independent physical rules and supply-chain evidence.
* ``benchmarks/physical_inventory_audit/water_land.py``: dimensional checks
  and boundary-aware water/land ledgers.
* ``benchmarks/physical_inventory_audit/report.py``: triage, clusters and
  standalone report/visualizations.
* ``benchmarks/data/physical_inventory_audit.yaml``: scenario panel, sampling
  quotas, seeds, triage settings and explicit exceptions.
* ``tests/data/physical_audit_contracts.yaml``: sourced quantities, applicability
  and model-specific physical contracts.
* ``tests/test_physical_inventory_audit.py`` and
  ``tests/test_physical_audit_mutations.py``: meaningful rule, negative-control
  and historical-fault tests without workbook hash expectations.

These paths are proposed; implementation may consolidate small modules once
their actual responsibilities are clear. Reuse existing graph validation and
provenance types. Integrate new checks into default premise validation only
after their applicability and false-positive behavior are established.

Write a new dated result directory, preserving all earlier audit results:

* ``manifest.json`` and ``sample.csv``: build/method provenance and sampling.
* ``scores.parquet`` plus convenient per-family CSV tables: paired/physical
  scores and signs with exact units. Full detailed inventories remain private.
* ``coverage.csv`` and ``rule-coverage.csv``: indicator and physical-rule scope.
* ``findings.csv`` and ``root-causes.csv``: evidence, confidence, affected
  activities, status and proposed remedies.
* ``selected-contributions.csv``: selected explanatory chains and residuals.
* ``mutation-validation.json`` and ``numerical-validation.json``: independent
  detector and calculation checks.
* A standalone searchable HTML report with activity, sector, indicator,
  scenario, issue-type and confidence filters; paired-score plots, zero-event
  markers and contribution views. PDF/PNG figures support sharing.
* A final report distinguishing measured quantities, declared attribution
  conventions, qualified proxies and unresolved scientific questions.

Only metadata, aggregate results and selected diagnostic contributions are
eligible for sharing. Licensed databases, complete LCI exports and matrices
stay in the isolated private project.

11. Delivery sequence and completion criteria
----------------------------------------------

Deliver in this order:

1. Freeze inputs, define indicators/contracts, inventory existing checks, and
   implement the historical-fault and negative-control panel.
2. Pilot approximately 200 activities in the primary 2050 graph. Establish
   numerical agreement, coverage reporting, useful diagnostics and actual
   runtime before expanding.
3. Run whole-graph screening and the 5,000-activity primary audit. Present
   the first ranked findings and root-cause clusters.
4. Apply the same panel to native/2020/2030 controls, then the 1,000-activity
   stress panels. Investigate version-sensitive failures with 3.8/3.11 probes.
5. Review leading findings and 100 unflagged controls. Record decisions and
   proposed fixes; separately implement approved changes and rebuild affected
   graphs before making before/after claims.
6. Promote confirmed defect mechanisms into semantic regression and mutation
   tests. Establish a small deterministic PR audit and a rotating broader
   scheduled campaign only after measuring reliability and cost. Scheduling
   or CI changes are implementation deliverables, not performed by this plan.

Completion of the first campaign requires reproducible sampling, coverage
counts for every rule and indicator, validated numerical calculations, all
specified deterministic historical faults detected, contextual negative
controls retained, and every high-priority finding either resolved or
explicitly documented as an open scientific/data issue. A zero error count
alone is not a completion criterion. The report states the limits of what
was checked; it does not claim universal scientific correctness.

Methodological references
-------------------------

* `Brightway matrix construction
  <https://docs.brightway.dev/en/latest/content/overview/matrix.html>`_:
  product/activity mappings, non-normalized production and matrix signs.
* `ecoinvent impact assessment
  <https://support.ecoinvent.org/impact-assessment>`_ and the installed
  version-specific inventory-indicator CF tables: distinguish physical
  inventory quantities from characterized environmental impacts.
* `ecoinvent water supply
  <https://support.ecoinvent.org/water-supply>`_: extraction and treatment
  can be represented at the water-production supplier.
* `ecoinvent glossary W
  <https://support.ecoinvent.org/glossary-w>`_: distinguish withdrawal,
  water balances and related inventory concepts.
* `LCA wiki: life cycle interpretation
  <https://github.com/sentier-dev/lca-wiki/blob/main/core/concepts/interpretation.md>`_
  and `allocation and multifunctionality
  <https://github.com/sentier-dev/lca-wiki/blob/main/core/concepts/allocation-and-multifunctionality.md>`_:
  consulted as methodological summaries, with source IDs ``ilcd-2010`` and
  ``iso-14044-2006``. The original standards were not independently checked
  for this plan. The local wiki clone was unavailable; the public pages were
  consulted. No claim of formal standards compliance is made.
