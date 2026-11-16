Validation certificates and checkpoints
=========================================

Every completed scenario update receives a saved validation result, called a
validation certificate. It records the inventory and rules used for the checks.
Incremental sector phases and their production coverage phase are persisted
with the scenario checkpoint; exporter schema phases are held only in memory so
exports cannot rewrite a certified checkpoint.

Combined source checkpoints include the fingerprints of the native source,
prepared inventories and both metadata caches in their filenames. A refreshed
inventory therefore creates a distinct base; scenario deltas saved earlier keep
their original base reference. Unchanged cache fingerprints reuse the existing
checkpoint, and a writer lock prevents concurrent builds from replacing it.
Checkpoints using the older fixed filename are left intact rather than migrated
or overwritten. This preserves valid older scenarios; it does not reconstruct a
base that was already overwritten before this change.


.. code-block:: python

   report = ndb.get_validation_report(scenario=0)
   report.raise_for_errors()

   # Run the complete graph diagnostic explicitly when needed.
   exhaustive = ndb.get_validation_report(scenario=0, exhaustive=True)

   for phase in report.phase_results:
       checked = sum(result.checked_object_count for result in phase.rule_results)
       print(phase.phase_id, checked, phase.elapsed_seconds)


Validation is read-only and cannot be disabled through the public API.
Unsuppressed errors stop an update before checkpointing or export; warnings and
narrow, versioned suppressions remain visible in the immutable report.

Exhaustive graph validation stages
----------------------------------

``InventoryGraphValidator.validate()`` coordinates the exhaustive checks in
``premise/validation/validation_framework.py``:

1. Index activity metadata once. The private index holds the original read-only
   payload views and provider lookup tables, without copying the inventory.
2. Resolve the declared targets and cardinality. Unaffected activities remain
   available as suppliers and, when requested, enter the scope fingerprint check.
3. Traverse exchange storage once. Activity and exchange checks share parsed
   fields; provider checks resolve explicit links before semantic fallbacks.
   Market-share sign checks wait for all reference-production rows.
4. Check the accumulated supplier vectors, scope, coverage, algorithm and cycles.
5. Apply suppressions and construct the immutable report in the existing rule
   order. A non-applicable intent returns its sector phase without exchange checks.

Certificate lookup remains outside this traversal. Reusing a certificate for the
same generation and validation inputs performs no graph scan. Refactoring these
stages preserves the ruleset version and certificate identity; a change to the
meaning of a check requires the corresponding ruleset update.

Imported rechargeable-battery classifications
---------------------------------------------

Reviewed rechargeable cells, cell-containing packs and their product markets
use ISIC Rev.4 ``2720: Manufacture of batteries and accumulators`` and
`CPC 2.1 46420: Electric accumulators
<https://unstats.un.org/unsd/classifications/Econ/Structure/Detail/EN/1074/46420>`_.
The former CPC 46410 label describes primary cells and primary batteries.
The explicit activity/product entries in ``classifications.csv`` include raw
workbook identities, prepared aliases and cell products bundled with end-of-life
provision. Packs without electronics or central controls still contain cells.

The fifteen explicitly reviewed non-cell pack-component provision products use
ISIC 2720 and `CPC 2.1 46430: Battery parts
<https://unstats.un.org/unsd/classifications/Econ/Structure/Detail/EN/1074/46430>`_.
These recipes express fixed pack hardware provision per nominal kWh of installed
capacity. The kWh reference, supplier quantities, controls boundary and bundled
end-of-life provision are retained unchanged.

Capacity-labelled markets which provision battery hardware per nominal installed
kWh also use CPC 46420. This classification does not convert their reference to
kilograms, operating electricity or lifetime delivered electricity. The separate
chemicals, electronics, parts and treatment activities use their own product or
service categories.

These reviews change classification metadata only. No general name-based battery
fallback is applied. Changes to the classification table invalidate prepared import caches,
including retained CLIC workbooks; native ecoinvent source caches are preserved.

Other reviewed import classifications
-------------------------------------

``premise/data/utils/import/classification_review.yaml`` records the review of
the 221 distinct missing activity/product pairs observed in default ecoinvent
3.12 cutoff / IMAGE SSP2-M imports, plus related legacy aliases and rechargeable
module/capacity labels. It stores each group's CPC and ISIC labels, rationale,
exact activity/product identities, source workbooks and original units. Source
URLs and checksums refer to the UN CPC 2.1 structure, its updated explanatory
notes and CPC/ISIC correspondence, and the ISIC Rev.4 structure. The existing
mapping supplied relevant product precedents; erroneous primary-battery, ore,
construction or chemical labels were not copied to new products.

Runtime loading continues to use explicit rows in ``classifications.csv``.
The YAML file is review provenance, not an additional matching mechanism.
Populated control boards are distinguished from bare printed circuits; discrete
power devices from integrated circuits; finished hardware from processing and
waste-treatment services; and cell-free PV assemblies from complete PV devices.
For the populated control boards and electronic connector, ISIC 2610's explicit
activity definitions are used even though the generic CPC correspondence lists
2710 for their control/connection product categories. This choice is recorded
in those groups rather than treating the correspondence as an exact equivalence.

Parent categories are retained where mixed equipment bundles, service allocation
references or undocumented delivery forms do not support a narrower subclass.
Composite pressure-vessel mappings follow the declared reinforced-polymer
construction. Waste-service categories do not establish hazard status,
destination or recovery rates. None of these metadata
changes validates or alters the underlying engineering recipes, replacement
budgets, signed waste exchanges, allocation denominators or supplier links.
Unreviewed activity/product pairs continue to produce missing-classification
diagnostics.

Validation certificate scope
----------------------------

Certification is mandatory for scenario updates and exports. Internal
certification methods return a certificate or report, or raise a validation
error. ``raise_on_error=False`` retains findings for diagnostics while still
running the checks. Unit tests of unrelated storage and export mechanics use
explicit test substitutes rather than a runtime validation-disable flag.

Missing, malformed, and outdated cached report payloads are recovered through
certification. A matching store certificate can be reused during recovery.
Export phases remain in memory for reporting and are excluded from the
semantic report persisted with the scenario checkpoint.

The production certificate combines sector-specific checks evaluated while
the transformation still owns direct references to its changed activities.
They check coverage, finite physical values, reference production, market
composition, and sector-specific methodological expectations. Electricity and
fuel checks independently recompute consequential marginal mixes; heat,
steel, cement, biomass, metals, transport, batteries, renewables, mining,
carbon removal, final energy, emissions, and external scenarios add their
specific coverage, composition, linking, and physical-bound checks.

Car and truck exhaust checks run again at scenario certification, after all
requested transformations, including the final GAINS emissions update. The
``sector:car:final_exhaust`` and ``sector:truck:final_exhaust`` phases replace
only the pollutant rules previously evaluated in ``sector:car`` and
``sector:truck``. Fuel-carbon, energy, fleet and linking results remain active.
Successful final checks explicitly replace earlier warnings too. Validation
reads the final inventory without changing any exchanges.

``report.issues``, ``report.warnings`` and ``report.errors`` reflect these
current results. Original findings remain in ``report.phase_results`` as
sector-stage history, with explicit phase/rule replacement references in the
final phases' ``supersedes`` field. Change-report Validation Findings uses the
current results; Validation Coverage identifies historical rules in its
``superseded by`` column. Checkpoint serialization preserves both stages.

The exhaustive diagnostic additionally covers every activity and exchange:
required fields and exchange types, uncertainty, provider identity and
product/unit agreement, geographic fallback, exact duplicate suppliers, stale
links, declared transformation scope, the expected number of matching targets, and newly introduced
cycles. It runs automatically when sector coverage is unavailable or a
certified store generation changes, and explicitly when
``get_validation_report(exhaustive=True)`` is requested.

Source normalization happens once when its compact checkpoint is created.
Fast Brightway export assigns only required database and input identifiers and
lets the bounded writer serialize compatible NumPy scalars. Other exporters
retain their format-specific normalization. Validation itself is read-only. A
certificate key includes the store generation, scenario, source and IAM
identities, system model, ecoinvent version, and validation ruleset version.
Changing the store or ruleset therefore requires validation to run again. Brightway,
SimaPro, openLCA,
datapackage, and superstructure exports reuse the inventory validation result and add
format checks as they write the data.

Standard exporters share ``NewDatabase._prepare_scenario_for_export()`` for
runtime inventory loading, preparation, failure diagnostics, and export phase
recording. Callers certify before choosing an export path and own writing and
cleanup. Brightway's fast preparation and the final superstructure union checks
remain separate because they operate on different runtime inventory layouts.

``NewDatabase.get_validation_report()`` and the immutable ``ValidationIssue``,
``ValidationRuleResult``, ``ValidationPhaseResult``, ``ValidationReport``, and
``PremiseValidationError`` types are exported from ``premise``.
``ChangeReportArtifacts`` is likewise exported for the structured V2
Excel/Parquet report returned by ``NewDatabase.generate_change_report()``.
Existing constructor and export signatures remain accepted, but integrations
that read or mutate ``NewDatabase.database`` must migrate to the store API or
a copy obtained with ``materialize_inventory()``.

Inspect individual findings in Python
--------------------------------------

After updating a scenario, inspect its checks and individual findings:

.. code-block:: python

   report = ndb.get_validation_report(scenario=0)
   report.raise_for_errors()
   for issue in report.issues:
       print(issue.severity, issue.rule_id, issue.message)
       print(issue.activity_key, issue.exchange_id)
       print("Expected:", issue.expected, "Actual:", issue.actual)

Iterate ``report.phase_results`` to inspect the full history, including earlier
exhaust findings replaced by the final checks.

The following executable example constructs illustrative findings using
public report types. Its rule IDs start with ``example.``; they are teaching
examples rather than findings from a generated database.

.. literalinclude:: /examples/validation_findings.py
   :language: python

.. _physical-audit-development:

Independent scientific contracts
---------------------------------

Default rules catch a conventional fossil fuel receiving an incompatible
CO2 classification, the current custom iridium proxy missing its one-kg
resource attribution, and market-average hydrogen bypassing an available
generated market. Mixed/unknown fuel origins, co-product attribution and
generic primary-resource coverage are treated conservatively. Broad carbon
composition and heating-value assumptions are plausibility screens, not
measured industrial carbon balances.

Gas tracing retains methane and biomethane suppliers even when a market mixes
cubic metres, kilograms and energy units. Fatty acid methyl ester inputs are
also recognized in transport. A recognized fuel name does not establish its
carbon origin: synthetic methane and ester recipes remain unresolved unless
their carbon-bearing feed can be independently traced. Missing conversion
bases remain unresolved rather than making the remaining supply appear purely
fossil.

Use ``PhysicalContract`` for a quantity whose source, functional unit,
boundary and applicable system models/versions you can establish independently:

.. code-block:: python

    from premise import PhysicalContract

    contract = PhysicalContract(
        name="my_reference_land",
        indicator="land.occupation",
        name_pattern="my documented electricity production",
        unit="kilowatt hour",
        scope="direct",
        minimum=0.05,
        maximum=0.05,
        source="Example only: 100 m2 / (20 years * 100 kWh per year)",
        system_models=("cutoff",),
        versions=("3.12",),
    )
    config = AuditConfig(contracts=(contract,), version="3.12", system_model="cutoff")

Replace the example with the applicable source data. Bounds are never inferred
from a desired historical score, and a universal one-kg resource rule is not
applied to alloys, recycled supply or economically allocated co-products.

Use ``BudgetTerm`` and ``ExchangeBudget`` for a documented exchange ledger.
Each term specifies an exact exchange type, unit, name pattern, optional
compartment and conversion coefficient. Required missing terms fail explicitly
instead of allowing an empty ledger to appear balanced:

.. code-block:: python

    from premise import BudgetTerm, ExchangeBudget

    budget = ExchangeBudget(
        name="my_complete_water_ledger",
        name_pattern="my aqueous process",
        source="My independently documented complete process-water balance",
        unit="cubic meter",
        terms=(
            BudgetTerm("market for tap water", "technosphere", "kilogram", 0.001),
            BudgetTerm("Water", "biosphere", "cubic meter", -1, categories=("water",)),
            BudgetTerm("Water", "biosphere", "cubic meter", -1, categories=("air",)),
        ),
    )
    config = AuditConfig(budgets=(budget,))

This example assumes no retained product water or reaction-water term. Include
those terms when required by the source. Do not impose closure on a partial
inventory with unspecified returns or product moisture.

Run a matched campaign
-----------------------

.. code-block:: python

    from premise import AuditCase, AuditConfig, audit_campaign

    reports = audit_campaign(
        [
            AuditCase("primary-2050", "my-2050-database", AuditConfig(sample_size=5000)),
            AuditCase("control-2030", "my-2030-database", AuditConfig(sample_size=5000)),
            AuditCase("native", "my-native-source", AuditConfig(include_default_contracts=False)),
        ],
        output="results/my-new-campaign",
    )

The first case defines the discovery sample. Later cases retain matching
identities; smaller samples are balanced across discovery panels/sectors.
Missing identities remain explicit. These matched controls do not constitute
independent uniform samples of their own databases. Databases must already
exist in the current project. The campaign runs sequentially and generates
per-case reports, comparison triggers and a campaign manifest. Completed
individual reports can be reloaded without rerunning their calculations.

Command line and pytest
-------------------------

.. code-block:: bash

    python -m premise.audit --project my-project --database my-database \
        --system-model cutoff --version 3.12 --sample-size 5000 \
        --output results/my-new-audit

``--brightway-dir`` selects an existing isolated Brightway data directory.
``--native`` disables Premise-specific contracts. ``--contracts`` accepts a
JSON list of ``PhysicalContract`` definitions; ``--budgets`` accepts ledger
definitions with nested ``BudgetTerm`` records. ``--crosscheck-size 0`` omits
the independent Brightway session. Exit status 1 indicates error-level audit
findings; the report is still written when scoring completes.

Alternatively, ``--campaign cases.json`` replaces ``--database``. Its JSON
array contains ``label``, ``database`` and optional ``config`` records matching
``AuditConfig`` fields. The first case is the discovery graph. The project and
output arguments remain required.

License-free analytical and mutation checks run in the ordinary pytest suite:

.. code-block:: bash

    pytest -q tests/test_physical_inventory_audit.py tests/test_physical_audit_brightway.py

They cover the fossil-to-non-fossil kerosene fault with unchanged total CO2,
missing/duplicated Ir, source units and compartments, upstream-only resources,
zero/credit cancellation, treatment signs, water/land budgets, scenario H2,
deterministic sampling and actual Brightway dependency/metadata behavior.
They use explicit quantities rather than pinned workbook hashes.

An additional opt-in numerical integration test uses an existing generated
database when ``PREMISE_AUDIT_DATABASE``, ``PREMISE_AUDIT_PROJECT`` and
``PREMISE_AUDIT_BRIGHTWAY_DIR`` are set. Optional version, system-model and
sample-size settings use the corresponding ``PREMISE_AUDIT_`` environment
variables. It checks graph/numerical consistency and explicit contracts;
scientific plausibility findings remain available for review in its report.

A passing detector test does not certify every inventory in a generated database. Review findings and
source limitations before changing scientific assumptions or coefficients.

Physical audit sampling
-----------------------

The audit samples 5,000 activities by default: 60% uniformly, 30% across
sector/reference-unit strata and 10% from a risk-ranked remainder. All panels
are disjoint. Physical rules also examine the complete graph. Only the uniform
panel supports an ordinary random-sample flag rate; flags are not established
error rates.


.. _physical-audit-numerics:

Physical audit numerical checks
-------------------------------

One numerical factorization serves all audit indicators and sampled demands.
The auditor balances equation and reference-unit scales with exact powers of
two before factorization, then refines residuals and maps solutions back to
the physical basis. Metadata record the solver, equilibration and refinement
count. Forward, adjoint and componentwise residual checks retain their stated
tolerances.
A separate standard Brightway session checks up to 250 sampled demands by
default, with one additional factorization. No LCIA methods or databases are
registered by the audit. Brightway can refresh its normal processed caches
when a database is marked dirty.

Solver choice matters for severely ill-scaled inventories. Native SciPy-only
Brightway calculations can disagree with refined audit results, particularly
for trace resources in large infrastructure supply chains. These disagreements
remain numerical findings. UMFPACK is recommended for investigating such cases;
install ``scikit-umfpack`` in the Brightway environment when its platform and
SuiteSparse requirements are met. The audit uses UMFPACK when available and
otherwise uses SciPy. Do not interpret a numerical disagreement alone as proof
of an inventory error.
