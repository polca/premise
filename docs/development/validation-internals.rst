Validation certificates and checkpoints
=========================================

Every completed scenario update receives a saved validation result, called a
validation certificate. It records the inventory and rules used for the checks.
Incremental sector phases and their production coverage phase are persisted
with the scenario checkpoint; exporter schema phases are held only in memory so
exports cannot rewrite a certified checkpoint.


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

The production certificate combines sector-specific checks evaluated while
the transformation still owns direct references to its changed activities.
They check coverage, finite physical values, reference production, market
composition, and sector-specific methodological expectations. Electricity and
fuel checks independently recompute consequential marginal mixes; heat,
steel, cement, biomass, metals, transport, batteries, renewables, mining,
carbon removal, final energy, emissions, and external scenarios add their
specific coverage, composition, linking, and physical-bound checks.

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
   for phase in report.phase_results:
       for rule in phase.rule_results:
           for issue in rule.issues:
               print(issue.severity, issue.rule_id, issue.message)
               print(issue.activity_key, issue.exchange_id)
               print("Expected:", issue.expected, "Actual:", issue.actual)

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
