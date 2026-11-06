Physical inventory audits
==========================

Use the physical audit after an update when numerical LCIA tests do not explain
a suspicious result. It checks carbon origin, natural-resource attribution,
flow identities, independent quantity contracts and numerical consistency.
It does not change the inventory, repair exchanges or refresh expected scores.

The audit samples 5,000 activities by default: 60% uniformly, 30% across
sector/reference-unit strata and 10% from a risk-ranked remainder. All panels
are disjoint. Physical rules also examine the complete graph. Only the uniform
panel supports an ordinary random-sample flag rate; flags are not established
error rates.

Audit an updated premise inventory
-----------------------------------

.. code-block:: python

    from premise import AuditConfig, audit_database

    # ndb is an existing NewDatabase after ndb.update().
    store = ndb.get_inventory_store(0)
    report = audit_database(
        store,
        config=AuditConfig(
            sample_size=5000,
            seed=20261101,
            version="3.12",
            system_model="cutoff",
        ),
        provenance={"model": "remind", "pathway": "SSP2-PkBudg1000", "year": 2050},
    )
    report.write("results/my-new-physical-audit")

Lists of complete linked inventory dictionaries are also accepted. Suppliers
must be present: a missing or ambiguous provider blocks scoring rather than
receiving zero impact. Single-reference-product allocated inventories are
supported; unresolved multi-output references are reported as structural errors.

Audit a written Brightway database
-----------------------------------

.. code-block:: python

    import bw2data as bd
    from premise import AuditConfig, audit_brightway_database

    bd.projects.set_current("my-existing-project")
    report = audit_brightway_database(
        "my-generated-database",
        config=AuditConfig(sample_size=5000, version="3.12", system_model="cutoff"),
        crosscheck_size=250,
    )
    print(report.has_errors)
    for finding in report.findings:
        print(finding.rule_id, finding.confidence, finding.issue.message)
    report.write("results/my-written-database-audit")

The caller selects the project; the function does not change it. Actual linked
dependencies and biosphere metadata are read automatically. The adapter uses
relational supplier links, including exports whose serialized link metadata is
stale. It scores Brightway's processed coefficients and records differences
from nominal exchange quantities. Differences beyond a documented float32
rounding envelope are findings, not silently accepted corrections.
Native imports may name intermediate exchanges after the product rather than
the activity. The adapter accepts both descriptions of an explicit supplier
link and records the product-name count; unrelated names, units or products
remain findings.

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

For a native control, set ``include_default_contracts=False``. This avoids
applying a Premise-specific resource convention to an unmodified native source.
Record its version and system model as for a generated graph.

Read the results
-----------------

``report.findings`` distinguishes structural errors, carbon-classification
contradictions, exact physical-contract violations, numerical disagreements,
and coverage/plausibility warnings. Each finding includes a standard
``ValidationIssue``, confidence, activity identity and supporting evidence.

``report.has_errors`` means an error-level finding exists. Its inverse does
not establish full scientific correctness. Review ``report.rule_coverage``
for unresolved or inapplicable rules and ``report.coverage`` for missing or
unused indicator flows. Unknown carbon origin remains unknown, and an
uncharacterized or disconnected resource is not interpreted as zero resource
use.

The default indicator set includes energy-resource extraction, physical fossil/non-fossil/stock CO2,
atmospheric uptake, geological CO2, CO and methane, water withdrawals/returns,
land occupation/transformation and all 65 RELICS 0.0.6 extraction mappings.
The latter are bundled under their BSD-3-Clause licence; installing ``relics``
is not required. These categories include minerals and nonmetals and can
overlap. Do not sum them into a single total metals mass.

Water withdrawal is distinct from evaporation or net consumption. Cooling and
turbine water are reported separately. Land occupation uses m2 year, while
transformation uses m2; the two are never summed. Scores are per demand of
**plus one reference-product unit**. A negative treatment reference output
therefore needs explicit interpretation before describing its sign as a
benefit or a burden.

.. code-block:: python

    key = (
        "iridium production, economic allocation, proxy",
        "iridium", "ZA", "kilogram",
    )
    value = report.score(key, "metals.Iridium")
    direct = report.score(key, "metals.Iridium", scope="direct")

``scope="positive"`` and ``scope="negative"`` split first-tier supplier scores
and direct exchanges before cancellation. This can reveal a burden and a
credit even when their net score is zero. It is not a reconstruction of gross
physical mining throughput, and cancellation can still occur deeper within a
supplier. Selected explanations include upstream process contributions,
first-tier drivers and the residual outside the selected process list.

Saved artifacts include ``audit.json``, ``scores.csv``, ``scores.npz``, sample,
coverage, rule coverage, findings, investigation grouping and selected contribution
tables. ``report.html`` is a standalone searchable report with an indicator
selector and score plot. Output directories must be new, preserving earlier
audits. The ``root-causes.csv`` table groups shared fuel suppliers where possible;
these are investigation clusters, not proof of a common defect. Matrices and
complete inventories are not exported.

``manual-review.csv`` queues up to 100 unflagged sampled activities, balanced
across sectors, with selected carbon, copper, water and land supplier traces.
They are explicitly marked ``not_reviewed``. This queue supports a human check
for detector blind spots and does not claim that those activities are correct.

.. code-block:: python

    from premise import AuditReport, compare_audits

    earlier = AuditReport.read("results/earlier-audit")
    later = AuditReport.read("results/later-audit")
    changes = compare_audits(earlier, later, threshold_percent=20)

Comparison requires matching functional identities and indicator definitions.
It preserves unmatched identities, sign changes and zero-to-nonzero events.
Percentages for zero baselines are undefined. A comparison trigger is evidence
to investigate, not proof of a defect.

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

Broader scientific review, the native/2020/2030/2050 panel and cross-model
stress cases are described in the :doc:`implementation plan
</development/physical-inventory-probing-plan>`. A passing detector test does
not certify every inventory in a generated database. Review findings and
source limitations before changing scientific assumptions or coefficients.
