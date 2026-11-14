Physical inventory audits
==========================

These optional checks help investigate a suspicious result after you have
reviewed the normal :doc:`validation report <validation>` and
:doc:`inventory changes <reports>`. The audit checks carbon origin, resource
flows, supplier identities, independently specified quantities and numerical
consistency.
It does not change the inventory, repair exchanges or refresh expected scores.

By default, the audit samples 5,000 activities and also checks rules across
the complete supply network. Review the reported findings and coverage;
a flagged result is a prompt to investigate, rather than proof of an error.

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
        provenance={"model": "remind", "pathway": "SSP2-NPi", "year": 2025},
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

A separate Brightway calculation checks up to 250 sampled demands. Numerical
disagreements remain findings for investigation. No LCIA methods or databases
are registered, although Brightway can refresh its processed caches. See
:ref:`physical-audit-numerics` for solver choices and numerical details.

For a native control, set ``include_default_contracts=False``. This avoids
applying a Premise-specific resource convention to an unmodified native source.
Record its version and system model as for a generated graph.

.. raw:: html

   <span id="command-line-and-pytest"></span>
   <span id="independent-scientific-contracts"></span>
   <span id="run-a-matched-campaign"></span>

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

To define your own physical rules, compare several databases with the same
sample, or use the command line, see :ref:`physical-audit-development`.
These are optional extensions to the checks above.
