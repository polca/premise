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
