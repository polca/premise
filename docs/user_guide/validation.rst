Validation reports
====================

Validation checks whether the generated inventory meets the implemented
structural and methodological rules. A passing report does not independently
validate source measurements, the IAM scenario or every scientific assumption.

Check a completed update
--------------------------

After the setup and update in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   report = ndb.get_validation_report(scenario=0)
   report.raise_for_errors()
   for issue in report.warnings:
       print(issue.rule_id, issue.message, issue.activity_key)

An unsuppressed error stops the update before checkpointing or export.
Warnings allow completion but require interpretation. Suppressed findings
remain visible with their explanation; suppression is not evidence that the
underlying assumption has been scientifically validated.

Find and interpret an issue
-----------------------------

.. code-block:: python

   for phase in report.phase_results:
       for rule in phase.rule_results:
           for issue in rule.issues:
               print(issue.severity, issue.rule_id, issue.message)
               print(issue.activity_key, issue.exchange_id)
               print("Expected:", issue.expected, "Actual:", issue.actual)

Use the activity identity and exchange identifier to locate the record through
:doc:`inventories`. Review its reference product, unit, supplier and amount.
If an imported exchange has the wrong unit or no provider, fix the originating
inventory or mapping and rebuild; do not merely silence the finding.

Worked finding examples
-------------------------

The executable :download:`validation_findings.py </examples/validation_findings.py>`
constructs illustrative findings using the public report types. Its rule IDs
are prefixed ``example.``; they are teaching examples, not generated sector
rule IDs or evidence of a defect in your database.

.. literalinclude:: /examples/validation_findings.py
   :language: python

The warning asks the reader to assess geographical representativeness. The
error indicates an invalid amount that must be corrected before calculation.
Use the actual rule ID in your report when investigating a production failure.

When to request exhaustive checks
-----------------------------------

.. code-block:: python

   exhaustive = ndb.get_validation_report(scenario=0, exhaustive=True)
   exhaustive.raise_for_errors()

The exhaustive diagnostic traverses the complete activity/exchange graph. It
is useful after custom inventory edits or when targeted checks do not explain
a suspicious result. Passing it still cannot establish that a supplier is
the best scientific proxy.

Read :doc:`reports` to review numerical changes,
:doc:`/methodology/validation` for uncertainty, and
:doc:`/development/validation-internals` for certificate persistence and
exporter internals.
