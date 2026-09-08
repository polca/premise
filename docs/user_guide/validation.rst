Validation reports
====================

Premise checks inventories automatically when it updates and exports a
database. The :doc:`change-report workbook <reports>` includes the results
in its **Validation Findings** and **Validation Coverage** sheets. Start
there; a separate Python command is only needed to inspect the results
programmatically or request additional checks.

Check a completed update
--------------------------

Read **Validation Findings** for the explanation of each issue and the
affected activity. **Validation Coverage** shows which checks were applied.

* **Errors** stop an update or export unless a documented exception applies.
  Correct the cause before using the database.
* **Warnings** allow the operation to finish. Review whether the assumption
  or data limitation matters for your study.
* **Suppressed findings** are issues covered by a documented exception.
  They remain visible with an explanation so you can assess that exception.

A passing result means the inventory passed the checks that applied. It
does not confirm every source measurement or scientific assumption, or
establish that an IAM scenario will occur.

Find and interpret an issue
-----------------------------

1. Read the message and identify the affected activity and scenario.
2. Check its product, unit, supplier and input or emission amount. Where
   relevant, compare the change with the **Key Changes** or **Market Changes**
   sheet.
3. If the problem comes from an inventory or mapping you supplied, correct
   that input and rebuild. For an unexplained issue, keep the report and
   build settings when asking for help; see :doc:`troubleshooting`.

Worked finding examples
-------------------------

A warning about using a supplier from another region asks you to assess
whether that supplier represents your study location. It does not necessarily
mean the calculation is invalid.

An error about an invalid exchange amount requires correction before
calculation. Inspect the activity and the input data that supplied that amount.
These are illustrative examples; use the message in your own report to
decide what to investigate.

When to request exhaustive checks
-----------------------------------

Additional checks of every activity and exchange can help after custom
inventory edits or when the usual checks do not explain a suspicious result.
They do not replace an assessment of whether the datasets represent your study.

See :doc:`/development/validation-internals` for Python commands and detailed
validation examples. For uncertainty and the limits of these checks, read
:doc:`/methodology/validation`.
