Generating change reports
===========================

.. raw:: html

   <span id="generate-a-report-explicitly"></span>


*premise* compares the normalized source inventory with each validated scenario
before export. Database exports automatically create a compact Excel
workbook and a complete record of changes in Parquet format when ``generate_reports=True``.
The workbook contains summaries, key numeric changes, market
and proxy information, and validation findings; complete exchange lists remain in
Parquet.

Start with this workbook to review both inventory changes and validation
results. You do not need to request a separate validation report for this
review.

Read the workbook in this order
---------------------------------

.. list-table:: A review sequence
   :header-rows: 1

   * - Sheet
     - Question to answer
     - Next action
   * - Overview
     - Are source, model, pathway, year and status the intended ones?
     - Correct the configuration before interpreting changes.
   * - Key Changes
     - Which reported coefficients changed most?
     - Inspect the activity and unit; a large relative change from a small baseline can be misleading.
   * - Market Changes
     - Did the supplier shares change as expected?
     - Trace technology shares separately from changes inside each supplier.
   * - Fallbacks & Proxies
     - Where were substitute inventories used?
     - Assess geographical and technological representativeness.
   * - Validation Findings
     - Were any errors or warnings reported?
     - Read the explanation and inspect the affected activity. See :doc:`validation` for guidance.
   * - Validation Coverage
     - Which checks applied to this scenario?
     - Check what was tested before interpreting a passing result.

For example, a lower material input per kWh in a PV activity may result from
higher yield or efficiency, not a lower material requirement per square metre.
Match activity identity and scenario before following its exchange details in
Parquet. Workbook rankings are review aids, not rankings of LCIA contributions.
See :doc:`interpreting-results` for guidance on tracing score changes and
assessing uncertainty. Exact fields and remaining sheets are described in
:doc:`/reference/change-report-schema`.

Generate a report on request
------------------------------

Reports can also be generated immediately after ``update()``, including when
automatic reports were disabled:

.. code-block:: python

    ndb = NewDatabase(..., generate_reports=False)
    ndb.update()
    artifacts = ndb.generate_change_report(
        filepath="review/reports",
        name="ssp2-review.xlsx",
    )
    print(artifacts.workbook_path)
    print(artifacts.details_path)

``generate_change_report()`` returns an immutable ``ChangeReportArtifacts``
object. Filenames contain a UTC timestamp and the build ID and existing report
files are never overwritten. The report schema version is ``2``. See :doc:`/reference/change-report-schema` for the workbook
and Parquet schemas.

Scenario-variable summary
---------------------------

A scenario report summarizes the IAM variables used in the build. It is
separate from the inventory change report and validation certificate:

.. code-block:: python

    ndb.generate_scenario_report()

Compare :doc:`/reference/models` and :doc:`/reference/scenarios` when
interpreting different model/pathway results.

Verified report excerpt
-------------------------

The completed documentation run's Overview records status ``passed``,
ecoinvent ``3.12``, system model ``cutoff``, Premise ``2.5.1`` and report
schema ``2``. This verifies the reporting workflow, not the correctness of
all source assumptions. The workbook also includes Scenario Summary, Sector
Summary, Validation Findings, Validation Coverage and Methodology sheets.
