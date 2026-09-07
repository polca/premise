Generating change reports
===========================

*premise* compares the normalized source inventory with each certified,
pre-export scenario. Database exports automatically create a compact Excel
workbook and an exhaustive Parquet audit when ``generate_reports=True``.
The workbook contains review-oriented summaries, key numeric changes, market
and proxy information, and validation findings; raw exchange vectors remain in
Parquet.

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
     - Did the supplier vector change as expected?
     - Trace technology shares separately from changes inside each supplier.
   * - Fallbacks & Proxies
     - Where were substitute inventories used?
     - Assess geographical and technological representativeness.

For example, a lower material input per kWh in a PV activity may result from
higher yield or efficiency, not a lower material requirement per square metre.
Match activity identity and scenario before following its exchange details in
Parquet. Workbook rankings are review aids, not rankings of LCIA contributions.
See :doc:`interpreting-results` for an archived PV example and a numerical
sensitivity exercise. Exact fields and remaining sheets are described in
:doc:`/reference/change-report-schema`.

Generate a report explicitly
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
files are never overwritten. The report schema version is ``2``. The former
pipe-delimited log workbook was removed; historical log files are neither
imported nor backfilled. See :doc:`/reference/change-report-schema` for the workbook
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
