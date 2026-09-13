Change-report schema and lifecycle
====================================

``NewDatabase.generate_change_report()`` reports the exact difference between
the normalized source inventory and each validated scenario in that
``NewDatabase`` instance, before export. Reporting is read-only: exporter database
names, storage IDs, validation caches, ``_``-prefixed runtime fields, and
``log parameters`` are excluded.

.. code-block:: python

    ndb = premise.NewDatabase(..., generate_reports=False)
    ndb.update(persist=True)
    artifacts = ndb.generate_change_report(
        filepath="export/change reports",
        name="review.xlsx",
    )

The returned ``ChangeReportArtifacts`` object cannot be modified. It contains ``report_id``,
``status``, ``workbook_path``, ``details_path``, ordered
``scenario_identities``, the source fingerprint, and validation certificate
keys. Calling the method before ``update()`` raises ``RuntimeError``.
``generate_reports=False`` disables automatic success and failure reports, but
never disables this explicit method.

Lifecycle and failures
------------------------

Successful Brightway, SimaPro, openLCA, matrix, datapackage, superstructure,
and scenario-array exports add their exporter validation phase before the
workbook is finalized. The complete Parquet comparison is cached by report schema,
source fingerprint, scenario certificate keys, and store generations; later
exporters reuse it and refresh only the workbook.

If inventory or export-format validation fails while reports are enabled, premise
attempts to create a workbook with status ``failed`` from the invalid read-only
store. The original ``PremiseValidationError`` remains the raised exception and
its ``artifacts`` property points to the diagnostic files. A secondary reporting
failure is logged and does not replace the validation or export result.

Excel workbook
----------------

The workbook summarizes the changes. It contains:

* **Overview** -- identities, versions, ruleset, fingerprints, status, counts,
  and a relative link to Parquet.
* **Scenario Summary** and **Sector Summary** -- unique changed-object counts.
* **Key Changes** -- the 20 largest numeric changes per scenario, sector,
  object type, and change type.
* **Market Changes** and **Fallbacks & Proxies** -- supplier shares and
  geographical substitutions.
* **Validation Findings** and **Validation Coverage** -- unsuppressed findings,
  successful rules, applicability, and documented suppressions.
* **Methodology** -- transformation algorithms, IAM/configuration references,
  normalization actions, reasons for data-source choices, certificate keys, and the
  rule-specific target values used by material updates or preserve decisions.

Every tabular sheet uses an Excel table with filters, frozen headers, wrapped
text, and appropriate numeric and severity formatting. Complete exchange lists
and the full list of changes are stored in Parquet.

Parquet schema
----------------

Parquet metadata includes ``premise_report_schema_version=2``. Rows are sorted
by scenario, activity identity and occurrence, exchange identity and
occurrence, changed field, and change type. Columns cover:

* report, build, scenario, source/final fingerprint, and certificate identity;
* contributing transformations and sector;
* object and change type;
* complete activity and exchange identity plus a repeatable occurrence number for duplicate records;
* old/new provider identity for relinks;
* changed field and old and new values in a consistent JSON format;
* typed numeric values, absolute and relative differences, and unit; and
* reason code, explanation, IAM variable, algorithm, configuration reference,
  proxy, and fallback rank.

Change types distinguish activity and exchange additions/removals, field and
uncertainty modifications, amount changes, and supplier relinks. Multiple
contributing transformations are retained. Differences that cannot be linked to recorded transformations or their
validated scope are labelled
``unattributed``.
