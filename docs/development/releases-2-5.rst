Premise 2.5 release and migration guide
=========================================

.. contents:: On this page
   :local:
   :depth: 1

Version 2.5.1 moves runtime metals rules to validated YAML, applies each
dataset/rule pair once, preserves the component-based material structure of
``EPR construction``, and records rule-specific decisions and target values in
the structured change report. The inventory API introduced in 2.5.0 is
unchanged.

Version 2.5.0 introduces a controlled inventory API, read-only validation
certificates, structured change reports, and a more efficient scenario and
export pipeline. Constructor, update, and export signatures remain available,
but integrations that accessed the mutable ``NewDatabase.database`` attribute
must migrate.

Migrating inventory access
----------------------------

Integrations that accessed mutable ``NewDatabase.database`` lists must use the
store API or request an independent copy of the inventory. See
:doc:`/user_guide/inventories` for the maintained query, transaction and
examples of copying inventories into dictionaries. The runtime backend distinction and a transaction
limitation found during documentation review are recorded in
:doc:`documentation-audit`.

Validation and reports
------------------------

The 2.5 series introduced saved inventory validation results and structured
Excel/Parquet change reports. Read :doc:`/user_guide/validation` for certificate
behaviour, :doc:`/user_guide/reports` for report generation, and
:doc:`/reference/change-report-schema` for the current schema. These pages
replace duplicated API instructions in the release notes.

Update and export workflow
----------------------------

``update_and_write()`` avoids the former intermediate scenario dump and reload
when a scenario should be written directly to Brightway:

.. code-block:: python

    ndb.update_and_write(name="image-ssp2-2050")

For exploratory use, the repository now provides a numbered series of 12
notebooks covering construction, consequential scenarios, custom inputs,
external datapackages, export formats, scenario arrays, matrices, incremental
databases, reports, and score comparison.

Further details
-----------------

See :doc:`/reference/change-report-schema` for the report schemas and lifecycle. The
complete release notes, including performance, diesel-market, battery-market,
and certification changes, are recorded in the project ``CHANGELOG.md``.
