Premise 2.5 release and migration guide
=========================================

.. contents:: On this page
   :local:
   :depth: 1

Version 2.5.3 introduces **Brightpath as the exporter for openLCA and SimaPro**.
openLCA receives native JSON-LD packages containing processes, flows, units,
and provider links, replacing the previous openLCA-compatible SimaPro CSV
route. SimaPro receives more accurate CSVs with corrected flow mappings, units,
classifications, waste-treatment conventions, and metadata that respect
SimaPro Desktop's import requirements. Export reports identify unmatched or
excluded biosphere flows and classification decisions that need review.

The release also corrects fuel-carbon and hydropower water balances, preserves
biosphere and custom-scenario mappings, and fixes scenario-array ZIP writing
on Windows. GAINS reductions apply across every matching pollutant compartment,
and electricity-market comments again describe IAM-region coverage.

Upgrading to 2.5.3
-----------------

- Use **Python 3.12 or newer**. Premise installs
  ``brightpath>=1.0.0a4,<2`` from PyPI; Conda environments also need the
  ``romainsacchi`` channel for Brightpath.
- Remove legacy ``backend`` and ``format`` arguments from SimaPro/openLCA
  export calls. Both routes now use Brightpath exclusively. See
  :doc:`/user_guide/export/simapro` and :doc:`/user_guide/export/openlca` for
  the current commands and export-report interpretation.
- Supply a matching local LCIA method package to ``write_db_to_olca``.
  This route currently supports ecoinvent 3.8 and 3.12.
- Declare explicit ``bounds`` if a custom datapackage requires limits on
  absolute-efficiency targets. The former implicit clipping is removed;
  omitted bounds now leave supplied targets unclipped. See
  :doc:`/reference/external-scenario-format` for configuration and audit fields.
- Regenerate scenario databases to apply the inventory corrections. Updating
  the Python package does not modify databases already written to Brightway
  or imported into another LCA application.

Earlier 2.5 releases
-------------------

Version 2.5.2 corrects passenger-car fuel selection, adds a configurable
provisional combustion-car energy floor, and preserves exchange uncertainty
during inventory disaggregation. It also improves photovoltaic updates, IAM
scenario handling, and change-report performance. See
:doc:`/methodology/transport/passenger-cars` for the floor's scope, assumptions,
and configuration. The inventory API introduced in 2.5.0 is unchanged.

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
