Troubleshooting and FAQ
=========================

.. raw:: html

   <span id="how-does-premise-handle-the-different-granularities-between-the-iam-regions-and-the-ecoinvent-regions"></span>


.. contents:: On this page
   :local:
   :depth: 1

Find the symptom below, then check the explanation for the relevant topic.
For installation and the first run, start with :doc:`/getting_started/index`.

Start with the symptom
------------------------

.. list-table:: Common failures and next steps
   :header-rows: 1

   * - Symptom
     - Check
     - Remedy
   * - Source database is missing
     - Existing project and database names
     - Run :doc:`/getting_started/preparation`; select the existing import.
   * - IAM file cannot be decrypted or found
     - Model/pathway filename, configured directory and matching key
     - Obtain the matching encrypted file/key; never print the key in an issue report.
   * - Output already exists
     - Output name in the selected project
     - Choose a distinct name; the tutorial deliberately refuses overwrite.
   * - Validation stops the update
     - Rule ID, activity identity, expected and actual values
     - Follow :doc:`validation`; correct the input or mapping and rebuild.
   * - An update reports missing scenario variables
     - File coverage versus mapped aliases
     - Consult :doc:`/reference/coverage`; absence is not a zero production volume.
   * - A GWP score changes unexpectedly
     - Functional unit, LCIA method, market shares and upstream contributions
     - Follow :doc:`interpreting-results` before changing an inventory.
   * - Inspecting the whole database exhausts memory
     - Whether the entire inventory was copied into dictionaries
     - Query the store through :doc:`inventories` and select only needed activities.


.. raw:: html

   <span id="can-i-share-results-obtained-with-the-modified-ecoinvent-database"></span>
   <span id="can-i-share-the-modified-ecoinvent-database"></span>
   <span id="can-i-use-the-modified-ecoinvent-database-for-commercial-purposes"></span>
   <span id="ecoinvent"></span>
   <span id="how-can-i-share-modified-ecoinvent-databases"></span>
   <span id="how-does-premise-handle-differences-between-iam-and-ecoinvent-regions"></span>
   <span id="how-does-premise-use-ecoinvent"></span>
   <span id="is-the-unit-and-the-description-of-these-parameters-documented-or-are-they-necessarily-the-same-as-the-ones-of-the-ecoinvent-datasets-they-refer-to"></span>
   <span id="what-is-ecoinvent"></span>
   <span id="what-is-the-ecoinvent-version-used-in-premise"></span>

Database access and sharing
-----------------------------

Premise needs an ecoinvent database supplied separately. Check
:doc:`/reference/compatibility` for accepted versions and system models, and
:doc:`/getting_started/preparation` for local project and database names.
A Brightway project name is not a database import.

The software licence and source-data licences are separate. Check the
applicable source licences before distributing a database or using its data
commercially. :doc:`export/datapackage` describes reconstruction packages
whose recipients also need the source database. When publishing results,
retain the source and scenario attribution listed in
:doc:`/getting_started/overview`.

.. raw:: html

   <span id="does-premise-generate-more-regionalised-datasets-than-in-original-ei3-x-database"></span>
   <span id="how-big-an-effort-would-it-be-to-link-to-a-new-iam-as-simple-as-an-extension-of-the-mapping-files-what-difficulties-can-be-anticipated"></span>
   <span id="i-use-a-different-iam-than-remind-image-message-gcam-or-tiam-ucl-can-i-still-use-premise"></span>
   <span id="id10"></span>
   <span id="what-columns-are-necessary-in-the-iam-files"></span>

Scenario files and coverage
----------------------------

A model may represent a technology without reporting it in your scenario
file. Premise also needs a mapping that connects the variable to its update.
Check :doc:`/reference/coverage` and the :doc:`scenario catalogue
</reference/scenarios>`. :doc:`/reference/iam-variables` describes the file
layout, units and variable names.

A missing variable is different from a reported zero. Depending on the
sector, missing data can leave an input unchanged, skip an update or use a
documented alternative. Read the relevant sector's **Inputs and applicability**
section to identify the rule used.

.. raw:: html

   <span id="additional-inventories"></span>
   <span id="can-additional-inventories-be-modelled-with-parameters-if-so-how-are-they-used"></span>
   <span id="can-some-parameters-of-the-additional-inventories-be-made-scenario-and-time-dependant"></span>
   <span id="how-was-the-list-of-variables-in-the-mapping-files-established"></span>
   <span id="iam-data-collection"></span>
   <span id="iam-models"></span>
   <span id="is-it-possible-to-expand-this-list-e-g-agriculture-crops-for-energy"></span>
   <span id="what-if-a-variable-in-premise-corresponds-to-several-variables-in-the-iam"></span>

Custom models and variables
-----------------------------

Use :doc:`external-scenarios` to add your own projections for existing
technologies. Supporting another IAM may also require geographical definitions,
unit conversions and code for technologies represented differently. Follow
:doc:`/development/iam-mapping` for that work.

Adding a variable to a mapping does not create an update by itself: a
transformation must use it. Several IAM variables can map to one technology,
but their units and aggregation must describe the same intended quantity.
The mappings were developed with IAM collaborators listed in
:doc:`/development/contributing`.

.. raw:: html

   <span id="are-all-datasets-named-market-for-heat-replaced"></span>
   <span id="are-datasets-regionalized-on-the-basis-of-the-iam-scenario-only-or-does-it-come-from-other-sources"></span>
   <span id="does-the-secondary-heat-market-supply-both-buildings-and-industry"></span>
   <span id="heat"></span>
   <span id="regionalization"></span>
   <span id="why-are-legacy-ecoinvent-heat-markets-still-present-after-the-update"></span>
   <span id="why-can-several-exchanges-point-to-the-same-new-heat-market"></span>

Locations and heat markets
----------------------------

Regional variants use IAM geography and available inventory suppliers;
external assumptions can also determine their locations. Consult
:doc:`/methodology/regionalization` for supplier selection and
:doc:`/reference/geography` for the location mapping.

Heat updates replace selected consumers, not every activity with ``market for
heat`` in its name. Existing heat markets can remain in the database.
Different original exchanges can also end up buying from the same new supplier;
that alone does not show double counting. See :ref:`heat-transformation` for
purchased heat, on-site heat and the market boundaries.

.. raw:: html

   <span id="can-premise-manage-an-efficiency-evolution-for-the-additional-inventories"></span>
   <span id="efficiency-adjustments"></span>
   <span id="is-the-calculated-scaling-factor-ratio-of-efficiencies-in-year-20xx-vs-2020-applied-to-all-inputs-of-the-transformed-dataset-or-only-to-the-energy-feedstock-input"></span>
   <span id="what-happens-if-the-iam-does-not-provide-efficiencies-for-certain-processes"></span>
   <span id="why-use-external-data-sources-for-pv-efficiency-rather-than-the-output-of-iam"></span>

Efficiency changes
--------------------

Each sector selects the exchanges affected by an efficiency change.
Electricity's general adjustment scales all non-production exchanges;
cars and trucks scale selected energy inputs and fuel-related emissions,
while other transport modes also scale all biosphere exchanges. PV changes
module area. The :doc:`/methodology/shared-principles` chapter explains the
common rules and links to the exceptions.

If an IAM efficiency is missing, an update can leave the inventory unchanged
or use an external trajectory. For example, PV uses packaged module-efficiency
data; see :ref:`pv-efficiency-transformation`. Custom scenarios can supply
time-dependent efficiency assumptions through
:doc:`/reference/external-scenario-format`.

.. raw:: html

   <span id="consistency-with-climate-targets"></span>
   <span id="how-do-we-ensure-consistency-between-iam-scenario-and-plca-results-in-terms-of-global-warming-temperature-increase"></span>

Unexpected impact results
---------------------------

Start with :doc:`interpreting-results`: check the product, unit, suppliers and
method before attributing a change to a technology. An updated inventory does
not independently reproduce the IAM's global temperature outcome. Coverage,
system boundaries and additional data affect that comparison; see
:doc:`/methodology/validation`.

Getting help
--------------

Keep the full error message, Premise version, model, pathway, year, source
version and system model, and the relevant report. Include a small example
when reporting an issue in the
`Premise issue tracker <https://github.com/polca/premise/issues>`__.
Remove access keys and private inventory data before sharing an example.
