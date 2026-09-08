Troubleshooting and FAQ
=========================

.. raw:: html

   <span id="how-does-premise-handle-the-different-granularities-between-the-iam-regions-and-the-ecoinvent-regions"></span>


.. contents:: On this page
   :local:
   :depth: 1

Here are some frequently asked questions about ``premise``.
If you have a question that is not answered here, please contact us.

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


Ecoinvent
-----------

What is ecoinvent?
~~~~~~~~~~~~~~~~~~~~

Ecoinvent is the source life-cycle inventory database used by *premise*. It must be supplied separately; see :doc:`/getting_started/installation`.

What is the ecoinvent version used in ``premise``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :doc:`/reference/compatibility` for the maintained version and system-model list.

How does ``premise`` use ecoinvent?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It imports additional inventories and updates mapped activities for a scenario. See :doc:`/methodology/workflow` for boundaries and :doc:`/methodology/index` for sector details.

Can I share the modified ecoinvent database?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check the applicable ecoinvent and additional-inventory licence terms before sharing a database. A modified database still includes source data. For reconstruction packages, see :doc:`export/datapackage`.

Can I share results obtained with the modified ecoinvent database?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Results and databases are different deliverables. Consult the applicable source licences and publication requirements; retain the scenario and inventory attribution described in :doc:`/getting_started/overview`.

Can I use the modified ecoinvent database for commercial purposes?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The software licence and source-data licences are separate. Review the terms applying to your ecoinvent database and additional inventories; see :doc:`/reference/inventories` for data sources.

How can I share modified ecoinvent databases?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :doc:`export/datapackage` for reconstruction packages that require the recipient to have the source database. This is distinct from distributing the source database itself.


IAM models
------------

I use a different IAM than REMIND, IMAGE, MESSAGE, GCAM or TIAM-UCL ... Can I still use ``premise``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, but another model requires variable mappings and geographical definitions, and may need code support for different representations. Follow :doc:`/development/iam-mapping`.

What columns are necessary in the IAM files?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The file must identify regions, variables, units and values by year in the supported layout. See :doc:`/reference/iam-variables` for the file specification.

How big an effort would it be to link to a new IAM? As simple as an extension of the mapping files? What difficulties can be anticipated?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mapping existing technologies may be sufficient; missing variables, different units or new technology representations require additional work. See :doc:`/development/iam-mapping`.


IAM data collection
---------------------

How was the list of variables in the mapping files established?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The mappings were developed with IAM collaborators. Contributors are listed in :doc:`/development/contributing`; the maintained configuration is described in :doc:`/reference/iam-variables`.

Is it possible to expand this list? (e.g. agriculture crops for energy)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Add the relevant IAM aliases and ensure the transformation consumes them. Merely adding a variable does not create a new sector update. See :doc:`/development/iam-mapping`.

Is the unit and the description of these parameters documented? Or are they necessarily the same as the ones of the ecoinvent datasets they refer to?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

IAM units and ecoinvent reference-product units are not generally identical. Consult :doc:`/reference/iam-variables` and the sector transformation for the required conversion.

What if a variable in ``premise`` corresponds to several variables in the IAM?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mappings can represent several IAM variables. Their aggregation must reflect the intended technology and unit; see :doc:`/reference/iam-variables` and the corresponding sector page.


Regionalization
-----------------

Are datasets regionalized on the basis of the IAM scenario only, or does it come from other sources?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both IAM regions and external inventory assumptions matter. See :doc:`/methodology/regionalization`; for a country-extension example, see :doc:`/methodology/electricity/photovoltaics`.

Does ``premise`` generate more regionalised datasets than in original EI3.x database?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mapped processes can receive additional regional variants. Coverage depends on the IAM data and available inventories; see :doc:`/methodology/regionalization`.

How does ``premise`` handle differences between IAM and ecoinvent regions?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The geographical mapping connects countries and ecoinvent locations to IAM regions. See :doc:`/reference/geography` for the correspondence table and :doc:`/methodology/regionalization` for supplier selection.


Heat
--------

Does the secondary heat market supply both buildings and industry?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The secondary market supplies purchased heat to the end-use heat markets; on-site technologies remain distinct. See :ref:`heat-transformation` for the architecture and coverage.

Why are legacy ecoinvent heat markets still present after the update?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The heat update relinks selected consumers while retaining legacy datasets. See :ref:`heat-transformation` for the treatment of legacy markets.

Why can several exchanges point to the same new heat market?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Distinct original exchanges can converge on the same replacement supplier. Repeated supplier names alone do not demonstrate double counting; see :ref:`heat-transformation`.

Are all datasets named ``market for heat`` replaced?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Replacement follows configured activity and product patterns. Specialized markets may remain unchanged; see :ref:`heat-transformation` for the scope.


Consistency with climate targets
----------------------------------

How do we ensure consistency between IAM scenario and pLCA results (in terms of global warming / temperature increase)?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An inventory update is not an independent reproduction of the IAM climate outcome. Sector coverage, system boundaries and additional data affect the comparison. See :doc:`/methodology/validation` and :doc:`/getting_started/overview`.


Additional inventories
------------------------

Can additional inventories be modelled with parameters? If so, how are they used?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

External-scenario packages can specify scenario-dependent changes, including efficiencies. See :doc:`create-external-scenario` and :doc:`/reference/external-scenario-format`.

Can some parameters of the additional inventories be made scenario- and time-dependant?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, through the supported external-scenario configuration and data tables. See :doc:`/reference/external-scenario-format`.

Can ``premise`` manage an efficiency evolution for the additional inventories?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, where an efficiency mapping and the relevant transformation are provided. See :doc:`/reference/external-scenario-format`.


Efficiency adjustments
------------------------

Is the calculated scaling factor (ratio of efficiencies in year 20XX vs 2020) applied to all inputs of the transformed dataset, or only to the energy feedstock input?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The scope is sector-specific. CDR, for example, scales mapped electricity and heat/fuel inputs separately; PV adjusts module area and related exchanges. See :doc:`/methodology/cdr` and :doc:`/methodology/electricity/photovoltaics`.

What happens if the IAM does not provide efficiencies for certain processes?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The result depends on the sector: an update may be skipped or use an explicitly documented external trajectory. Consult the corresponding :doc:`/methodology/index` page.

Why use external data sources for PV efficiency, rather than the output of IAM?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Detailed PV module trajectories are taken from the packaged external-data file. See :ref:`pv-efficiency-transformation` for the current values, sources and scaling rules.
