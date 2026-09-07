Workflow and system boundaries
================================

.. image:: /main_workflow.png
   :alt: Main workflow diagram of premise ETL process

As illustrated in the workflow diagram above, *premise* follows an Extract, Transform, Load (`ETL <https://www.guru99.com/etl-extract-load-process.html#:~:text=ETL%20is%20a%20process%20that,is%20Extract%2C%20Transform%20and%20Load.>`__) process:

1. Extract the ecoinvent database from a `brightway <https://brightway.dev/>`__ project or from `ecospold2 <https://ecoinvent.org/the-ecoinvent-database/data-formats/ecospold2/>`__ files.
2. Expand the database by adding additional inventories for future production pathways for certain commodities, such as electricity, heat, steel, cement, etc.
3. Modify the ecoinvent database, focusing primarily on process efficiency improvements and market adjustments.
4. Load the updated database back into a Brightway project or export it as a set of CSV files, such as Simapro CSV files.

Choosing a system model
-------------------------

Choose a source database and a compatible ``system_model`` before building a
scenario. Cut-off modelling describes supply with attributional allocation
rules; consequential modelling identifies suppliers responding to a change in
demand. This choice affects market construction and inventory compatibility.
See :doc:`/user_guide/source-databases` for setup and :doc:`system-models` for
the detailed consequential method at the end of this methodology guide.

Extraction and preparation
----------------------------

The constructor extracts and normalizes the source database, imports the
applicable additional inventories, loads scenario data and prepares reusable
caches. See :doc:`/user_guide/source-databases` and
:doc:`/reference/inventories` for the inputs.

Transformation and certification
----------------------------------

``ndb.update()`` applies the mapped sector transformations in dependency order.
Source inventories, geography, efficiencies and market composition can all
influence results. See :doc:`/user_guide/updates` for selective updates and
:doc:`validation` for the meaning and limits of certification.

Export and interpretation
---------------------------

An exporter writes the scenario in its target format and adds the relevant
schema checks. See :doc:`/user_guide/export/index`. Export does not turn a
scenario projection into a forecast or establish agreement with an IAM's
aggregate climate outcome.
