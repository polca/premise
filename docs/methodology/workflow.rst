Workflow and system boundaries
================================

.. raw:: html

   <span id="transformation-and-certification"></span>


A Premise build has three main steps:

1. **Prepare inputs.** Read the source database and scenario data, add the
   applicable technology inventories and prepare reusable input data.
2. **Apply updates.** Change the selected processes and supply markets for
   each scenario, then check the resulting inventory.
3. **Export.** Write the scenario database to Brightway or another format.
   Review the change report before calculating and interpreting impacts.

.. image:: /main_workflow.png
   :alt: Read the source database, add inventories, apply scenario updates and export the result.

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

Transformation and validation
-------------------------------

``ndb.update()`` applies the mapped sector transformations in dependency order.
Source inventories, geography, efficiencies and market composition can all
influence results. See :doc:`/user_guide/updates` for selective updates and
:doc:`validation` for what validation checks and what it cannot establish.

Export and interpretation
---------------------------

An exporter writes the scenario in its target format and adds the relevant
schema checks. See :doc:`/user_guide/export/index`. Export does not turn a
scenario projection into a forecast or establish agreement with an IAM's
aggregate climate outcome.
