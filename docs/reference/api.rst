Public Python API
===================

.. contents:: On this page
   :local:
   :depth: 1


These signatures and descriptions are generated from the installed source
package. See :doc:`/user_guide/inventories` for working with inventories and
:doc:`/getting_started/first-scenario` for the main workflow.

Database workflows
--------------------

.. autoclass:: premise.NewDatabase
   :members: update, update_and_write, get_inventory_store, materialize_inventory, get_validation_report, generate_change_report, generate_scenario_report, write_db_to_brightway, write_db_to_matrices, write_db_to_simapro, write_db_to_olca, write_superstructure_db_to_brightway, write_scenario_array_db_to_brightway, write_datapackage
   :special-members: __init__

.. autoclass:: premise.IncrementalDatabase

.. autoclass:: premise.PathwaysDataPackage

Inventory stores
------------------

.. autoclass:: premise.InventoryStore
   :members:

.. autoclass:: premise.CompactInventoryStore

.. autoclass:: premise.LegacyInventoryStore

.. autoclass:: premise.InventoryStoreBuilder
   :members:

Validation and reports
------------------------

.. autoclass:: premise.ValidationReport
   :members: raise_for_errors

.. autoclass:: premise.ValidationIssue

.. autoclass:: premise.ValidationRuleResult

.. autoclass:: premise.ValidationPhaseResult

.. autoclass:: premise.PremiseValidationError

.. autoclass:: premise.ChangeReportArtifacts

Utilities
-----------

.. autofunction:: premise.clear_cache

.. autofunction:: premise.clear_inventory_cache

.. autofunction:: premise.get_regions_definition
