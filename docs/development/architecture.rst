Inventory and scenario architecture
=====================================

Inventories are owned by ``InventoryStore``. The compact backend provides
copy-on-write scenario forks and versioned checkpoints; the legacy backend
supports compatibility and differential checks. Public users should use
:doc:`/user_guide/inventories`, not private mutable activity lists.

See :doc:`performance-history` for dated measurements and
:doc:`/reference/api` for the supported interface. Runtime storage behaviour
is defined in ``premise/inventory_store.py``; source loading and scenario generation is
in ``premise/new_database.py``.
