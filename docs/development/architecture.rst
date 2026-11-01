Inventory and scenario architecture
=====================================

Inventories are owned by ``InventoryStore``. The compact backend provides
copy-on-write scenario forks and versioned checkpoints; the legacy backend
supports compatibility and differential checks. Public users should use
:doc:`/user_guide/inventories`, not private mutable activity lists.

See :doc:`performance-history` for dated measurements and
:doc:`/reference/api` for the supported interface. Runtime storage behaviour
is defined in ``premise/inventory/inventory_store.py``; source loading and scenario generation is
in ``premise/database/new_database.py``.

Topic packages
---------------

Runtime modules are grouped by responsibility. Package entry points load their
implementations on demand so a transport helper can be imported independently
of scenario generation and validation.

.. list-table:: Python source layout
   :header-rows: 1

   * - Package
     - Responsibility
   * - ``premise.database``
     - Database construction and incremental workflows
   * - ``premise.iam``
     - IAM inputs, geography, system models and external scenarios
   * - ``premise.inventory``
     - Inventory stores, imports, normalization and activity mappings
   * - ``premise.transport``
     - Transport transformations, vehicle energy, batteries and fuel handling
   * - ``premise.materials``
     - Metals, mining, steel, cement and shared material components
   * - ``premise.electricity`` and ``premise.energy``
     - Electricity, renewables, heat, biomass and final energy
   * - ``premise.batteries`` and ``premise.emissions``
     - Battery transformations, emissions and carbon removal
   * - ``premise.fuels``
     - Fuel supply chains, markets and carbon accounting
   * - ``premise.validation``
     - Validation rules and structured reports
   * - ``premise.export`` and ``premise.reporting``
     - Export backends, change reports and result comparison functions
   * - ``premise.core``
     - Shared transformation, filesystem, cache and logging infrastructure

The public ``from premise import NewDatabase`` and
``from premise.transport import Transport`` APIs remain available. Legacy
module imports resolve centrally to the same implementations, allowing older
cache class paths to load without copying implementation files at package root.
New implementation imports should use their topic paths.
