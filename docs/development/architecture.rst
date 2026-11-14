Inventory and scenario architecture
=====================================

Inventories are owned by ``InventoryStore``. The compact backend provides
copy-on-write scenario forks and versioned checkpoints; the legacy backend
supports compatibility and differential checks. Public users should use
:doc:`/user_guide/inventories`, not private mutable activity lists.

See :doc:`/user_guide/inventories` for storage guidance and
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
   * - ``premise.validation`` and ``premise.audit``
     - Validation rules and independent physical auditing
   * - ``premise.export`` and ``premise.reporting``
     - Export backends, change reports and result comparison functions
   * - ``premise.core``
     - Shared transformation, filesystem, cache and logging infrastructure

The public ``from premise import NewDatabase`` and
``from premise.transport import Transport`` APIs remain available. Legacy
module imports resolve centrally to the same implementations, allowing older
cache class paths to load without copying implementation files at package root.
New implementation imports should use their topic paths.

Bundled inventory profiles
--------------------------

``premise/inventory/profiles.py`` applies source-specific selections as part of
``DefaultInventory`` preparation, before or after ordinary version migration.
The quantities, supplier identities and version/system-model boundaries are
stored in ``premise/data/utils/import/inventory_profiles.json``. Its
``source_selection`` entries also declare companion workbook roles, versions
and preparation order. The constructor
coordinates the sources; it does not implement individual component patches.
Selections validate the complete affected scope before mutation and preserve
unrelated inputs and uncertainty. Cache identities include applicable profile
data and the shared implementation. Regression tests inspect the current
packaged inventories using explicit scientific contracts, rather than replacing
them with historical workbook copies.

Native source normalization
---------------------------

``premise/data/utils/import/source_normalizations.json`` declares the recognized
cement refractory and passenger-car engine aluminium corrections. The shared
executor in ``DatabaseCleaner`` checks native identities, reference-output
signs, complete supplier chains, coefficients and uncertainty before applying
any edit. These corrections run on the source database before caching;
additional-inventory profiles run separately. Applicable normalization data
participate in native source cache identities, while profile and source-selection
data participate in import cache identities.

Scientific reference data
--------------------------

Metal-content factors, carrier identities and resource-attribution boundaries
live in ``premise/data/metals/resource_attribution.json``. Graph traversal and
validation remain in the materials modules. The data retain inherited values
and global applicability. Chain-derived factors are attribution conventions;
their original database-version derivations still require scientific review.
Moving them into data does not establish independently measured ore grades.

Compression model assumptions live in
``premise/data/fuels/compression_parameters.json`` with their limitations.
Photovoltaic lifetime metadata and country adapters share the lifetime in
``premise/data/solar/pv_2026_parameters.json``. Independent auditor contracts and
fuel plausibility ranges live in
``premise/data/utils/audit/reference_contracts.json``; the auditor does not read
its expected resource amounts from the production attribution configuration.

The reusable direct-metal-input comparison belongs to ``premise.audit``.
Historical comparison imports resolve to the same implementation; the unused
campaign-specific bundle writer and IAM mapping spreadsheet script have been
removed. Exports and remote inventory downloads use per-call temporary
directories, with only explicitly selected resources included in datapackages.

Compact storage and checkpoints
---------------------------------


``CompactInventoryStore`` shares unchanged data between scenarios and copies
data when it changes. It saves inventories in Arrow IPC files and keeps
additional activity metadata in separate files. Reopening a saved inventory
preserves its Python and NumPy numeric types. ``LegacyInventoryStore`` stores
activities as dictionaries.
The bundle contains:


.. code-block:: text

   manifest.json
   strings.arrow
   activities.arrow
   exchanges.arrow
   metadata.bin
   metadata_offsets.arrow
   activity-fingerprints.pkl
   checksums.json


Checkpoint writes use a sibling temporary directory and replacement; every
file is verified before a bundle is opened. Store schema versions are
independent from the historical pickle cache schema. Existing
``inventory_backend="legacy"`` and ``inventory_backend="compact"`` calls remain
accepted. Both backends apply the same validation rules.



Automatic scenario cache expiry
---------------------------------


``NewDatabase`` checks for managed scenario checkpoints unused for more than 24
hours at startup. It also removes expired incomplete checkpoint writes. Cleanup
runs only when no other ``NewDatabase`` instance is alive: a process-held shared
lock protects inventories throughout updates, repeated exports, and reports.
The lock is released when the instance is garbage-collected or its process exits.
A long-lived notebook instance therefore defers cleanup until a later startup.

Pass ``cleanup_expired_caches=False`` to skip the startup sweep. This instance still
holds the shared lock. Restart existing premise processes when adopting this
feature so all builds participate in locking. Standalone use of the low-level
``InventoryStore`` API is not registered as an active build; keep checkpoints for
such use outside premise's internal ``cached_files`` directory.

Only newly written, UUID-named checkpoints in ``cached_files`` carry expiry
metadata. Successful checkpoint reopening refreshes their last-use timestamp;
expiry metadata is separate from the inventory files protected by checksums. Base
checkpoints needed by retained checkpoints are preserved. Existing unmarked
checkpoints are never automatically adopted or removed; clean them separately
after stopping active builds. Source/import caches in ``cache``, validation
caches, and extracted datapackages are outside this expiry policy.

Cleanup logs a count and estimated reclaimed space at INFO level (unless
``quiet=True``); filesystem errors are warnings and do not abort a build.

Transport performance requirements
----------------------------------


The project acceptance limit is a maximum 0.1% increase in complete scenario-build
runtime. Fuel lookups are cached, and the implementation adds no database-wide
scan. Acceptance requires repeated interleaved baseline/patched builds with the
same inputs, cache policy, export mode and warning handling. A one-sided 95%
upper confidence bound on the paired runtime ratio must not exceed 1.001.
An inconclusive benchmark is not a pass; microbenchmarks alone cannot establish
compliance with the whole-build limit.
