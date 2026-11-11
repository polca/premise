# Mapping-declared prepared inventories

A mapping alias may declare `inventory_resource`, a JSON resource under `premise/data`. The normal inventory-loading step links its activities before the shared sector transformation runs.

Resources may now declare `clic_roots`, a list of required public dataset slugs. These join the ordinary CLIC root selection before preparation, using the same live archive resolution, migration policy, ownership, provenance and cache handling. Dependencies are not attached as heat-role suppliers: a kg CO2 capture service remains a dependency of a MJ heat process.

`inventory_requires_active_role: true` makes a resource conditional on that role being present with nonzero postprocessed IAM data for a supported model. Without this flag, the previous unconditional resource behaviour remains. A missing IAM object does not activate an explicitly conditional resource. This prevents a CCS-only inventory or its dependencies appearing in a heat scenario with no CCS role.

A resource may declare `supplier_checks`. Each check specifies a supplier identity and exchange selectors with reviewed amounts. An absent/ambiguous supplier, changed quantity, boolean quantity or non-finite value stops loading before mutating the destination. These checks protect derived physical balances, rather than requiring database metadata labels. Optional biosphere database rebinding uses the selected database name and the reviewed target-version flow UUIDs.

The reproduced motivating gap is native CCS heat: the prepared record requires a public capture chain that the old root planner did not request. Its capture quantity also depends on a specific native CHP allocation/combustion basis. The loading changes are confined to `prepared_resources.py` and its integration in `NewDatabase.prepare_inventories`. Sector transformation algorithms are not changed.

Tests cover conditional selection, preserved ordinary-resource behaviour, dependency validation/deduplication, mutation-free quantity refusals and biosphere database naming. The controlled CCS workflow additionally exercises normal lazy dependency preparation through `NewDatabase.update` and Brightway export.
