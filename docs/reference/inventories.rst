Inventory catalogue and sources
=================================

After the ecoinvent database is extracted and checked, a number of additional inventories
are imported, regardless of the year of scenario that is being considered.

All inventories can be found in the `premise/data/additional_inventories <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories>`__ folder.

Inventory sources by sector
-----------------------------

* :doc:`/methodology/electricity/photovoltaics` — IEA PVPS inventories,
  country electricity mixes and separate emerging-technology supplements.
* :doc:`/methodology/electricity/generation` — other electricity technologies.
* :doc:`/methodology/fuels/index` — hydrogen, ammonia, biofuels and synthetic fuels.
* :doc:`/methodology/metals` and :doc:`/methodology/mining` — material production
  and mining inventories.
* :doc:`/methodology/cement` and :doc:`/methodology/steel` — industrial processes.
* :doc:`/methodology/batteries` — mobile and stationary batteries.
* :doc:`/methodology/transport/index` — vehicle inventories.
* :doc:`/methodology/cdr` — carbon capture and removal routes.

Import conditions depend on the ecoinvent version and system model; see each
sector page. For example, the 2026 PV core replaces the legacy core only for
ecoinvent 3.12 cut-off. See :doc:`inventory-migration` for background linking
between versions and :doc:`/user_guide/external-scenarios` for custom inventories.

Constructor workbook selection
--------------------------------

This table is generated from the constructor's inventory-selection statements.
It records selection before activity-level filtering, migration and linking.
Selection does not guarantee that every activity survives those steps or is
used by a scenario. Workbook source version is distinct from target version.

.. include:: generated/inventory-selection.inc
