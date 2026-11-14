Inventory catalogue and sources
=================================

CLIC pages describe the published reference inventories. Premise's source
selection, adaptations and generated scenario activities are documented in
these chapters.

After the ecoinvent database is extracted and checked, a number of additional inventories
are imported, regardless of the year of scenario that is being considered.

Explore the `CLIC inventory catalogue <https://clic.psi.ch/browse/>`__ and
the dataset pages linked below for source descriptions, provenance and reuse.

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

.. raw:: html

   <span id="epr-cladding-supply"></span>
   <span id="qualified-mdea-capture-burden-proxy"></span>

Source-specific adaptations are explained with their inventories:
:ref:`MDEA capture supply <mdea-capture-reference>` and
:ref:`EPR cladding supply <epr-cladding-reference>`.

.. _constructor-workbook-selection:

Inventory source selection
----------------------------

This table is generated from the constructor's inventory-selection statements.
It records selection before activity-level filtering, migration and linking.
Selection does not guarantee that every activity survives those steps or is
used by a scenario. Import source version is distinct from the target version
and the background profile currently published on CLIC. Linked records identify
published processes within each source family; the sector chapters describe
individual routes and remaining unmatched activities.

.. include:: generated/inventory-selection.inc
