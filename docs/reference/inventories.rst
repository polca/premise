Inventory catalogue and sources
=================================

CLIC pages describe the published reference inventories. Premise's source
selection, adaptations and generated scenario activities are documented in
these chapters.

After extracting and checking the source database, Premise selects supplemental
inventories from CLIC according to the sectors and scenario technologies needed
by each build. Required workbook-only inventories remain in the hybrid pipeline.

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

CLIC is the standard supplemental inventory provider; there is no
``inventory_source`` selector. Inventories are prepared lazily when sectors are
updated, and the hybrid dependency selection retains required workbooks.
CLIC inventories target cutoff backgrounds. Consequential builds use compatible
workbooks instead and record the reason and requested sectors in the inventory
manifest. Ecoinvent 3.7 builds also retain compatible workbooks because reviewed
CLIC routes start at 3.8. Commercial policy rejects both unassessed fallbacks.
Other cutoff and EN15804 builds use slug-based CLIC selection and strict
destination linking.

This table is generated from the constructor's inventory-selection statements.
It records selection before activity-level filtering, migration and linking.
Selection does not guarantee that every activity survives those steps or is
used by a scenario. Import source version is distinct from the target version
and the background profile currently published on CLIC. Linked records identify
published processes within each source family; the sector chapters describe
individual routes and remaining unmatched activities.

.. include:: generated/inventory-selection.inc

Commercial CLIC selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To screen the selected CLIC activities and their upstream CLIC suppliers for
commercially permitted licenses, set
``clic_options={"license_policy": "commercial"}``.

See :doc:`clic-commercial` for replacement behavior, supported targets, report
contents, recipe replay and license scope. Approximate replacements can change
results substantially; the policy does not certify the entire resulting database
for commercial reuse.
