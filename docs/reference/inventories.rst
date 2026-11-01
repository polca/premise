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

Qualified MDEA capture burden proxy
-----------------------------------

Only for ecoinvent 3.12 cut-off, the source-3.12 companion
``lci-biomethane-capture-MDEA-proxy-312.xlsx`` loads before the original
source-3.9 carbon-capture workbook. Ordinary deduplication retains one capture
recipe. Its 0.000034 kg of native RER MDEA per kg CO2 is an explicit legacy
burden assumption, not measured neat make-up, aqueous blend or promoter demand.
The native market includes delivery; no extra delivery freight is added.
The storage module remains in its own source-3.9 workbook and is purchased once.
Other target/model selections and native-source cache keys are unchanged;
only the 3.12 cut-off inventory cache includes the companion.

EPR cladding supply
-------------------

Only for ecoinvent 3.12 cut-off, ``lci-epr-m5-native-312.xlsx`` (source 3.12)
is imported immediately before ``lci-nuclear_EPR.xlsx`` (source 3.8). Ordinary
import duplicate checks retain one M5 alloy producer, purchasing 0.9884 kg of
native nuclear-grade zirconium sponge per kg alloy; its six other inputs stay
unchanged. The qualified custom Kroll provider remains an unused legacy
alternative, so its reagents, yield, utilities and residues are not added again.

The original EPR source and four fuel-fabrication buyers keep their authored
3.8 profiles and assembly doses. Other target versions and consequential builds
retain the existing source selection. Only the 3.12 cut-off inventory cache
includes the companion; native source cache keys are unchanged.

Constructor workbook selection
--------------------------------

This table is generated from the constructor's inventory-selection statements.
It records selection before activity-level filtering, migration and linking.
Selection does not guarantee that every activity survives those steps or is
used by a scenario. Workbook source version is distinct from target version.

.. include:: generated/inventory-selection.inc
