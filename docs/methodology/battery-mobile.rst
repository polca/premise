Mobile batteries
==================


.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``battery`` update changes mass inputs in existing mobile battery
capacity markets and the chemistry shares in scenario-average markets.
Their functional unit is 1 kWh of installed battery capacity, not 1 kWh of
electricity discharged over the battery's life.

.. figure:: /_static/process-diagrams/battery-mobile.svg
   :class: process-diagram
   :alt: Mobile battery capacity: Read cell-density targets; Scale kilogram inputs in capacity markets; Update available chemistry mixes; Supply explicit capacity consumers.
   :align: center

Inputs and applicability
--------------------------

Battery inventories and capacity markets are present after initialization,
subject to source-version import rules. ``data/battery/energy_density.yaml``
supplies external cell-energy-density targets and activity aliases;
``data/battery/mobile_scenarios.csv`` supplies chemistry scenarios
(LFP, NCx, PLiB and MIX). These are separate from the IAM pathway name.
``ndb.update("battery")`` handles mobile and stationary batteries together;
it is skipped if both battery scenario arrays are absent.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("battery")

Transformation
----------------

For an aliased capacity market, kilogram-unit technosphere inputs are
multiplied by the configured mean density in 2020 divided by the interpolated
mean density in the requested year. Interpolation is linear between 2020 and
2050 and holds endpoint values outside that interval. For uncertainty type 5,
the location and bounds are also rescaled using their respective trajectories.

The operation scales an existing pack-mass coefficient; it does not rebuild a
cell-to-pack engineering model. The source inventory therefore retains its
pack composition. The density targets below must not be interpreted as
whole-pack densities or converted directly into an absolute kg/kWh figure.

Markets and downstream links
------------------------------

Premise updates mapped chemistry exchanges in existing mobile scenario
markets. Chemistry shares are interpolated between observations, held at
endpoints outside the data range, and missing values are filled with zero.
Finite supplier exchanges with the market's unit are normalized when their
total is positive. These markets coexist; the selected consumer link decides
which scenario supplies a study.
Vehicle inventories determine where mobile capacity is consumed;
:doc:`transport/overview` explains the separate fleet update.


Assumptions and limitations
-----------------------------

Changing capacity-market mass does not by itself specify cycle life,
round-trip losses, utilization or delivered electricity. Those parameters
belong to the consuming vehicle or storage service. An imported chemistry can
be available without a positive scenario share. A zero-total chemistry market
is not repaired by normalization and requires investigation.

Worked example and checks
---------------------------

For NMC811, the configured mean cell densities are 0.28 kWh/kg in 2020 and
0.34 in 2050. The 2050 mass factor is 0.28/0.34 = 0.8235. An illustrative
starting coefficient of 5 kg/kWh therefore becomes 4.118 kg/kWh; 5 kg/kWh is
an example, not a universal inventory coefficient. Check the actual market's
kilogram exchanges, chemistry share closure and its consumer. For storage
LCIA, inspect throughput and lifetime in the operating activity as well.

Sources and inventory details
-------------------------------

Configured density targets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These cell-density targets and mass factors are generated from the packaged
configuration. They are not whole-pack densities or absolute pack masses.
The aliases determine which capacity markets receive each trajectory.

.. include:: /reference/generated/battery-density.inc

Source inventories: li-ion batteries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Battery imports depend on the source ecoinvent version. The NMC111/811/NCA/LFP
supplement is skipped for 3.9 and later because those inventories are already
represented in the background. The NMC622/NMC532 supplement is skipped for
3.11 and 3.12. Other battery supplements are selected separately by the
constructor's import registry.
NMC-111, NMC-622 NMC-811 and NCA Lithium-ion battery inventories are originally
from `Dai <https://www.mdpi.com/2313-0105/5/2/48>`__ et al. 2019. They have been adapted to ecoinvent by `Crenna <https://doi.org/10.1016/j.resconrec.2021.105619>`__ et al, 2021.
LFP and LTO Lithium-ion battery inventories are from  `Schmidt <https://doi.org/10.1021/acs.est.8b05313>`__ et al. 2019.
Li-S (Lithium-sulfur) battery inventories are from `Wickerts <https://doi.org/10.1021/acssuschemeng.3c00141>`__ et al. 2023.
Li-O2 (Lithium-air) battery inventories are from `Wang <https://doi.org/10.1016/j.jclepro.2020.121339>`__ et al. 2020.
Finally, SIB (Sodium-ion) battery inventories are from `Zhang22 <https://doi.org/10.1016/j.resconrec.2023.107362>`__ et al. 2024.
Ecoinvent provides also inventories for LMO (Lithium Maganese Oxide) batteries.

They introduce the following datasets:

 ============================================================= =========== ======================================
  Battery components                                            location    source
 ============================================================= =========== ======================================
  battery management system production, for Li-ion battery        GLO         Schmidt et al. 2019
  market for battery, Li-ion, NMC111, rechargeable, prismatic     GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, NMC622, rechargeable, prismatic     GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, NMC811, rechargeable, prismatic     GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, NCA, rechargeable, prismatic        GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, LFP, rechargeable, prismatic        GLO         Schmidt et al. 2019
  market for battery cell, Li-ion, LTO                            GLO         Schmidt et al. 2019
  market for battery, Li-sulfur, Li-S                             GLO         Wickerts et al. (2023)
  market for battery, Li-oxygen, Li-O2                            GLO         Wang et al. (2020)
  market for battery, Sodium-ion, SiB                             GLO         Zhang et al. (2024)
  market for battery, NaCl, rechargeable, prismatic               GLO         Galloway & Dustmann (2003)
 ============================================================= =========== ======================================

These battery inventories are mostly used by battery electric vehicles,
stationary energy storage systems, etc. (also imported by *premise*).

NMC-111, NMC-811, LFP and NCA inventories can be found here: `LCI_batteries1 <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-NMC111-811-NCA-LFP.xlsx>`__.
NMC622/NMC532 inventories are packaged in ``lci-batteries-NMC622-NMC532.xlsx``;
NMC955/LTO inventories are in ``lci-batteries-NMC955-LTO.xlsx``, in the same
additional-inventories directory.
Li-S inventories can be found here: `LCI_batteries3 <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-LiS.xlsx>`__.
Li-O2 inventories can be found here: `LCI_batteries4 <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-LiO2.xlsx>`__.
And SIB inventories can be found here: `LCI_batteries5 <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-SIB.xlsx>`__.

When using ecoinvent 3.9 and above, the NMC-111, NMC-811, LFP and NCA battery inventories
are not imported (as are already present the ecoinvent database).
