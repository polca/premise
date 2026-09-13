Stationary batteries
======================


.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``battery`` update changes mass inputs in existing stationary battery
capacity markets and the chemistry shares in scenario-average markets.
Their functional unit is 1 kWh of installed battery capacity, not 1 kWh of
electricity discharged over the battery's life.

.. figure:: /_static/process-diagrams/battery-stationary.svg
   :class: process-diagram
   :alt: Stationary battery capacity: Read cell-density targets; Scale kilogram inputs in capacity markets; Update available chemistry mixes; Supply explicit capacity consumers.
   :align: center

Inputs and applicability
--------------------------

Battery inventories and capacity markets are present after initialization,
subject to source-version import rules. ``data/battery/energy_density.yaml``
supplies external cell-energy-density targets and activity aliases;
``data/battery/stationary_scenarios.csv`` supplies chemistry scenarios
(CONT and TC). These are separate from the IAM pathway name.
``ndb.update("battery")`` handles mobile and stationary batteries together;
it is skipped if both battery scenario arrays are absent.

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

Premise updates mapped chemistry exchanges in existing stationary scenario
markets. Chemistry shares are interpolated between observations, held at
endpoints outside the data range, and missing values are filled with zero.
Finite supplier exchanges with the market's unit are normalized when their
total is positive. These markets coexist; the selected consumer link decides
which scenario supplies a study.
A storage activity uses a capacity market only through an explicit inventory
exchange. The electricity market builder does not automatically insert the
CONT capacity market. See :ref:`electricity-storage-operation` for mapped
storage-operation suppliers.


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

Source inventories: vanadium redox flow batteries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*premise* imports inventories for the production of a vanadium redox flow battery, used
for grid-balancing, from the work of `Weber <https://doi.org/10.1021/acs.est.8b02073>`__ et al. 2021.
It is available under the following dataset:

* vanadium-redox flow battery system assembly, 8.3 megawatt hour

The dataset providing electricity is the following:

* electricity supply, high voltage, from vanadium-redox flow battery system

The power capacity for this application is 1MW and the net storage capacity 6 MWh.
The net capacity considers the internal inefficiencies of the batteries and the
min Sate-of-Charge, requiring a certain oversizing of the batteries.
For providing net 6 MWh, a nominal capacity of 8.3 MWh is required for the
VRFB with the assumed operation parameters. The assumed lifetime of the stack
is 10 years. The lifetime of the system is 20 years or 8176
cycle-life (49,000 MWh).

These inventories can be found here: `LCI_vanadium_redox_flow_batteries <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-vanadium-redox-flow.xlsx>`__.

This publication also provides LCIs for Vanadium mining and refining from iron ore.
The end product is vanadium pentoxide, which is available under the following dataset:

* vanadium pentoxide production

These inventories can be found here: `LCI_vanadium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-vanadium.xlsx>`__.
