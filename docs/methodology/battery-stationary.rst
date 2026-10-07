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

Vanadium co-product allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starting with 2.5.4, the South African route in
``premise/data/additional_inventories/lci-batteries-vanadium.xlsx`` uses
attributional allocation for its ilmenite and sodium-sulfate co-products.
The quantities below refer to Weber et al. (2018),
`supporting information <https://acs.figshare.com/articles/journal_contribution/Life_Cycle_Assessment_of_a_Vanadium_Redox_Flow_Battery/7033001>`__,
Tables S9 and S12--S14. Allocation assumptions are also recorded in the
workbook's skipped ``Allocation 2.5.4`` sheet and in exchange comments.

* Mining jointly produces 1.53 kg vanadium-bearing magnetite and 0.46 kg
  ilmenite. Shared energy, infrastructure, emissions and land/water burdens
  receive an economic allocation of **58.6856% to magnetite**. The share is
  ``(1.53 * 0.075168516669567) / (1.53 * 0.075168516669567 + 0.46 * 0.176011)``.
  Both prices are EUR2005/kg from the magnetite and ilmenite production
  exchanges of ecoinvent 3.12 cut-off's ``ilmenite - magnetite mine operation``
  at GLO. Ordinary magnetite is a price proxy for vanadium-bearing magnetite;
  no vanadium premium is assumed. This fixed foreground assumption is used
  across supported background versions, rather than recalculated at import.
* Iron and vanadium extraction are assigned to the magnetite stream at
  ``1.08 / 1.53`` and ``0.019 / 1.53`` kg per kg magnetite. Titanium extraction
  is assigned to the separated ilmenite co-product. These element-specific
  resource flows are not economically scaled. The former negative
  ``ilmenite - magnetite mine operation`` input is removed. Thus the later
  metals resource correction cannot retain an avoided-ilmenite extraction
  credit after zeroing the mine's direct titanium flow.
* The slag inventory already encoded **50% economic allocation** through a
  reference output of ``0.1226 = 0.0613 / 0.5``. Normalizing this output to one
  kilogram preserves that allocation. Despite its legacy name, the reference
  quantity represents V2O5 contained in slag, not bulk slag at 25% V2O5.
* Refining produces 1 kg V2O5 and 1 kg sodium sulfate, while consuming 0.5 kg
  sodium sulfate. The chemical input is retained and the negative
  sodium-sulfate market input is removed. Shared burdens receive an economic
  allocation of **98.9280% to V2O5**, calculated as
  ``6.46 / (6.46 + 140 / 2000)``. Both prices refer to 2010 USD: 6.46 USD/lb
  V2O5 from the `USGS 2014 vanadium summary <https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/mineral-pubs/vanadium/mcs-2014-vanad.pdf>`__
  and 140 USD/short ton sodium sulfate from the
  `USGS 2011 sodium-sulfate summary <https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/mineral-pubs/sodium-sulfate/mcs-2011-nasul.pdf>`__.
  A short ton contains 2000 lb. These US prices are historical geographic
  proxies for the South African refinery, not current market prices.

The historical ``Allocation titanium-vanadium`` sheet is retained for
provenance but is not used: its approximately 77/23 split combines contained
metal quantities and a titanium-slag price instead of comparable mining
outputs. The Chinese vanadium route retains its original allocation and
exchange amounts. Recyclable iron-scrap outputs and waste-treatment signs
are retained; the cut-off background classifies unsorted iron scrap as a
recyclable output without an avoided-primary-production benefit.

All affected South African datasets are normalized to one kilogram of their
reference product. For lognormal uncertainty, ``loc = ln(abs(amount))`` and
the existing relative ``scale`` is retained. Negative exchanges keep their
negative flag. Formula caches in unaffected workbook cells are preserved so
that a spreadsheet application does not need to recalculate them before import.

.. important::

   This correction resolves co-product allocation and the negative titanium
   credit. It does not establish a closed physical vanadium balance for the
   published metallurgical chain: Table S9 reports 0.034 kg V2O5 entering the
   ore route, while Tables S12--S13 describe a 0.0613 kg V2O5 slag stream.
   The source yields are retained, and this discrepancy requires clarification
   before those yields can be revised. Economic allocation and physical
   recovery yields must not be confused.

The additional-inventory cache includes the workbook's content hash.
Previously exported Brightway databases or files must be regenerated;
installing the new package does not modify them in place.
