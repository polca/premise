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
pack composition. Most density targets below describe cells, not whole packs.
VRFB instead uses a fixed whole-system reference derived below; its configured
density and capacity-market kg/kWh share that installed-system basis.

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

.. _stationary-battery-mix-scenarios:

Battery mix scenarios
~~~~~~~~~~~~~~~~~~~~~

The stationary mixes draw on
`Schlichenmaier and Naegler (2022) <https://doi.org/10.1016/j.egyr.2022.11.025>`__:
**CONT** represents continuity in technology adoption, while **TC** represents
technology change toward a larger contribution from alternative storage
chemistries. The packaged ``premise/data/battery/stationary_scenarios.csv``
defines the actual capacity shares used by Premise.

In 2020, both cases have 40% LFP, 40% NMC, 10% lead-acid, 5% vanadium
redox flow (VRFB) and 5% in the source's sodium-based ``NAS`` category.
CONT retains 90% LFP/NMC capacity in 2050. TC instead reaches 33% VRFB and
33% NAS, with the remaining 34% supplied by LFP/NMC. Capacity shares in
percent are:

.. include:: /reference/generated/battery-stationary-mixes.inc

The current supplier mapping represents ``NAS`` with the
``market for battery capacity, Sodium-Nickel-Chloride, Na-NiCl, stationary``
inventory. This is a chemistry proxy, not a sodium-sulfur or sodium-ion
inventory. Organic and hybrid flow-battery inventories can be available
without receiving a share in either mix.

Shares are interpolated between the packaged annual observations in
2010–2050 and held at the endpoints outside that range. These assumptions
are independent of the IAM pathway and of energy-density improvements.
The markets are ``market for battery capacity, stationary (CONT scenario)``
and ``market for battery capacity, stationary (TC scenario)``, at GLO.
Each provides 1 kWh of installed capacity. The update adjusts both available
markets; a storage consumer's explicit supplier exchange selects which it
uses. Throughput, operating losses and replacement schedules belong to that
consumer, rather than to the chemistry shares.

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

See :ref:`battery-density-targets` for the configured trajectories and
:doc:`battery-inventories` for manufacturing references shared with mobile
applications. Electronics, material inputs and cooling are documented in
:doc:`battery-components`. The stationary-specific flow-battery references
follow below.

Source inventories: vanadium redox flow batteries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `vanadium-redox flow battery system assembly, 8.3 megawatt hour <https://clic.psi.ch/datasets/vanadium-redox-flow-battery-system-assembly-83-megawatt-hour/>`__; `electricity supply, high voltage, from vanadium-redox flow battery system <https://clic.psi.ch/datasets/electricity-supply-high-voltage-from-vanadium-redox-flow-battery-system/>`__; `vanadium pentoxide production <https://clic.psi.ch/datasets/vanadium-pentoxide-production/>`__; `gasket production, FKM, for VRFB system <https://clic.psi.ch/datasets/gasket-production-fkm-for-vrfb-system/>`__.

*premise* imports inventories for the production of a vanadium redox flow battery, used
for grid-balancing, from the work of `Weber <https://doi.org/10.1021/acs.est.8b02073>`__ et al. 2021.
It is available under the following dataset:

* `vanadium-redox flow battery system assembly, 8.3 megawatt hour <https://clic.psi.ch/datasets/vanadium-redox-flow-battery-system-assembly-83-megawatt-hour/>`__

The dataset providing electricity is the following:

* `electricity supply, high voltage, from vanadium-redox flow battery system <https://clic.psi.ch/datasets/electricity-supply-high-voltage-from-vanadium-redox-flow-battery-system/>`__

The original publication associates 6 MWh usable output with an 8.3 MWh nominal
label and 1 MW power, and gives a 10-year stack life and 20-year system life.
The Premise reference below derives nominal capacity independently from the
electrolyte charge; its capacity functional unit is not the publication's
6 MWh usable output. The assembly name is a stable identifier.

These inventories can be found here: `vanadium-redox flow battery system assembly, 8.3 megawatt hour (RER) <https://clic.psi.ch/datasets/vanadium-redox-flow-battery-system-assembly-83-megawatt-hour/>`__; `electricity supply, high voltage, from vanadium-redox flow battery system (RER) <https://clic.psi.ch/datasets/electricity-supply-high-voltage-from-vanadium-redox-flow-battery-system/>`__.

For ecoinvent 3.12, the FKM gasket uses the qualified
`FKM gasket reference on CLIC <https://clic.psi.ch/datasets/gasket-production-fkm-for-vrfb-system/>`__. This source is declared against both
technosphere and biosphere 3.12 and is imported before the VRFB inventory.
Native vinylidene-fluoride supply replaces the custom synthesis route. The
existing HFP, polymerisation and compounding stages are expanded into the
gasket reference once; no additional complete compound is purchased.

The same gasket identity is retained, so the VRFB stack still purchases
**428 kg**, and the hybrid-flow boundary-seal recipe still purchases
**1 kg per reference output**. Freight, composition, lifetime allocation
and finished-gasket retirement remain unchanged. The FE5540-type formulation,
cure-package allowance, manufacturing retention, utilities and generic
hazardous-waste treatment remain engineering proxies, not verified commercial
gasket specifications or fluorinated-waste destruction performance.

Targets below 3.12 use the original gasket route with its 3.9 source
declaration. The 3.12 native-vinylidene-fluoride reference is a separate
source profile.

The foreground
compilation retains the CC BY 4.0 terms and compiler attribution;
external inventories retain their separate terms.

This publication also provides LCIs for Vanadium mining and refining from iron ore.
The end product is vanadium pentoxide, which is available under the following dataset:

* vanadium pentoxide production

These inventories can be found here: `vanadium pentoxide production (ZA) <https://clic.psi.ch/datasets/vanadium-pentoxide-production/>`__; `vanadium pentoxide and unalloyed steel production (CN) <https://clic.psi.ch/datasets/vanadium-pentoxide-and-unalloyed-steel-production/>`__.

VRFB composition, charge and lifetime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The VRFB reference defines nominal capacity from electrolyte composition,
charge transfer and a stated voltage. The source assembly name is retained
as an identifier. Electrolyte production, hardware, capacity and delivered
electricity have separate reference quantities, described below.

Electrolyte and preparation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `vanadium electrolyte solution production <https://clic.psi.ch/datasets/vanadium-electrolyte-solution-production/>`__; `vanadium electrolyte conditioning <https://clic.psi.ch/datasets/vanadium-electrolyte-conditioning/>`__.

The solution follows the nominal composition in
`Oxkem VOS16, issue 1 (2024)
<https://www.oxkem.com/wp-content/uploads/2025/04/Vanadium-Electrolyte-TDS-1.6mol.L.pdf>`__:
1.6 mol V/litre, 4.2 mol total sulfate/litre, density 1.36 kg/litre and equal
amounts of V(III) and V(IV). Phosphoric acid is selected at 0.05 mol/litre,
the lower end of its stated customer-specific range. This also distinguishes
total sulfate from free acid, rather than treating the source's 2 M sulfate
statement as a complete commercial-electrolyte specification.

Per kg finished solution, purchase 0.106987059 kg V2O5, 0.302891029 kg pure
equivalent sulfuric acid and 0.003602721 kg pure equivalent phosphoric acid.
The latter supplier is specified without water in an 85% solution state;
its implicit carrier contributes another 0.000635774 kg water. A conditioning
service adds 0.600000182 kg deionised water and accounts for preparation.
V(V) to mean V(III/IV) needs 1.5 electrons per V atom. With oxygen evolution
at the counter-electrode, this releases 0.014116765 kg O2 per kg product.
Feeds including acid-carried water equal product plus O2; dilution water is
not added twice. These are complete-conversion, pure-equivalent material
quantities, not measured plant yields.

Reduction electricity uses
`Martin, Schafner and Turek (2020), Table 6 and Equation 5
<https://d-nb.info/1260641937/34>`__:
1.69 kWh per kg **elemental vanadium**, giving 0.101283688 kWh per kg solution.
The experiment used 4 M sulfate at 30 degrees C and 50 mA/cm2. Applying it to
the 4.2 M product with German electricity is an explicit process/location
proxy. Peripheral energy, thermal conditioning and manufacturing-equipment
wear (including electrodes and membranes) are coverage gaps. Process solvent
recirculation is not a new source of purchased or recycled vanadium.
Historical Table S8 freight distances are retained as proxies, with freight
quantities proportional to the corrected V2O5 shipment mass.

Installed capacity and voltage assumption
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Retain the source's individual hardware masses, including 272,220 kg solution,
32,589 kg tanks, a 10,016 kg stack and 5,125 kg periphery. Their sum is
319,950 kg, 2 kg above the printed Table S1 total; use the component sum for
installed mass. Periphery includes the source's 3,000 kg inverter assumption.
Its modern power-to-mass specification is not independently validated.

Both reservoirs contain active vanadium. A paired discharge transfers one
electron per **pair** of vanadium atoms across those reservoirs, as shown by
`Li et al. (2011), Equations 1--3
<https://www.pnnl.gov/sites/default/files/media/file/at/publications/333_926201145351.pdf>`__.
Use that paper's 1.25 V standard potential as a transparent nominal reference,
not a measured cycle-average voltage. The capacity calculation is

.. math::

   Q_{\mathrm{Ah}} &= \frac{m_{\mathrm{solution}}}{\rho}
       c_V \frac{F}{2\times3600}, \\
   E_{\mathrm{kWh}} &= Q_{\mathrm{Ah}} U_{\mathrm{ref}} / 1000.

This gives 4,291,705.41 cell-equivalent Ah and **5,364.631763 nominal reference
kWh** for the retained system. The
`stationary vanadium capacity market <https://clic.psi.ch/datasets/market-for-battery-capacity-redox-flow-vanadium-stationary/>`__
buys 59.640627 kg installed system per nominal kWh,
including 50.743464 kg electrolyte and **3.041116 kg contained V**.
The legacy activity name containing ``8.3 megawatt hour`` is an identifier,
not a capacity input. The source's 19.4 Wh/kg effective density is not used as
gross capacity, and no extra 21% balance-of-plant mass factor is imposed.

The configuration fixes this whole-system reference at 0.0167670941 kWh/kg
for all years. Capacity is deterministic; no concentration, voltage or
hardware redesign is projected. Engineering uncertainty remains: changing the
reference voltage from 1.25 V to 1.2/1.4 V gives 3.168/2.715 kg V per nominal
kWh. These conditional comparisons are not a measured probability interval.

Lifetime and disposal
^^^^^^^^^^^^^^^^^^^^^^^

The kilogram market represents installed equipment plus one replacement
stack over the source's 20-year system life and 10-year stack life. The
replacement is a lifecycle burden; it does not double installed electrolyte
or increase nominal capacity. A dedicated VRFB waste market sends the same
materials to existing treatments: two stacks, one electrolyte fill, one tank
and one set of periphery. Its treatment inputs sum to
``1 + 10016/319950`` kg per kg installed equipment, matching manufacture
including replacement. The capacity market purchases equal signed installed
mass for manufacture and this lifecycle waste service.

The standalone electricity supplier retains 8,176 cycles over 20 years,
selects a 95% usable SOC fraction and uses 75% round-trip efficiency. At the
reference voltage, delivered lifetime output is 41,668,167.83 kWh. Initial
system, stack replacement and disposal all use that denominator; charging
requires ``1/0.75`` kWh per delivered kWh. At this boundary a usable SOC window
is applied once, and the round-trip loss is not also subtracted from the
already defined delivered output. This reference convention is not a measured
SOC-dependent operating model or a reproduction of the source's 6/8.3 MWh
performance claim.

The generic mixed-storage consumer has its own lifetime-throughput
allocation; the standalone VRFB lifetime calculation does not define the
performance of that mixed-storage service. Treatment technologies and
primary/secondary market shares are separate inventory assumptions.

Vanadium co-product allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The South African
`vanadium pentoxide route <https://clic.psi.ch/datasets/vanadium-pentoxide-production/>`__ uses
attributional allocation for its ilmenite and sodium-sulfate co-products.
The quantities below refer to Weber et al. (2018),
`supporting information <https://acs.figshare.com/articles/journal_contribution/Life_Cycle_Assessment_of_a_Vanadium_Redox_Flow_Battery/7033001>`__,
Tables S9 and S12--S14.

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
  resource flows are not economically scaled. No avoided-ilmenite mine
  credit is included alongside this attribution to the separated products.
* The slag inventory already encoded **50% economic allocation** through a
  reference output of ``0.1226 = 0.0613 / 0.5``. Normalizing this output to one
  kilogram preserves that allocation. Despite its legacy name, the reference
  quantity represents V2O5 contained in slag, not bulk slag at 25% V2O5.
* Refining produces 1 kg V2O5 and 1 kg sodium sulfate, while consuming 0.5 kg
  sodium sulfate. The chemical input is retained without an avoided
  sodium-sulfate market credit. Shared burdens receive an economic
  allocation of **98.9280% to V2O5**, calculated as
  ``6.46 / (6.46 + 140 / 2000)``. Both prices refer to 2010 USD: 6.46 USD/lb
  V2O5 from the `USGS 2014 vanadium summary <https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/mineral-pubs/vanadium/mcs-2014-vanad.pdf>`__
  and 140 USD/short ton sodium sulfate from the
  `USGS 2011 sodium-sulfate summary <https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/mineral-pubs/sodium-sulfate/mcs-2011-nasul.pdf>`__.
  A short ton contains 2000 lb. These US prices are historical geographic
  proxies for the South African refinery, not current market prices.

The Chinese vanadium route retains its original allocation and
exchange amounts. Recyclable iron-scrap outputs and waste-treatment signs
are retained; the cut-off background classifies unsorted iron scrap as a
recyclable output without an avoided-primary-production benefit.

All affected South African datasets are normalized to one kilogram of their
reference product. For lognormal uncertainty, ``loc = ln(abs(amount))`` and
the existing relative ``scale`` is retained. Negative exchanges keep their
negative flag.

.. important::

   The published metallurgical chain does not close its physical vanadium
   balance: Table S9 reports 0.034 kg V2O5 entering the
   ore route, while Tables S12--S13 describe a 0.0613 kg V2O5 slag stream.
   The source yields are retained, and this discrepancy requires clarification
   before those yields can be revised. Economic allocation and physical
   recovery yields must not be confused.

.. raw:: html

   <span id="additional-battery-components"></span>
   <span id="additional-fitted-monitoring-board-packages"></span>
   <span id="battery-control-connector-material-reference"></span>
   <span id="battery-control-resistor-supplies"></span>
   <span id="battery-monitor-capacitor-families"></span>
   <span id="central-control-fasteners-and-mass"></span>
   <span id="configured-density-targets"></span>
   <span id="control-package-wafer-allocation"></span>
   <span id="dry-zinc-chloride-carrier-water-accounting"></span>
   <span id="flow-battery-aluminium-end-plate-losses"></span>
   <span id="ho-tempo-peroxide-carrier-water"></span>
   <span id="li-o2-final-assembly-electricity"></span>
   <span id="li-o2-module-monitor-manufacturing-reference"></span>
   <span id="li-s-factory-allocation-per-nominal-capacity"></span>
   <span id="lithium-foil-internal-manufacturing-return"></span>
   <span id="magnesium-precipitation-land-boundary"></span>
   <span id="magnesium-precursor-wet-feed-water-accounting"></span>
   <span id="nickel-on-alumina-catalyst-preparation"></span>
   <span id="radiator-and-manifold-aluminium-feed"></span>
   <span id="sixteen-module-detailed-cooling-reference"></span>
   <span id="sixteen-module-monitoring-reference"></span>
   <span id="standalone-interface-and-embedded-residual-boundaries"></span>
   <span id="standalone-li-o2-cooling-supplier"></span>
   <span id="tmatempo-oxidation-carrier-water"></span>
   <span id="vrfb-controller-materials-and-moulding"></span>

See :doc:`battery components </methodology/battery-components>` and :doc:`battery inventories </methodology/battery-inventories>` for these inventory details.
