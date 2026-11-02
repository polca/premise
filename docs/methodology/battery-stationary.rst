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

These density targets and mass factors are generated from the packaged
configuration. Except for the explicit VRFB whole-system reference below, they
are not whole-pack densities or absolute pack masses.
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
The original publication associates 6 MWh usable output with an 8.3 MWh nominal
label, and gives a 10-year stack life and 20-year system life. The current
reference correction below derives capacity independently from the electrolyte
charge; those original capacity/output numbers no longer define its functional
unit. The legacy assembly name is retained as a stable identifier.

These inventories can be found here: `LCI_vanadium_redox_flow_batteries <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-vanadium-redox-flow.xlsx>`__.

For ecoinvent 3.12, the FKM gasket uses the qualified CLIC reference in
``lci-fkm-gasket-native-312.xlsx``. This source is declared against both
technosphere and biosphere 3.12 and is imported before the VRFB workbook.
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

Before preparing the original VRFB source for 3.12, Premise omits its old
gasket producer and four internal synthesis/compound helpers. Scientific
checks and incoming-reference checks reject changed recipes or newly exposed
helper consumers before changing any payload. Other sources and native
inventories are not rewritten. Targets below 3.12 retain the original route
and the VRFB workbook's 3.9 source declaration. Imported-source caches include
the new workbook and its buyer. The original VRFB workbook is unchanged.

The pinned scientific reference and source qualifications are recorded in
``tests/data/inventory_contracts.json``. The foreground
compilation retains the approved CC BY 4.0 terms and compiler attribution;
external inventories retain their separate terms.

This publication also provides LCIs for Vanadium mining and refining from iron ore.
The end product is vanadium pentoxide, which is available under the following dataset:

* vanadium pentoxide production

These inventories can be found here: `LCI_vanadium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-vanadium.xlsx>`__.

VRFB reference correction: composition, charge and lifetime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The former electrolyte purchased 0.286 kg V2O5, 0.196 kg sulfuric acid,
0.004899 kg phosphoric acid and 0.863 kg water per kg output. These were the
original supporting Table S8's **one-litre, 1.35 kg** batch inputs. In addition
to that unit mismatch, the authors subsequently corrected the excessive V2O5
dose. Their `updated archive <https://zenodo.org/records/10589869>`__ still
warns of another unspecified error. This correction does not identify or claim
to resolve that warning, and does not import the archive's processes or
recycling parameters.

The independent reconstruction and its limitations are recorded in
``data/battery/vanadium_reference_design.json``. Reproduce the workbook edits
offline using ``python tests/support/update_vanadium_reference_inventory.py``.
The authoring script guards the source layout and main coefficients, retains
unrelated formulas and their cached values, and does not rewrite already
corrected files. It does not read or write a Brightway database.

Electrolyte and preparation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
kWh** for the retained system. One nominal kWh buys 59.640627 kg installed
system, including 50.743464 kg electrolyte and **3.041116 kg contained V**.
The legacy activity name containing ``8.3 megawatt hour`` is an identifier,
not a capacity input. The source's 19.4 Wh/kg effective density is not used as
gross capacity, and no extra 21% balance-of-plant mass factor is imposed.

The configuration fixes this whole-system reference at 0.0167670941 kWh/kg
for all years. The original 2020--2050 density trajectory changed the amount
of an unchanged electrolyte without a supporting concentration, voltage or
hardware redesign; it is removed for VRFB only. Old triangular capacity bounds
mixed incompatible designs and are replaced by a deterministic reference.
This does not imply zero engineering uncertainty: for example, changing the
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

The generic legacy waste market used by other chemistries retains its previous
quantities; its dependent Excel formulas are frozen so the VRFB service
correction does not change those other recipes. Existing treatment technology
and primary/secondary market shares remain unchanged. The generic mixed-storage
consumer's lifetime-throughput allocation is a separate limitation; only the
standalone VRFB supplier receives the specific lifetime reconstruction here.

Conditioning imports before the main VRFB workbook, which contains the dedicated
waste market; capacity imports follow both. The two VRFB workbooks and capacity
workbook participate in the import-cache fingerprint, invalidating stale
compact checkpoints through their import-cache references. Rebuild scenarios
to apply this correction. Saved scenario databases, indicators and dashboards
are not regenerated. In-memory tests establish internal concentration, charge,
mass and allocation consistency, not complete commercial-design validation or
corrected system-level vanadium results. Upstream extraction/refining yields
and preparation-equipment coverage remain separate limitations.

Vanadium co-product allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Battery-control resistor supplies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Three reviewed control-electronics references in ``lci-batteries-LiS.xlsx``
use the existing GLO ``market for resistor, surface-mounted`` as a qualified
whole-component manufacturing proxy. The 128-part monitoring-board bundle
purchases **0.00053595 kg**: its **111 nonzero resistors** retain a 0.0004893 kg
mass reference and its **17 zero-ohm jumpers** retain 0.00004665 kg as separate
functional/count provenance. The central-board allowance remains
**0.006122448979591837 kg/kg board**, equivalent to 3 g per 490 g reference;
the standalone interface remains **0.005 kg/kg interface**. The bundle's
part counts are not transferred to those independent mass allowances.

Native whole-resistor supply replaces the custom film, lead-glass and
ruthenium manufacturing chain for these purchases. It does not establish
exact Vishay/Yageo composition, jumper electrical equivalence or complete
Ru/Pd coverage. Native supplier uncertainty remains its own distribution;
equivalence to the former joint uncertainty is not claimed. Other components,
parent purchases, board masses and existing retirement ownership remain
unchanged, with no additional end-of-life purchase or recovery credit.
Existing unused source helpers remain preserved outside these new purchase
paths. Technosphere and biosphere source declarations both stay **3.9**.

The Li-S workbook participates in the imported-inventory cache fingerprint,
so changing these purchases invalidates stale imported sources and their
compact checkpoints. Native source-database caches retain their keys. The
pinned authoring reference is
``tests/data/inventory_contracts.json``.

Battery-monitor capacitor families
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fitted 134-capacitor bundle corrects **39 Class 2 to C0G assignments**:
four 0402, two 0603 and 33 0805 parts. The corrected family counts are
51 Class 2 0402, 25 Class 2 0603, five Class 2 0805, six Class 2 1206,
one flexible 0805, 11 C0G 0402, two C0G 0603 and 33 C0G 0805. The two
KEMET 0402 parts retain their inherited 1.06 mg allowances; the other nine
C0G 0402 parts use the 1.2 mg family reference. These allowances do not
establish the exact fitted-part masses or assays.

The capacitor subtotal changes from **1.27361 g to 0.98098 g** per board.
The **0.29263 g** difference increases the existing unresolved-component
coverage allowance, while the board reference remains **70 g**. Its normalized
residual-proxy purchase increases by ``0.00029263 / 0.07`` kg/kg board.
This is coverage bookkeeping, not a claim of additional measured interface
hardware or laminate. All other parent doses, assembly and retirement stay
unchanged; the board still purchases one 134-part bundle per 70 g reference.

The two C0G 0603/0805 helpers retain the qualified source-family compositions,
95% known-feed retention, utilities and mixed-waste treatment assumptions.
Unknown retained fractions and dopants remain unresolved rather than
renormalized into known ingredients. Existing calcium-zirconate production is
reused once. Both source declarations remain **3.9**. Original row addresses
and formula caches are preserved, with the new helpers appended to the source.
The pinned reference is ``tests/data/inventory_contracts.json``.

Control-package wafer allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CC2662R1FTWRGZRQ1, LM5168FQDDARQ1 and TXU0204QPWRQ1 package references,
and the battery-monitoring ASIC proxy, convert their unchanged silicon masses
to fabricated-wafer area using **270 micrometres** and **2330 kg/m3**:
``area = silicon mass / (2330 * 270e-6)``. Relative to the former
300 micrometre allocation, the wafer purchase increases by **11.111%**.
The 300 micrometre choice remains a diagnostic alternative; neither value
establishes measured fitted-die geometry.

Native fabrication and yields are purchased once. Non-wafer inputs, package
counts, parent purchases, board budgets, assembly and retirement remain
unchanged. The monitoring ASIC retains the imperfect BQ79616 material proxy
for the fitted BQ79718 role; this is not a functional replacement claim.
Package chemistry, fused-silica manufacture and unmodelled phosphorus gaps
remain outside this correction. Both original source declarations stay
**3.9**. The pinned reference is
``tests/data/inventory_contracts.json``.

Battery-control connector material reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The connector recipe uses a qualified material reference based on **five
Molex headers, 36 contacts and 8.594 g**. The additional Samtec geometric
estimate is excluded from this material reference, without asserting that
the header is absent from fitted hardware or that all headers are covered.
The reference is scaled by mass to the unchanged **20 g/module-board** and
**100 g/central-board** allowances.

Plating volumes are subtracted from the finished-contact volume before the
remaining alloy volume is converted to mass. The housing residual then closes
to the fixed reference mass. The thin-layer surface approximation, geometry,
alloy proportions, plating thickness and 70/30 nylon/glass housing remain
engineering assumptions. The **95%** net handling reference is not a measured
plating-deposition efficiency or manufacturing-loss rate.

Nominal rejects remain **0.05263157894736836 kg/kg finished connector**.
Their composition, destination and treatment burdens are unassigned; the
unsupported hazardous-waste incineration purchase is removed. This does not
establish zero physical loss, zero impact or an environmental improvement.
Assembly remains **1 kWh/kg**, with existing board masses, connector doses
and retirement ownership preserved. Both source declarations remain **3.9**.
The pinned reference is ``tests/data/inventory_contracts.json``.

Additional fitted monitoring-board packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 70 g monitoring-board reference itemizes one each of TPS3436BDADADDFRQ1,
TPS71533QDCKRQ1, TPD6E004RSER and ISO7741FQDWRQ1. Their combined full declared
package mass is **0.716853378 g**. Deducting that mass from the current
post-capacitor unresolved allowance changes it from **5.913802956 g** to
**5.196949578 g**, preserving the 70 g board, 134 capacitors, 128 resistors,
existing package doses, parent counts, assembly and retirement.

First-variant material declarations are qualified package-material proxies,
not exact fitted-lot assays. The deduction includes chemically unresolved
additives within each full package mass. Known ingredients are not
renormalized to fill those gaps and missing chemical forms are not invented.
The 270 micrometre wafer-allocation reference and single native fabrication
and yield purchase remain in place. The **1 kWh/kg** package-assembly input
is an engineering allowance. The separate CC2662R1FRGZT and two ESD2CAN24DBZR
devices remain unquantified within residual coverage.

Both source declarations remain **3.9**. The expanded board block is relocated
within the workbook and four package helpers are appended, preserving all
other inventories' row addresses and formula caches. This material reference
does not certify electrical topology, vehicle qualification or complete
manufacturing and trace-metal coverage. The pinned reference is
``tests/data/inventory_contracts.json``.

Standalone interface and embedded residual boundaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The standalone auxiliary interface replaces **0.24 kg glass fibre plus
0.16 kg epoxy** with **0.12987012987012989 m2** of native unmounted Pb-free
through-hole board per kg interface. This uses a qualified **40% finished-board
fraction** and **3.08 kg/m2** two-layer board proxy. Its existing copper
allowance now denotes external component stock rather than known board
tracks; the actual component/wiring partition remains unverified. Other
standalone component allowances and **2 kWh/kg** assembly remain unchanged.

The already detailed 490 g central and 70 g module control boards no longer
purchase the complete auxiliary-interface recipe for unidentified components.
Their unchanged residual masses instead buy direct copper/ferrite/aluminium/
nylon stock at fractions **0.3461538462 / 0.2884615385 / 0.1923076923 /
0.1730769231**, with **2 kWh/kg residual** for component preparation. This
coverage reference adds no second PCB laminate or already itemized capacitors,
resistors, ICs or connectors. It is not measured residual composition and does
not establish zero missing precious metals or complete fabrication coverage.

Existing board budgets, fitted counts, component doses, assembly and retirement
remain unchanged. Both original source declarations remain **3.9**. Other
inventories, formula caches and non-worksheet workbook parts are preserved.
The pinned reference is
``tests/data/inventory_contracts.json``.

Magnesium precursor wet-feed water accounting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The active sodium-ion precursor route keeps synthetic magnesium chloride in
solution before hydrated-lime precipitation. Its MgCl2 reference is a solute
equivalent; acid-carrier and reaction water count toward the downstream brine
water requirement. Per kg magnesium hydroxide, **3.0709019328959926 kg** of
acid-carrier water and **0.3251626038638667 kg** of reaction water meet part
of the existing **104.07159280039637 kg** solution-water requirement.
Additional tap water is therefore **100.67552826363651 kg**, or
**0.9516544877931421 kg/kg brine** at the unchanged **105.79 kg** brine dose.
The corresponding reaction-water emission from the MgCl2 helper is removed.

This is a qualified wet-feed assumption, not measured plant operation or
complete industrial water accounting. Reagents, utility allowances, waste
treatment, precursor quantities and filtrate throughput remain unchanged.
The disconnected legacy recipe and separate land-occupation question remain
outside this correction. Both original source declarations stay **3.9**.
The pinned reference is ``tests/data/inventory_contracts.json``.

The SIB workbook content participates in the import-cache fingerprint so
changed recipes invalidate imported sources and their dependent compact
checkpoints. Native source-database cache keys remain unchanged.

Magnesium precipitation land boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The active synthetic magnesium-hydroxide precipitation stage removes its
inherited **1.63 square meter-year/kg** mineral-extraction occupation.
Upstream supplier inventories retain their land burdens. Own-stage
chemical-plant land remains unquantified, not zero; no substitute factory area
or land transformation is added.

The disconnected legacy recipe and its land exchange, the **0.0275 kg**
NMMT precursor dose, wet-feed water accounting, reagents, utilities, treatment
and uncertainty remain unchanged. Both original source declarations stay
**3.9**. The pinned reference is
``tests/data/inventory_contracts.json``. Existing SIB source
fingerprinting propagates the correction to rebuilt imports.

Nickel-on-alumina catalyst reagent-stock ledger
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The qualified hydrogenation-catalyst reference includes carrier water for
its two assumed **50% nitric-acid and caustic stocks**. The unchanged
**4.131754633808913 kg** dilution-water allowance plus
**1.4205358314581416 kg** carrier water requires **5.552290465267054 kg**
deionised water per kg catalyst. A calculated **0.05738652942776854 kg**
oxygen outlet to generic air completes the nominal manufacturing mass ledger,
without an oxygen sale or recovery credit.

Stock concentrations, catalyst loading and retention, utilities and NOx
capture remain engineering assumptions. The oxygen outlet is calculated,
not measured factory emissions. Existing water, nitrogen and NOx outlets,
other chemicals, residue treatment, uncertainty and consumer quantities stay
unchanged. Both original source declarations remain **3.9**. The pinned
reference is ``tests/data/inventory_contracts.json``.

Organic/hybrid redox-flow workbook content participates in the import-cache
fingerprint. Revisions invalidate imported sources and dependent compact
checkpoints while preserving native source-database cache keys.

4-HO-TEMPO peroxide-carrier water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 4-HO-TEMPO batch counts **0.065 kg carrier water** accompanying its
unchanged **0.065 kg active peroxide**, supplied as **0.130 kg of 50% solution**.
Together with the existing **0.080 kg** initial water, the deionised-water
purchase is **0.145 kg/batch** at the same grade and supplier. Product output
remains **0.108673330176 kg**, and all other exchanges, uncertainty and
consumer quantities remain unchanged.

The added **0.065 kg/batch** retains an unassigned outlet. Evaporation,
recovery, wastewater fate and complete reaction/workup mass closure remain
unresolved. This input correction does not add a new outlet or treatment.
Both original source declarations stay **3.9**. The pinned reference is
``tests/data/inventory_contracts.json``. Existing organic
redox source fingerprinting propagates the correction to rebuilt imports.

TMATEMPO oxidation carrier water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The TMATEMPO oxidation reference counts the full carrier water for its
documented **30% peroxide feed**. The unchanged **0.407459375 kg** active
peroxide carries **0.9507385416666666 kg** water per kg product. Together
with the existing **1.0027 kg** initial-water allowance, total deionised
water is **1.9534385416666666 kg**, replacing the partial-carrier total
of **1.5459791666666667 kg**.

The extra **0.407459375 kg/kg product** retains an unassigned outlet.
Evaporation, recovery, discharge and complete reaction/workup mass closure
remain unresolved. Output, active peroxide, existing neat-caustic and
initial-water basis, pH adjustment, other chemicals, utilities, waste treatment,
uncertainty and consumer quantities remain unchanged. Both original source
declarations stay **3.9**. The pinned reference is
``tests/data/inventory_contracts.json``. Existing
organic-redox source fingerprinting propagates the correction.

Flow-battery aluminium end-plate losses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The nominal plate reference buys **1 kg** generic wrought alloy,
**1 kg** aluminium rolling service and the unchanged **1 kg** average
working service per kg finished plate. The unsupported extra foreground
95% retention allowance and its **0.05263157894736836 kg** incineration
purchase are removed. Native embedded processing stock, scrap, losses,
treatment and uncertainty remain at their version-specific values.

No fixed native-stock cancellation is applied. The reviewed reference's
**0.00298 kg/kg** stock-minus-scrap diagnostic residual remains unallocated;
no new waste, emission or recovery credit is assigned. Alloy-product kilograms
are nominal stock, while service kilograms are process purchases.

Generic alloy and forming remain engineering proxies: the source specifies
**10 mm** plate, whereas native rolling describes **0.2–6 mm** sheet. Geometry
and processing equivalence remain unverified. Plate output, installed parent
quantities and consumers are unchanged. Both original source declarations
stay **3.9**. The pinned reference is
``tests/data/inventory_contracts.json``. Existing organic
source fingerprinting propagates the correction.

VRFB controller mass and moulding inputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Per kg controller, retained PP housing **0.15210236949152542 kg**, LCD
**0.5750483805084746 kg** and electronics **0.27284925 kg** total **1 kg**.
Using the reviewed **0.994** moulding-retention reference once, external
PP feed and injection-moulding service each purchase **0.15302049244620264 kg**.
Native auxiliaries, embedded stock, recorded waste and uncertainty remain
intact; unallocated polymer-residual fate remains unknown.

The inherited **15-inch LCD** is a mass proxy for a **7-inch device**;
dimensional equivalence and complete component manufacture remain unverified.
Suppliers, freight, output and consumer amounts stay unchanged. The parent
**13 kg**, **91% monitoring / 9% BMS** allocation remains an assumption.
Both original source declarations stay **3.9**. Existing profile-specific
supplier mapping and regional PP shares for later targets are preserved.
The pinned reference is ``tests/data/inventory_contracts.json``.
Existing VRFB source fingerprinting propagates the correction.

Lithium-foil internal manufacturing return
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Per kg finished foil, the adopted source convention purchases **1 kg net
external lithium** from lithium-chloride electrolysis and returns manufacturing
offcuts internally. The separate **0.020408163265306145 kg** external hazardous
waste incineration purchase is removed. Actual losses remain unverified;
this convention does not establish measured zero loss or a recovery credit.

The **4.95 kg** density-adjusted rolling-service coefficient and assumed
**0.5 kWh/reference-kg** intensity are preserved, giving **2.475 kWh**.
The coefficient does not establish measured throughput or pass count.
General electricity **2.1 kWh**, cooling **4.2 MJ**, heat **5 MJ**, output and
all consumers remain unchanged. Additional internal handling, remelting,
atmosphere and lubrication burdens remain unquantified. No duplicate factory
is added. Both original source declarations remain **3.9**.

Expanding the rolling helper reproduces the curated CLIC foil exchanges.
The qualification is recorded in the rolling exchange's original ``Comment``
field; the full historical text remains appended. The pinned reference is
``tests/data/inventory_contracts.json``. Existing LiS source
fingerprinting propagates the correction.

Dry zinc-chloride carrier-water accounting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The retained **1.363 kg** nominal dry-salt batch uses **0.72916 kg**
pure-equivalent HCl. Under the adopted **30 wt%** stock-preparation convention,
add **1.7013733333333334 kg** deionised water from Europe without Switzerland:
``0.72916 * (1 / 0.30 - 1)``. This explicit purchase aligns the reference with
curated CLIC; the earlier assertion that no separate water purchase is needed
is superseded in the activity comment, with historical text preserved.

All nine existing exchanges stay unchanged, including **0.0017013733333333333
m3** water to air as liquid-volume-equivalent evaporation accounting,
**0.02 kg** hydrogen, **8.189967723809524 MJ** heat and **0.1363 kWh** electricity.
The dry phase, stock concentration and evaporation/utility allowances remain
engineering assumptions; industrial assay, losses and actual energy demand
are unverified. Preserve the nominal batch rounding without invented closure
or duplicate wastewater. Reagents, freight, output and all consumers remain
unchanged. Both original source declarations remain **3.9**.

Only the new final exchange row, its required worksheet-range extension and
the explanatory comment change. The pinned reference is
``tests/data/inventory_contracts.json``. Existing organic
redox source fingerprinting propagates the correction.

Li-S factory allocation per nominal capacity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The reviewed static reference allocates **5e-7 operating-factory units per
nominal kWh**. At the retained **150 Wh/kg cell**, this is **7.5e-8 factory
units/kg cell**: ``5e-7 * 0.15``. The earlier per-cell conversion is superseded
for this exchange. The worksheet formula and its cached value both use the
corrected capacity basis.

This is a qualified allocation convention; it does not establish measured
industrial productivity or authorize future learning or automatic resizing.
The **47.319 g** ingredient-sum cell basis, specific energy, cell assembly,
formation electricity, factory construction and utility recipe, output and
all consumers remain unchanged. Both original source declarations remain
**3.9**.

Only the factory-dose cell and its exchange comment change. The comment
preserves the historical text; unrelated uses of the original shared string
remain untouched. The pinned reference is
``tests/data/inventory_contracts.json``. Existing LiS
source fingerprinting propagates the correction.

Li-O2 final-assembly electricity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Apply **0.41 MJ** of final-assembly electricity once per complete **333.85 kg**
manufacturing reference. The corresponding US medium-voltage electricity
purchase is **0.00034113790291714506 kWh/kg battery**, equivalent within rounding
to ``0.41 / 3.6 / 333.85``. This final-system assembly allowance is separate
from existing cell processing and casing manufacture. It is not a per-cell
allowance or a charge per delivered or lifetime kWh.

The detailed component sum remains an adopted engineering reference; the
source's **321 kg system / 267 kg pack** headline discrepancy remains explicit.
The US grid is the retained source proxy. All eight existing exchanges,
including the zero duplicate-retirement fragment, output, upstream recipes and
consumers stay unchanged. Both original source declarations remain **3.9**.
Cooling and control differences from curated CLIC remain separate; this
addition does not establish complete recipe equivalence or authorize future
learning.

Only the final-assembly exchange and activity comment change. The comment
retains the full historical text. The pinned reference is
``tests/data/inventory_contracts.json``. Li-O2 source
fingerprinting invalidates affected imported-inventory caches and dependent
compact checkpoints after workbook changes while preserving native database
cache paths.

Standalone Li-O2 cooling supplier
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Standalone battery manufacture reuses the existing **RER** ``cooling system
production, for Li-O2 battery`` reference. It includes radiator, manifolds,
clamps, pipe fitting, thermal pad, glycol and freight. The purchase remains
**0.08707503369776846 kg/kg battery**, representing **29.07 kg** cooling per
**333.85 kg** system; its formula and cached value remain unchanged.

This is a substantive modelling choice: it supersedes the simplified GLO
**7 kg aluminium / 22.07 kg pure glycol** choice for this consumer. The RER
composition remains an inherited engineering proxy; battery-specific thermal
design and performance are unverified. Its **0.99996 kg** nominal material sum
retains source rounding without invented closure.

Only the consumer's supplier name, location, reference product and activity
comment change. All helper recipes, other consumers, output and the approved
final-assembly electricity remain intact. The GLO helper remains available for
its other existing uses. Control alignment remains separate. Both original
source declarations remain **3.9**. The pinned reference is
``tests/data/inventory_contracts.json``. Existing Li-O2 source
fingerprinting propagates the correction.

Central-control fasteners and coupled mass accounting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The central-control material reference uses **2.8 g** fasteners, retaining
**0.49 kg** mounted board, **2.8 kg** HV equipment and **1.2 kg** LV equipment.
Its nominal mass is **4.4928 kg**, replacing **4.518 kg**. This remains a
qualified material reference, not a verified manufacturer BOM, actual circuit
topology or a literal fractional installed-controller count.

The correction updates **31 amount cells and 15 scoped notes** across four
workbooks. Fixed controller counts stay fixed. Li-S auxiliary retirement is
**15.5924 kg**, preserving its **1.1 kg** monitoring and **9.9996 kg** cooling
references. This does not adopt CLIC's different combined **15.6128 kg**
reference. Nine NMC955/LTO control references each retire **25.2 g** less,
with their other manufacturing purchases and treatment routes preserved.

Li-O2 keeps its **4 kg** central-control allowance by purchasing
``4 / 4.4928`` reference units, or ``4 / 4.4928 / 333.85`` per kg complete
battery. Its **88.07 kg** auxiliary retirement, final-assembly electricity
and detailed RER cooling input remain unchanged.

The buffer-pack references retain **44.6 kg/car pack**, **238 kg/truck pack**,
the **70%** cell share, controller counts and total retirement. Existing
residual-hardware proportions distribute the freed **25.2 g** per pack,
including corresponding working services. This preserves the existing
engineering allocation; it does not establish measured residual composition.
Indirect ASSB controller counts and the historical ``143 - 6.538`` hardware
partition remain intact; removed fastener mass is not reassigned to ASSB
materials.

Each original technosphere/biosphere pair remains unchanged: **3.9** for
Li-S/Li-O2, **3.8** for NMC955/LTO and **3.10** for battery capacity. The source
quantities remain deterministic. The pinned reference is
``tests/data/inventory_contracts.json``. NMC955/LTO joins the
existing source-content cache fingerprints so corrections invalidate imported
inventory caches and dependent compact checkpoints. Native cache paths stay
unchanged. Other monitoring, cooling and source-profile differences from CLIC
remain separately reviewed.

Sixteen-module monitoring reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 16-module control assembly adopts **16 monitoring boards at 70 g each**,
purchasing **1.12 kg** of the existing mounted-board recipe. The source's
**1.1 kg** allowance remains documented as a distinct mass reference; it is
not declared a proven error. Actual board masses, control topology and the
complete fitted BOM remain qualified engineering assumptions.

Retirement is **15.6124 kg**, comprising **4.4928 kg** central control,
**1.12 kg** monitoring and the existing **9.9996 kg** cooling materials. Central
and cooling counts, supplier recipes, module mass, lifetime and capacity
purchases stay unchanged. CLIC's different **15.6128 kg** combined allowance
is not adopted while its cooling reference remains a separate decision.

Only Li-S amounts ``B1132/B1134``, its activity note ``B1126`` and capacity
worksheet notes ``L132/L158`` change. The latter are existing free worksheet
annotations outside the exchange header; no new exchange metadata fields are
introduced. All capacity inventories and their purchases remain unchanged.
Original paired source declarations remain **3.9** for Li-S and **3.10** for
capacity. Existing Li-S/capacity cache fingerprints propagate the correction.
The pinned reference is ``tests/data/inventory_contracts.json``.

Radiator and manifold aluminium feed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing RER radiator and manifold references each purchase **1 kg** from
``market for aluminium, wrought alloy`` (GLO), replacing the mixed-metal-scrap
input. The former input represented scrap rather than an established finished
metal supply before forming. The wrought-alloy market is a qualified stock
proxy; actual alloy grade and recycled content remain unverified. No claim of
**100% secondary aluminium** is made.

All stock quantities, outputs, forming, freight, infrastructure, retirement,
other exchanges, formulas and caches remain unchanged. No additional remelting
process, recovery credit or yield correction is added. Native processing stock,
losses and uncertainty remain untouched. Original technosphere and biosphere
declarations both remain **3.9**. This feed correction does not certify the
other inherited thermal, forming or equipment assumptions.

The pinned reference is
``tests/data/inventory_contracts.json``. Matching new
CLIC revision candidates for datasets **1151/1152** are added to the curated
plan, preserving published base versions. Candidate adoption requires the next
handoff's dependency and selector validation; no CLIC database or publication
is modified by the source authoring script. Further Li-S cooling alignment
remains a separate decision. Existing Li-O2 source fingerprinting propagates
the corrected component recipes.

Sixteen-module detailed cooling reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent approved cooling decision supersedes the cooling and retirement
allowances in the preceding 16-module monitoring subsection. This assembly
now purchases **10 kg** of the existing detailed RER Li-O2 cooling reference,
including the corrected wrought-alloy radiator and manifold inputs. The GLO
unit cooler remains available for its other consumers. The inherited RER
forming services, regional inputs, freight and infrastructure remain qualified
engineering assumptions; thermal performance, alloy grade and procurement are
not established measurements. The full pad/pipe injection-service convention
is retained without changing native yields or processing coefficients.

Retirement is **15.6128 kg**: **4.4928 kg** central control, **1.12 kg** monitoring
and **10 kg** nominal cooling purchase. Listed cooler materials sum to
**9.9996 kg**; the **0.0004 kg** gap remains documented as source rounding.
No filler, yield correction or recovery credit is introduced. Supplier recipes,
other consumers, module mass, lifetime and capacity purchases are preserved.

Only six Li-S exchange cells and its activity note change. The two existing
capacity worksheet annotations are updated without altering any of its 35
imported inventories or adding exchange metadata. All original formulas and
cached values remain intact. Original technosphere/biosphere pairs remain
**3.9** for Li-S and Li-O2 and **3.10** for capacity; alignment of the assembly
recipe does not establish full background-graph or LCIA equivalence across
profiles. The pinned reference is
``tests/data/inventory_contracts.json``. Existing source-content
fingerprints propagate this correction to imported inventory caches.

Li-O2 module-monitor manufacturing reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Li-O2 module-packaging reference purchases the existing GLO mounted
battery-module monitor board recipe in place of the generic mounted-board
market. The quantity remains **0.67 / 19.51 = 0.03434136340338288 kg per kg**
of module hardware. This is a mass-based manufacturing proxy; it does not
establish a verified Li-O2 circuit, manufacturer BOM, topology or integer
board count. The former generic-board choice is not declared a transcription
error. Other module-packaging recipe differences remain separately reviewed.

Only the supplier name, reference product and a scoped activity comment
change. Every amount, uncertainty, formula, cached value, consumer coefficient
and retirement quantity remains unchanged. The existing monitor-board recipe
is reused without modification. Original technosphere and biosphere versions
both remain **3.9**. The reference is
``tests/data/inventory_contracts.json``. Existing Li-O2
source-content cache fingerprints propagate the supplier correction.

Additional-inventory baseline bookkeeping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The three Schmidt-derived multilayer-pouch references purchase **0.5 kg**
aluminium and retain **0.5 kg** aluminium rolling per kilogram of pouch.
Their listed polymers total **0.503 kg**, so the **1.003 kg** material total
retains source rounding. No extra yield, normalization or recycling credit
is inferred. The complete NMC532 battery market purchases **one kilogram**
of complete pack manufacture per kilogram sold; its cell share belongs in
pack manufacture, not in the market a second time. The BMS production
reference product matches its existing parent declaration.

The three all-solid-state pack references retain their existing cell fraction
and electricity purchase while using the reviewed shared passive-material
composition. This is a qualified engineering reference, not a measured
chemistry-specific pack bill of materials. No controller mass is reassigned.

Home-storage modules retain their sea and road delivery purchases and disable
the additional unsupported rail purchase. The NMC811 route retains its
approved GLO cell-market reference. Whole-system road delivery follows the
manufactured component budgets: **88.36128 tonne-km** for LFP and
**84.86574428571429 tonne-km** for NMC811. Existing nominal capacities,
160/280 Wh/kg cell references, endpoint-mean replacement convention and
assembly electricity remain unchanged; these are engineering assumptions,
not newly measured geometry or component survival.

These edits preserve the original source-version declarations, all formulas
and cached values, and unrelated imported records. Content fingerprints now
also cover both NMC source workbooks and the home-storage workbook, so their
changes invalidate imported inventories and dependent compact checkpoints
without changing native-source cache paths. Legacy ``lci-PV.xlsx`` remains
outside the default import set; its ordinary CIS-diode identity correction
does not change the active CIGS model or add it to the import fingerprint.
