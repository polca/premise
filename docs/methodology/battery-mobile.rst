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

Lithium hydroxide hydration in custom cathode oxides
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The custom ``NCA oxide`` and ``NMC811 oxide`` activities in
``lci-batteries-NMC111-811-NCA-LFP.xlsx`` buy ``lithium hydroxide, battery grade``.
That packaged product represents LiOH·H₂O, following
`Kelly et al. (2021) <https://doi.org/10.1016/j.resconrec.2021.105762>`__.
The previous quantities, 0.25 and 0.246 kg respectively, were retained from
anhydrous-LiOH recipes. An equal mass of monohydrate supplies less lithium.
The duplicate NCA oxide block in ``lci-batteries-NMC955-LTO.xlsx`` has the
same correction, including its water emission, so import order cannot
restore the old reagent amount.

The correction preserves the rounded anhydrous doses in the existing
workbook. Using Li = 6.94, H = 1.008 and O = 15.999 g/mol, each kg of
anhydrous LiOH requires ``41.962 / 23.947 = 1.75228629891`` kg monohydrate:

.. list-table:: Quantities per kg custom oxide output
   :header-rows: 1

   * - Activity
     - LiOH·H₂O purchased, kg
     - Additional bound water released, kg
     - Total retained water emission, m³
   * - NCA oxide
     - 0.438071574728
     - 0.188071574728
     - 0.000383691575
   * - NMC811 oxide
     - 0.431062429532
     - 0.185062429532
     - 0.000380682430

The added water of crystallisation exits through the existing water-to-air
flow during calcination, using 1,000 kg/m³. The substitution conserves the
LiOH mole dose, and its extra H, O and total mass equal the added emitted
water exactly. It introduces no new reaction-yield or excess-lithium factor.
Precursor quantities, nominal oxide output, electricity and existing
transport activities are retained. The greater monohydrate purchase carries
its supplier's existing upstream production and transport burdens.
No separate drying-energy estimate is added to the existing aggregate
calcination electricity; the changed reagent's thermal demand is unresolved.

The anhydrous source is the
`Crenna et al. (2021) supplement
<https://ars.els-cdn.com/content/image/1-s2.0-S0921344921002287-mmc2.xlsx>`__,
``NMC811`` J191–J196 and ``NCA`` J191–J196. Dai et al. (2018),
`GREET cathode update <https://greet.anl.gov/publication-update_bom_cm>`__,
Table 5, independently reports 0.246 kg LiOH/kg NMC811; its NCA material
entries are unspecified and are not evidence for the 0.25 kg NCA dose.
That dose comes from the Crenna workbook. This correction does not replace
the rounded doses with full-precision or independently optimized recipes.

Only the hydration increment is balanced here. The inherited
``0.00019562 m³`` reaction-water baseline is retained and remains unvalidated.
The original Crenna NCA recipe contains 0.04 kg oxygen and 0.00024 m³ water;
the packaged inventory omits that oxygen and repeats the NMC811 water value.
Those are separate reaction-balance discrepancies. This change does not
establish full atomic closure or the correctness of the complete LTO cell.
Ordinary anhydrous LiOH suppliers, native ecoinvent cathode inventories,
metal-characterization factors and primary/secondary shares are unchanged.
Rebuild scenario databases before deriving updated system results.

Solid-state chemistry mapping and reference inventories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The mobile source forecast distinguishes ``ASSB (polymer)``, ``ASSB (oxidic)``
and ``ASSB (sulfidic)`` from ``SIB``. Earlier versions renamed all three ASSB
categories to SIB before grouping the source data. In MIX in 2040, each category
has a 7.8% share: the previous mapping therefore reported 31.2% sodium-ion
instead of 7.8% sodium-ion and 23.4% solid-state batteries. The forecast ends
in 2040 and its endpoint is held thereafter. In PLiB, the solid-state shares
total 36% at that endpoint. Total-share closure did not detect this composition
error. The SIB inventory represents a sodium-ion NMMT design, not these lithium
solid-state batteries.

The three source identities are now retained and each has its own capacity
market in ``lci-batteries-ASSB.xlsx``. The published reference designs below
are explicit representatives for the forecast categories, not measured fleet
averages. Their nominal installed-capacity basis is independent of cycle life;
vehicle inventories still determine lifetime output and replacements.

.. list-table:: Fixed baseline cell designs and installed pack purchases
   :header-rows: 1

   * - Forecast category
     - Cell design
     - kWh/kg cell
     - kg pack/kWh
   * - ASSB (polymer)
     - LFP / lithium / PEO-LiTFSI, bipolar pouch
     - 0.1708073285
     - 8.065117902
   * - ASSB (oxidic)
     - NMC811 / lithium / LLZO, bipolar pouch
     - 0.3161617540
     - 4.357203949
   * - ASSB (sulfidic)
     - NMC811 / lithium / LiGPS + LiPS, cylindrical
     - 0.3166231749
     - 4.350854113

All designs are held at their published baseline throughout scenario years.
The optimal variants are alternatives, not an assumed annual progression.
No density improvement or probability distribution is inferred. Existing
liquid-electrolyte and sodium-ion trajectories are retained.

Inventory construction and documented adaptations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The polymer and oxide cell materials and manufacturing energy follow the
baseline reference designs of
`Zhang et al. (2025) <https://doi.org/10.1016/j.jclepro.2025.146607>`__.
The author model is pinned to revision
``40c51d668689dd7ce64bd22def55f61d864facc5`` of the
`dimensioning repository <https://github.com/Shan253/All_solid_state_battery_dimensioning_model>`__.
Full-precision outputs retain separate cathode active material, carbon, binder,
lithium, collectors, electrolyte and pouch layers. Their retained dry material
inputs sum to one kg per kg cell. Cathode mass times specific capacity times
voltage reproduces the reported cell energy. Manufacturing energy is converted
from kWh per kWh cell capacity to energy per kg cell using this same density.
German process electricity is an explicit supply-location choice.

The
`electrolyte supplement <https://ars.els-cdn.com/content/image/1-s2.0-S0959652625019572-mmc1.pdf?download=true>`__
provides PEO-LiTFSI mixing/heating/drying (S12) and LLZO synthesis (S13).
S12 labels its polymer as polyethylene although the chemistry is polyethylene
oxide. The explicit PEO material proxy instead uses one kg ethylene oxide per
kg addition polymer; polymerization energy and catalyst coverage remain gaps.
The existing LiS supplement supplies the same LiTFSI salt and lithium foil.
PVDF uses the existing battery inventory's polyvinylfluoride proxy. LLZO
retains the published reagent inputs, emissions, transport and factory;
alumina uses the non-metallurgical market, and unspecified process waste
uses a hazardous-incineration proxy. These choices do not establish complete
manufacturing chemistry or losses not reported by the source.

The sulfide inventory follows the scaled-production reference in
`Rietdorf et al. (2024) <https://doi.org/10.1007/s11367-024-02355-1>`__,
SM1 cell sizing and SM2 ``Scenario 1``. The catholyte contains germanium;
this choice is not representative of every sulfide electrolyte. All SM2
exchanges are multiplied by 0.9934412043717087: this normalizes its gross
NMC811 feed through the published cathode, assembly and final-cell yields
(0.92178405, 0.970299 and 0.95) and 200 Ah/kg at 3.75 V to exactly one kWh.
Retained cathode is 1.333333333 kg/kWh; gross purchases include losses.
Cell mass and capacity use the explicit SM1 mass model rather than its
inconsistent volumetric density. Published germanium resource/heat and
acrylonitrile binder proxies are retained as process-coverage limitations.

HDPE supply uses the native ``market for polyethylene, high density, granulate``
instead of the source's cutoff-specific recycled-material supplier. The kg
input is retained; recycling content follows the target market mix. The 3.10
global market is migrated to the target release's regional markets when needed.

The sulfide coating exchanges are reconstructed from the original
`Chordia et al. (2021) <https://doi.org/10.1007/s11367-021-01976-0>`__
Tables S5-10/S5-11. The later supplement treats its 0.610659 square metres
of plating per source kWh as 0.610659 kg nickel. The original coating needs
``1e-6 m * 8900 kg/m3 / 0.95 = 0.00936842105 kg nickel/m2``.
After capacity normalization, nickel feed for plating is approximately
0.00568339 kg/kWh, not 0.606654 kg/kWh. Water, electricity, emissions, sludge
and wastewater follow the original per-square-metre process too; mg and
cubic decimetres are converted to kg and cubic metres. The nickel-in-sulfamate
water emission uses the dissolved nickel-ion flow proxy.
The area is independently recovered from the surface-cleaning inventory:
9.770544 kg water divided by 16 kg water per square metre (S5-9), with the
same area obtained from its caustic-soda and sulfuric-acid inputs.

Pack hardware and treatment boundary
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each pack uses 0.72591 kg of its own ASSB cell per kg pack. Non-cell components
and assembly are copied from the existing NMC955 pack, including BMS, module
packaging, cooling and pack materials. This is an explicit generic hardware
proxy, not a published ASSB pack design. No conventional cell is copied.
For cell density ``rho``, the capacity market buys
``1 / (0.72591 * rho)`` kg pack per kWh and sends the same mass to the existing
used Li-ion battery market as a treatment proxy. ASSB-specific recovery yields,
pack redesign, lifetime and primary/secondary shares are not asserted.

Coverage checks and reproducibility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before changing any mobile/stationary scenario shares or pack masses, the
battery update verifies that every positive forecast chemistry has a mapped
capacity exchange with the market's unit. A missing mapping, absent exchange
or kilogram exchange in place of kWh raises ``ValueError`` naming the market,
scenario, year and missing shares. Completely represented mixes retain
normalization for source rounding; unsupported positive shares cannot be
silently removed. Interpolation and endpoint clamping are retained. Inventory
linking remains a separate check: a named exchange alone does not establish
that its supplier exists.

``data/battery/solid_state_reference_designs.json`` records source links,
revisions, supplement hashes, full-precision model outputs, source exchange
rows and local adaptations. Run
``python scripts/update_solid_state_battery_inventories.py`` to reconstruct
the workbook without internet or a Brightway database. The script also replaces
three unused zero-share placeholders in each of the four mobile scenario
markets, preserving all other capacity-workbook cells, formulas and caches.
The ``battery`` update uses these distinct suppliers after normal inventory
import and migration. The existing NMC900/NMC900-Si aggregation remains a
separate proxy requiring assessment. Germanium is present in the sulfide LCI;
metal-indicator methods that omit it do not report that demand.

Rebuild scenarios to apply these inventories. Saved scenario databases and
results are not rewritten by the source correction. Arithmetic, unit, mass and
supplier checks do not validate the future commercial design assumptions.

Constant-range vehicle capacity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Marked passenger cars, trucks and electric buses resize their battery capacity
in proportion to operating grid electricity per vehicle-kilometre relative to
the 2020 design. This holds range fixed, with no additional mass/energy feedback
on the IAM efficiency. Battery purchases remain nominal kWh times one plus
lifetime replacements. Energy-density improvements are applied in the capacity
markets, never as a second reduction of these kWh purchases.

Cars retain occupancy; BEV trucks retain payload utilization and adjust their
tonne-kilometre normalization. The ten electric bus variants in
``lci-buses.xlsx`` retain passenger occupancy and the existing replacement
policy. Their capacity-only metadata uses exact inventory exchanges; it does
not infer physical pack mass, road wear or curb mass from rounded comments.
Overnight, opportunity-charging and battery-equipped trolleybuses all use the
same capacity rule. Their original MIX or LTO supplier links are retained.

For fixed lifetime, occupancy and replacement count, both battery capacity and
electricity per kilometre change by the same factor, leaving purchased battery
capacity per MJ constant. For example, the 13 m single-deck overnight bus uses
399 kWh in 2020. If electricity use halves, it uses 199.5 kWh, rather than
allocating the original 399 kWh over half as much lifetime electricity.
Effective battery kg/MJ can still change through market density and chemistry.

Run ``python scripts/update_bus_battery_sizing.py`` after a compatible bus inventory
refresh to regenerate these metadata rows. It checks the source capacity,
lifetime and occupancy before writing and changes no exchange amounts.


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
