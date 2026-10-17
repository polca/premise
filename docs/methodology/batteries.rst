Batteries and storage
=======================

.. raw:: html

   <span id="batteries"></span>


These chapters describe battery manufacturing inventories, energy-density
changes and mobile or stationary technology mixes. They do not by themselves
define the storage operation included in an electricity market.

For storage inputs and losses in electricity supply, see
:ref:`electricity-storage-operation`. Vehicle fleets are described separately
under :doc:`transport/index`.

.. toctree::
   :maxdepth: 2

   /methodology/battery-mobile
   /methodology/battery-stationary

NMC955 dry-anode boundary correction
------------------------------------

The supplemental ``lci-batteries-NMC955-LTO.xlsx`` workbook supplies a dedicated
dry anode mixture to NMC955 cells. Previously, the cell bought 0.2 kg of a
complete silicon-graphite anode and another 0.12 kg of copper collector foil.
The complete anode already contained 62% dry paste and 38% collector foil.
That combination supplied 0.124 kg paste and 0.196 kg foil per nominal kg of
cell. The corrected cell buys 0.2 kg dry paste and 0.12 kg separate foil.
The cell's other material and assembly-energy exchanges are retained.

The original workbook instruction to replace 9% of graphite with silicon is
interpreted as replacement within the original 94% graphite pool. It is a
documented model choice, giving the following composition:

.. list-table:: Retained dry-anode ingredients
   :header-rows: 1

   * - Ingredient
     - kg per kg dry paste
     - kg per nominal kg cell
   * - Synthetic graphite
     - 0.8554 = 0.94 × 0.91
     - 0.17108
   * - Elemental silicon
     - 0.0846 = 0.94 × 0.09
     - 0.01692
   * - Carbon black
     - 0.01
     - 0.002
   * - Carboxymethyl cellulose
     - 0.03
     - 0.006
   * - Styrene-butadiene rubber
     - 0.02
     - 0.004

The ingredient and manufacturing basis is the
`Crenna et al. (2021) supplementary workbook
<https://ars.els-cdn.com/content/image/1-s2.0-S0921344921002287-mmc2.xlsx>`__,
sheet ``NMC811``, rows 145–154. That source contains a 2% silicon mixture;
8.46% silicon is the explicit Premise variant, not the published composition.
Synthetic graphite follows the original source recipe, replacing the mixed
natural/synthetic route of the previously linked complete-anode supplier.
The stated 1/3/2% additive/binder shares close the dry mixture exactly, without
the source workbook's small binder-rounding excess.

Per kg dry paste, processing retains the source's 0.46810728671732227 kWh of
mixing/cutting/calendaring electricity (J151), 8.48487394434316 MJ of drying
heat (J150), 0.96 kg of deionised water (J149), 0.00096 m³ of wastewater
(J152), and 4 × 10\ :sup:`−10` units of chemical factory (J153). Water is a
process input, excluded from retained dry solids. Wastewater uses a negative
technosphere amount to request treatment through the waste market. Electricity
uses Chinese supply. SBR follows the original source's ``latex`` market (J148),
whose native production documentation describes styrene-butadiene emulsion
polymerisation. This is a nominal dry-binder proxy; no wet-latex concentration
or new binder process is modelled. The source-version graphite identity follows
normal inventory migration. These process quantities are an
explicit proxy for the revised silicon mixture. Collector-foil manufacture is
supplied separately, and cell-assembly energy is retained at cell level.

Capacity and remaining coverage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The corrected anode contains 0.17108 kg graphite and 0.01692 kg silicon per
nominal kg cell. The retained positive electrode contains 0.3895 kg of
LiNi\ :sub:`0.9`\ Co\ :sub:`0.05`\ Mn\ :sub:`0.05`\ O\ :sub:`2`.
A deliberately generous screen uses one electron per formula unit,
LiC\ :sub:`6` and Li\ :sub:`15`\ Si\ :sub:`4`, with mean voltage 3.7 V:
the positive and negative materials support approximately 107.117 and
124.175 Ah per nominal kg cell. This passes a theoretical charge screen;
it does not establish practical discharge performance.

At the unchanged 2050 capacity-market mass, that theoretical screen gives
1.06636 kWh per nominal kWh. A separate published cell-design benchmark of
230 Ah/kg positive active material and 3.73 V gives 0.89905 kWh instead
(`Stephens et al., 2025, Table 4
<https://doi.org/10.1016/j.jpowsour.2025.236170>`__). That benchmark is not a
validated NMC955 replacement design. Practical utilization, reversible silicon
capacity, voltage curves, first-cycle lithium loss, expansion, manufacturing
yield and lifetime still require assessment.

The cell's other retained material inputs sum to 0.9919 kg per nominal kg
cell, including this dry paste. The remaining 0.0081 kg source rounding/coverage
gap is reported without adding an assumed material. The capacity markets also
retain their separate 71% cell-mass assumption, while the pack contains
72.591% cells. Neither that allocation mismatch nor the energy-density
trajectory is changed by the anode correction. Rebuild scenario databases to
apply the new inventory; saved results remain unchanged until regenerated.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
------------------------

.. include:: /reference/generated/source-batteries.inc
