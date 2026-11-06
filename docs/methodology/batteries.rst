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

.. _lfp-curated-reference:

Curated LFP manufacturing reference (ALIGN088)
----------------------------------------------

The complete-pack activity ``market for battery, Li-ion, LFP, curated reference``
follows the approved **CLIC dataset 1235** layout on a **203.4928 kg** pack basis:
196.85 kg of hardware including 147.58 kg of cells, one 4.4928 kg central control
unit, sixteen 0.07 kg monitoring boards (1.12 kg), and 1.03 kg of auxiliary
controls. Exchanges are scaled to
one kg of complete pack. This is a static engineering manufacturing reference;
retirement and end-of-life credits belong to separate lifecycle inventories.

The cell supply follows the target ecoinvent version:

* For **3.8**, ``lci-batteries-NMC111-811-NCA-LFP.xlsx`` retains its authored
  source version **3.8** and the legacy GLO cell-manufacturing recipe. Its
  original ``market for battery cell, Li-ion, LFP`` and five freight inputs
  remain available to existing buyers. The additional
  ``market for battery cell, Li-ion, LFP, curated supply`` buys exactly one kg
  of that original market; this alias adds no manufacture or freight.
* From **3.9** onward, ``lci-batteries-LFP-native-312.xlsx`` is imported with
  authored source version **3.12** and adapted to the target through the
  ordinary inventory migration route for technosphere and biosphere exchanges.
  Its curated cell market retains five freight inputs and buys a GLO wrapper
  that purchases one kg of native CN LFP cells. The native CN supplier is
  available from 3.9; the legacy recipe supplies the 3.8 route.

Both routes use the same reviewed pack layout. The custom root and curated
cell-market names distinguish these added foreground activities from native
ecoinvent inventories, whose names remain unchanged. The complete pack retains
the reference product ``battery, Li-ion, LFP, rechargeable, prismatic``, GLO
location and kilogram unit. The authored source profile remains attached to
each workbook when its exchanges are migrated to another target version.

Li-O2 capacity-route auxiliaries
--------------------------------

The separated Li-O2 capacity route uses the same qualified RER cooling
reference as standalone battery manufacture: 29.07 kg per physical reference
system of 63.5 gross kWh. The fixed auxiliary unit retains the 55 kg oxygen
tank and 4 kg central-control allowance, with its existing 88.07 kg retirement
quantity. The cooler remains an engineering proxy, rather than a measured
Li-O2 thermal design.

The auxiliary unit also owns the existing 0.41 MJ final-assembly allowance,
purchased as 0.41/3.6 kWh of US medium-voltage electricity. This counts assembly
once when the capacity route bypasses full-battery manufacture. The standalone
route keeps its own assembly input. Source technosphere and biosphere versions
both remain 3.9; ordinary inventory migration adapts the exchanges to the target.

The separated route retains the four finished-system delivery legs from the
full-battery market. The cell provider owns delivery per kg of cells; the
fixed component provider owns delivery for all 151.25 kg of noncell equipment
per 63.5 gross kWh, including the 88.07 kg auxiliary system. The auxiliary
provider has no second delivery charge. This reproduces the original
333.85 kg system at baseline while existing cell-density changes affect
only the cell share of delivery. Component-manufacturing transport remains
in its original suppliers.

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

NMC955 cell-to-pack capacity allocation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Both mobile and stationary NMC955 capacity markets use the manufactured pack's
0.72591 kg cells/kg pack, replacing the generic 0.71 assumption. At cell
density ``rho`` in kWh/kg cell, one kWh requires
``1 / (rho * 0.72591)`` kg pack. The pack recipe's remaining 27.409% consists
of other pack components. The 2020 cell densities remain 0.300, 0.340 and
0.380 kWh/kg cell for the minimum, mean and maximum respectively.

The resulting baseline pack purchase is 4.051709537805285 kg/kWh, with
mass bounds 3.625213796983677–4.591937476179324 kg/kWh. Minimum mass corresponds
to maximum density. This is a 2.19173% decrease from the previous 4.142502071251036
kg/kWh mean. Manufacturing and end of life use equal pack mass; disposal is
negative, with bounds equal to the negatives of the opposite manufacturing
bounds. The triangular uncertainty type and disposal sign flag are retained.
The workbook formulas reference their cell-fraction metadata field, and their
cached values are updated for direct import.

The existing annual density trajectory is retained. The source correction
sets the baseline bounds, which follow the inverse-density rule described
below when projected to future years. Matching nominal cell and pack masses
does not validate the projected cell performance or replacement schedule.

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

After the cell-fraction correction, the retained 2050 density trajectory buys
3.625213796983677 kg pack per nominal kWh, containing 2.631578947368421 kg cells.
At this mass, the theoretical screen gives 1.04298 kWh per nominal kWh.
A separate published cell-design benchmark of 230 Ah/kg positive active
material and 3.73 V gives 0.87935 kWh instead
(`Stephens et al., 2025, Table 4
<https://doi.org/10.1016/j.jpowsour.2025.236170>`__). That benchmark is not a
validated NMC955 replacement design. Practical utilization, reversible silicon
capacity, voltage curves, first-cycle lithium loss, expansion, manufacturing
yield and lifetime still require assessment.

The cell's other retained material inputs sum to 0.9919 kg per nominal kg
cell, including this dry paste. The remaining 0.0081 kg source rounding/coverage
gap is reported without adding an assumed material. The capacity-market
allocation now matches the pack's 72.591% cell fraction; the energy-density
trajectory remains a separate assumption. Rebuild scenario databases to
apply the new inventory; saved results remain unchanged until regenerated.

Battery mass uncertainty over time
----------------------------------

For fixed capacity and cell mass fraction, pack mass is inversely proportional
to cell energy density. When scaling triangular mass uncertainties, the lower
positive mass bound therefore follows the maximum-density trajectory and the
upper positive bound follows the minimum-density trajectory. Signed negative
disposal bounds follow the opposite ordering: their lower bound is the negative
of maximum mass and their upper bound is the negative of minimum mass.
The scaling uses each numerical bound's sign, so positive magnitude bounds
combined with a negative sign flag retain the positive-mass ordering.

For NMC955 in 2050, using the configured cell-density range 0.34–0.40 kWh/kg
and the manufactured cell fraction 0.72591, manufacturing mass bounds are
3.443953107134493–4.051709537805285 kg/kWh. The previous runtime calculation
gave 3.198718056162067–4.362340602370358 kg/kWh. Equal-mass signed disposal
bounds now mirror manufacture at −4.051709537805285 to −3.443953107134493
kg/kWh. The deterministic manufacture/disposal amounts remain
±3.625213796983676 kg/kWh.

This correction preserves deterministic amounts, triangular modes, uncertainty
types and sign flags. Linear interpolation between 2020 and 2050 and endpoint
values outside that interval are retained. It concerns marginal parameter
bounds and does not define correlations between exchanges or validate the
underlying density or lifetime assumptions. Rebuild scenario databases to
apply the runtime correction.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
------------------------

.. include:: /reference/generated/source-batteries.inc
