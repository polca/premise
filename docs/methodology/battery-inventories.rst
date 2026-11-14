Shared battery inventories
==========================

This page explains the manufacturing assumptions shared by the
:doc:`battery-mobile` and :doc:`battery-stationary` chapters. The application chapters explain scenario
shares and consumer links; the recipes here describe particular source profiles.
Complete-pack and separated-cell capacity routes have different boundaries and
must be read with their own quantities and assumptions.

.. contents:: On this page
   :local:
   :depth: 2

.. _lfp-curated-reference:

LFP manufacturing reference
------------------------------

The complete-pack activity `market for battery, Li-ion, LFP, curated reference <https://clic.psi.ch/datasets/market-for-battery-li-ion-lfp-rechargeable-prismatic/>`__
follows the **CLIC dataset 1235** layout on a **203.4928 kg** pack basis:
196.85 kg of hardware including 147.58 kg of cells, one 4.4928 kg central control
unit, sixteen 0.07 kg monitoring boards (1.12 kg), and 1.03 kg of auxiliary
controls. Exchanges are scaled to
one kg of complete pack. This is a static engineering manufacturing reference;
retirement and end-of-life credits belong to separate lifecycle inventories.

The cell supply follows the target ecoinvent version:

* For **3.8**, the legacy LFP cell reference retains its authored
  source version **3.8** and the legacy GLO cell-manufacturing recipe. Its
  original `market for battery cell, Li-ion, LFP <https://clic.psi.ch/datasets/market-for-battery-cell-li-ion-lfp/>`__ and five freight inputs
  remain available to existing buyers. The additional
  `market for battery cell, Li-ion, LFP, curated supply <https://clic.psi.ch/datasets/market-for-battery-cell-li-ion-lfp/>`__ buys exactly one kg
  of that original market; this alias adds no manufacture or freight.
* From **3.9** onward, the `curated LFP reference <https://clic.psi.ch/datasets/market-for-battery-li-ion-lfp-rechargeable-prismatic/>`__ is imported with
  authored source version **3.12** and adapted to the target through the
  ordinary inventory migration route for technosphere and biosphere exchanges.
  Its curated cell market retains five freight inputs and buys a GLO wrapper
  that purchases one kg of native CN LFP cells. The native CN supplier is
  available from 3.9; the legacy recipe supplies the 3.8 route.

Both routes use the same pack layout. The custom root and curated
cell-market names distinguish these added foreground activities from native
ecoinvent inventories, whose names remain unchanged. The complete pack retains
the reference product ``battery, Li-ion, LFP, rechargeable, prismatic``, GLO
location and kilogram unit. The authored source profile remains attached to
each inventory source when its exchanges are migrated to another target version.

Li-O2 capacity-route auxiliaries
--------------------------------

**CLIC dataset pages:** `cooling system production, for Li-O2 battery <https://clic.psi.ch/datasets/cooling-system-production-for-li-o2-battery/>`__; `battery auxiliaries supply and end-of-life treatment, lithium-oxygen <https://clic.psi.ch/datasets/battery-auxiliaries-supply-and-end-of-life-treatment-lithium-oxygen/>`__.

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

NMC955 manufacturing references
-------------------------------

The complete-pack and separated-cell references below retain different
capacity bases, binder compositions and manufacturing boundaries. Their
coefficients describe their respective profiles and are not interchangeable.

Complete-pack reference
~~~~~~~~~~~~~~~~~~~~~~~

Dry-anode composition
^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `anode paste production, silicon-graphite, for lithium-ion battery, nmc955 <https://clic.psi.ch/datasets/anode-paste-production-silicon-graphite-for-lithium-ion-battery-nmc955/>`__.

The `NMC955 dry-anode reference on CLIC <https://clic.psi.ch/datasets/anode-paste-production-silicon-graphite-for-lithium-ion-battery-nmc955/>`__
supplies **0.2 kg dry paste** per nominal kg cell. The cell purchases its
**0.12 kg copper collector foil** separately, while assembly energy belongs
to the cell inventory.

The original inventory instruction to replace 9% of graphite with silicon is
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
The paste uses synthetic graphite as its carbon-active-material reference.
The stated 1/3/2% additive/binder shares close the dry mixture exactly, without
the source inventory's small binder-rounding excess.

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

The complete-pack NMC955 capacity route uses the manufactured pack's
0.72591 kg cells/kg pack. At cell
density ``rho`` in kWh/kg cell, one kWh requires
``1 / (rho * 0.72591)`` kg pack. The pack recipe's remaining 27.409% consists
of other pack components. The 2020 cell densities remain 0.300, 0.340 and
0.380 kWh/kg cell for the minimum, mean and maximum respectively.

The resulting baseline pack purchase is 4.051709537805285 kg/kWh, with
mass bounds 3.625213796983677–4.591937476179324 kg/kWh. Minimum mass corresponds
to maximum density. Manufacturing and end of life use equal pack mass; disposal is
negative, with bounds equal to the negatives of the opposite manufacturing
bounds. The triangular uncertainty type and disposal sign flag are retained.

The baseline mass bounds follow the inverse-density rule described
below when projected to future years. Matching nominal cell and pack masses
does not validate the projected cell performance or replacement schedule.

Capacity and remaining coverage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The anode contains 0.17108 kg graphite and 0.01692 kg silicon per
nominal kg cell. The retained positive electrode contains 0.3895 kg of
LiNi\ :sub:`0.9`\ Co\ :sub:`0.05`\ Mn\ :sub:`0.05`\ O\ :sub:`2`.
A deliberately generous screen uses one electron per formula unit,
LiC\ :sub:`6` and Li\ :sub:`15`\ Si\ :sub:`4`, with mean voltage 3.7 V:
the positive and negative materials support approximately 107.117 and
124.175 Ah per nominal kg cell. This passes a theoretical charge screen;
it does not establish practical discharge performance.

The 2050 density trajectory buys
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
gap is reported without adding an assumed material. The capacity market uses the pack's 72.591% cell fraction; its energy-density
trajectory is a separate modelling assumption.

.. _nmc955-separated-cell-reference:

Separated-cell reference
~~~~~~~~~~~~~~~~~~~~~~~~

NMC955 reference capacity and fixed hardware
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `market for battery capacity, Li-ion, NMC955 <https://clic.psi.ch/datasets/market-for-battery-capacity-li-ion-nmc955/>`__; `battery production, Li-ion, NMC955 <https://clic.psi.ch/datasets/battery-production-li-ion-nmc955/>`__; `battery monitoring and central control system production, lithium-ion, nmc955 <https://clic.psi.ch/datasets/battery-monitoring-and-central-control-system-production-lithium-ion-nmc955/>`__.

The current NMC955 reference contains 108.03 kg of cells providing
28.0829301277 kWh, corresponding to 259.954921112 Wh/kg cell. The configuration's
340 to 380 Wh/kg trajectory is used as a relative growth index for this
reference. Its effective 2050 cell density is therefore
``259.954921112 * 380 / 340 = 290.537853007 Wh/kg``, not 380 Wh/kg.
The cell purchase decreases from 3.84682080925 to 3.44189230301 kg/kWh.

Non-cell materials remain at 1.34280859684 kg/kWh, with a separately supplied
6.5128 kg control system per 28.0829301277 kWh reference. The non-cell helper
contains 37.71 kg of materials per reference, not a second 145.74 kg complete
electronics-free pack: the latter includes the 108.03 kg of cells. Cells,
non-cell materials and controls each include their respective end-of-life
provision once.

Holding pack hardware and controls fixed is an explicit engineering
counterfactual, not a measured future design. The reference does not establish
future current ratings, thermal design or chemistry-specific pack geometry.
The source cell design and relative learning assumption are distinct from
the absolute values displayed in the density configuration. This capacity
normalization does not independently validate upstream cell-manufacturing
recipes or their utilities.

NMC955 factory-energy reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The modern NMC955 cell reference transfers the factory-energy quantities in
`Dai et al. (2019) <https://doi.org/10.3390/batteries5020048>`__: 30 MJ of
electricity and 140 MJ of steam per kWh of cell capacity. At the approved
259.954921112 Wh/kg reference density, this gives 2.16629100927 kWh electricity
and 36.3936889557 MJ steam per kg of delivered cell. The native chemical-industry
steam supplier documents 2.75 MJ/kg, giving a purchase of 13.2340687112 kg steam
per kg cell. Its RoW supply is a geographic/process proxy.

These quantities transfer observations from an older Chinese NMC manufacturing
plant to the reconstructed NMC955/silicon-graphite design. They are not measured
utilities for that design or a validated future factory. Factory energy is
included once at the cell and is not additionally multiplied by the material
reject factor. Existing density scaling remains: with utilities fixed per kg
cell, utility purchases per kWh capacity decrease with the cell-mass factor.
There is no separate forecast of manufacturing-energy efficiency. This energy
assumption does not validate the material-yield or electrode recipes.

NMC955 manufacturing yield and rejects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The modern reference assumes a static 93% final-cell acceptance yield. One kg
of accepted cell therefore requires ``1 / 0.93 = 1.07526881720`` kg of assembled
cells and produces ``1 / 0.93 - 1 = 0.07526881720`` kg of manufacturing rejects.
This final quality-control loss is distinct from upstream foil/film production
losses already included in their suppliers. Material and associated forming
service quantities include the reject factor; the capacity-based factory-energy
reference is not multiplied a second time.

The source is a transferred process assumption from Table 9 of
`Argonne ANL/CSE-24/1 (2024)
<https://publications.anl.gov/anlpubs/2024/01/187177.pdf#page=10>`_, which assumes
93% yield in 2030 and 95% in 2035 for different high-nickel designs. These are
cost-model assumptions, not measured NMC955 plant yields. The current reference
holds 93% constant rather than importing that report's complete technology/year
trajectory. This does not validate the timing or composition of real rejects.

Manufacturing rejects use the existing used-Li-ion-battery treatment market
as a generic proxy for fully assembled failed cells. They are separate from
the retained cells retired later through the capacity supplier. The negative
treatment purchase follows the supplier's negative-output convention and
contributes a positive burden in the cut-off case; it is not an
additional foreground recovery credit.

NMC955 dry anode and collector boundary
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `anode paste production, silicon-graphite, for lithium-ion battery, nmc955 <https://clic.psi.ch/datasets/anode-paste-production-silicon-graphite-for-lithium-ion-battery-nmc955/>`__.

The modern reference supplies one kg of dry anode paste containing 0.8554 kg
synthetic graphite, 0.0846 kg metallurgical silicon, 0.01 kg carbon black,
0.02 kg CMC and 0.03 kg SBR. The mixture closes to one kg; silicon is 9% of
the 94% graphite-plus-silicon active-material pool, not 9% of the whole paste.
The CMC/SBR/carbon-black matrix is an engineering transfer attributed to
`Wolf et al. (2023) <https://doi.org/10.3390/wevj14040087>`_. The silicon ratio
retains the declared NMC955 adaptation.

The dry-paste provider excludes copper foil and wet electrode processing.
The cell purchases its collector separately and owns the aqueous carrier,
mixing/coating/drying energy and evaporated water once. The former complete-anode
supplier included copper foil and a different composition, so its per-kg score
is not directly comparable to a kg of the new dry mixture.

All-synthetic battery-grade graphite is a reference supply choice, not a
forecast natural/synthetic mix. It dominates the paste's GWP in the reviewed
scenario. Metallurgical silicon covers bulk supply while grade-specific
purification and micronisation remain unquantified. Commodity CMC and dry SBR
are upstream burden proxies for the experimental binders, not proof of
battery-grade formulation or redispersibility. Mass closure and the specified
silicon ratio do not validate N/P balance, formation losses, swelling, ageing
or commercial cell performance.

Cathode precursors
------------------

NCA precursor and oxide supply
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `NCA hydroxide (GLO) <https://clic.psi.ch/datasets/nca-hydroxide/>`__; `NCA hydroxide (GLO) <https://clic.psi.ch/datasets/nca-hydroxide-2/>`__; `NCA oxide <https://clic.psi.ch/datasets/nca-oxide/>`__.

The linked NCA hydroxide and oxide activities in the legacy and modern
battery inventories are unit wrappers. Each buys exactly one kg
from the corresponding native ``market for NCA hydroxide`` or
``market for NCA oxide`` in ``CN`` per kg of reference output. These purchases
replace the former custom synthesis recipes completely. They do not retain
additional foreground reagents, process energy, freight, losses or emissions.
Ordinary inventory migration selects the native supplier in the target
ecoinvent version; matching identities do not imply identical recipes or
burdens across versions.

The wrapper's ``GLO`` location does not represent a global production mix:
Chinese supply is the selected geographic proxy. Its chemical grade and
production boundary should be assessed for the application.

The native NCA wrappers purchase the complete chemical supply without
additional foreground synthesis reagents or water.

Lithium hydroxide hydration in custom NMC811 oxide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `NMC811 hydroxide <https://clic.psi.ch/datasets/nmc811-hydroxide/>`__; `NMC811 oxide <https://clic.psi.ch/datasets/nmc811-oxide/>`__.

The custom `NMC811 oxide <https://clic.psi.ch/datasets/nmc811-oxide/>`__ activities supporting the legacy and modern battery references buy
``lithium hydroxide, battery grade``.
That packaged product represents LiOH·H₂O, following
`Kelly et al. (2021) <https://doi.org/10.1016/j.resconrec.2021.105762>`__.
The previous 0.246 kg quantity was retained from an anhydrous-LiOH recipe.
An equal mass of monohydrate supplies less lithium. Both custom NMC811 blocks
use the corrected amount, including its water emission, so import order cannot
restore the old reagent amount.

The correction preserves the rounded anhydrous dose in the existing
inventory source. Using Li = 6.94, H = 1.008 and O = 15.999 g/mol, each kg of
anhydrous LiOH requires ``41.962 / 23.947 = 1.75228629891`` kg monohydrate:

.. list-table:: Quantities per kg custom oxide output
   :header-rows: 1

   * - Activity
     - LiOH·H₂O purchased, kg
     - Additional bound water released, kg
     - Total retained water emission, m³
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
``NMC811`` J191–J196. Dai et al. (2018),
`GREET cathode update <https://greet.anl.gov/publication-update_bom_cm>`__,
Table 5, independently reports 0.246 kg LiOH/kg NMC811. This correction does
not replace the rounded dose with a full-precision or independently optimized
recipe.

Only the hydration increment is balanced here. The inherited
``0.00019562 m³`` reaction-water baseline is retained and remains unvalidated.
This hydration correction does not establish full atomic closure or the
correctness of the complete LTO cell.
Ordinary anhydrous LiOH suppliers, native ecoinvent cathode inventories,
metal-characterization factors and primary/secondary shares are unchanged.
Rebuild scenario databases before deriving updated system results.

.. _battery-density-targets:

Configured density targets
--------------------------

These density targets and mass factors are generated from the packaged
configuration. Except for the explicit VRFB whole-system reference in
:doc:`battery-stationary`, they refer to cells. They are not whole-pack densities or absolute pack masses.
The aliases determine which capacity markets receive each trajectory.

.. include:: /reference/generated/battery-density.inc

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

For the complete-pack NMC955 route in 2050, using the configured cell-density range 0.34–0.40 kWh/kg
and the manufactured cell fraction 0.72591, manufacturing mass bounds are
3.443953107134493–4.051709537805285 kg/kWh. Equal-mass signed disposal
bounds mirror manufacture at −4.051709537805285 to −3.443953107134493
kg/kWh. The deterministic manufacture/disposal amounts remain
±3.625213796983676 kg/kWh.

The scaling preserves deterministic amounts, triangular modes, uncertainty
types and sign flags. Linear interpolation between 2020 and 2050 and endpoint
values outside that interval are retained. It concerns marginal parameter
bounds and does not define correlations between exchanges or validate the
underlying density or lifetime assumptions.

Sources and data dates
----------------------

.. include:: /reference/generated/source-batteries.inc
