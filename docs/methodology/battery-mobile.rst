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

Chemistry coverage
~~~~~~~~~~~~~~~~~~

Before changing battery masses or scenario shares, Premise checks that every
chemistry with a positive share has a matching capacity input in the market's
unit. For example, a kWh market needs a kWh input; an input in kg is not a
substitute. Missing mappings or inputs raise ``ValueError`` with the market,
scenario, year and missing shares.

When all chemistries are represented, shares are normalized to handle source
rounding. Interpolation and endpoint handling still apply. Supplier linking
is checked separately: having a named input does not prove that its supplier
exists.

The three solid-state suppliers retain distinct chemistry and capacity
references after inventory migration. The NMC900/NMC900-Si aggregation is
a separate proxy requiring assessment. Germanium is present in the sulfide LCI;
metal-indicator methods that omit it do not report that demand.

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

The operation scales the kilogram exchanges present in the capacity market;
it does not rebuild a cell-to-pack engineering model. In references that
separate cells from pack hardware, only the cell provision follows this
factor. Non-cell hardware and controls are supplied through fixed kWh- or
unit-valued helpers, including their associated treatment. The composition
within each supplied cell reference is retained. Legacy capacity recipes
that still buy a complete pack in kg retain their original scaling boundary.
The configured density values supply relative growth factors and must not be
interpreted as whole-pack densities or used alone to infer absolute kg/kWh.

The shared :doc:`battery-inventories` page gives the configured density
trajectories, uncertainty rule and profile-specific manufacturing assumptions.
The :ref:`NMC955 separated-cell reference <nmc955-separated-cell-reference>`
explains its fixed hardware and cell-capacity basis.

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

Constant-range vehicle capacity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Marked passenger cars, trucks and electric buses resize their battery capacity
in proportion to operating grid electricity per vehicle-kilometre relative to
the 2020 design. This holds range fixed, with no additional mass/energy feedback
on the IAM efficiency. Battery purchases remain nominal kWh times one plus
lifetime replacements. Energy-density improvements are applied in the capacity
markets, never as a second reduction of these kWh purchases.

Cars retain occupancy; BEV trucks retain payload utilization and adjust their
tonne-kilometre normalization. The ten electric bus variants described under :doc:`transport/buses`
retain passenger occupancy and the existing replacement
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

Manufacturing recipes shared with stationary applications are documented in
:doc:`battery-inventories`; electronics, material inputs and cooling are in
:doc:`battery-components`. Application-specific solid-state references follow
below.

Source inventories: li-ion batteries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Battery imports depend on the source ecoinvent version. The NMC111/811/NCA/LFP
supplement is skipped for 3.9 and later because those inventories are already
represented in the background. The NMC622/NMC532 supplement is skipped for
3.11 and 3.12. Other battery supplements are selected separately by the
constructor's import registry, including the
:ref:`curated LFP manufacturing reference <lfp-curated-reference>` from 3.9.
NMC-111, NMC-622 NMC-811 and NCA Lithium-ion battery inventories are originally
from `Dai <https://www.mdpi.com/2313-0105/5/2/48>`__ et al. 2019. They have been adapted to ecoinvent by `Crenna <https://doi.org/10.1016/j.resconrec.2021.105619>`__ et al, 2021.
LFP and LTO Lithium-ion battery inventories are from  `Schmidt <https://doi.org/10.1021/acs.est.8b05313>`__ et al. 2019.
Li-S (Lithium-sulfur) battery inventories are from `Wickerts <https://doi.org/10.1021/acssuschemeng.3c00141>`__ et al. 2023.
Li-O2 (Lithium-air) battery inventories are from `Wang <https://doi.org/10.1016/j.jclepro.2020.121339>`__ et al. 2020.
Finally, SIB (Sodium-ion) battery inventories are from `Zhang22 <https://doi.org/10.1016/j.resconrec.2023.107362>`__ et al. 2024.
Ecoinvent provides also inventories for LMO (Lithium Maganese Oxide) batteries.

They introduce the following datasets:

.. list-table::
   :header-rows: 1

   * - Battery components
     - location
     - source
     - CLIC dataset page
   * - battery management system production, for Li-ion battery
     - GLO
     - Schmidt et al. 2019
     - `View dataset <https://clic.psi.ch/datasets/battery-management-system-production-for-li-ion-battery/>`__
   * - market for battery, Li-ion, NMC111, rechargeable, prismatic
     - GLO
     - Dai et al. 2019, Crenna et al. 2021
     - —
   * - market for battery, Li-ion, NMC622, rechargeable, prismatic
     - GLO
     - Dai et al. 2019, Crenna et al. 2021
     - —
   * - market for battery, Li-ion, NMC811, rechargeable, prismatic
     - GLO
     - Dai et al. 2019, Crenna et al. 2021
     - —
   * - market for battery, Li-ion, NCA, rechargeable, prismatic
     - GLO
     - Dai et al. 2019, Crenna et al. 2021
     - —
   * - market for battery, Li-ion, LFP, curated reference
     - GLO
     - Schmidt et al. 2019; curated layout
     - `View dataset <https://clic.psi.ch/datasets/market-for-battery-li-ion-lfp-rechargeable-prismatic/>`__
   * - market for battery cell, Li-ion, LTO
     - GLO
     - Schmidt et al. 2019
     - `View dataset <https://clic.psi.ch/datasets/market-for-battery-cell-li-ion-lto/>`__
   * - market for battery, Li-sulfur, Li-S
     - GLO
     - Wickerts et al. (2023)
     - `View dataset <https://clic.psi.ch/datasets/market-for-battery-li-sulfur-li-s/>`__
   * - market for battery, Li-oxygen, Li-O2
     - GLO
     - Wang et al. (2020)
     - `View dataset <https://clic.psi.ch/datasets/market-for-battery-li-oxygen-li-o2/>`__
   * - market for battery, Sodium-ion, SiB
     - GLO
     - Zhang et al. (2024)
     - `View dataset <https://clic.psi.ch/datasets/market-for-battery-sodium-ion-sib/>`__
   * - market for battery, NaCl, rechargeable, prismatic
     - GLO
     - Galloway & Dustmann (2003)
     - —

These battery inventories are mostly used by battery electric vehicles,
stationary energy storage systems, etc. (also imported by *premise*).

NMC-111, NMC-811, LFP and NCA inventories can be found here: `market for battery cell, Li-ion, NMC111 (GLO) <https://clic.psi.ch/datasets/market-for-battery-cell-li-ion-nmc111/>`__; `market for battery cell, Li-ion, NMC811 (GLO) <https://clic.psi.ch/datasets/market-for-battery-cell-li-ion-nmc811/>`__; `market for battery cell, Li-ion, NCA (GLO) <https://clic.psi.ch/datasets/market-for-battery-cell-li-ion-nca/>`__; `market for battery cell, Li-ion, LFP, curated supply (GLO) <https://clic.psi.ch/datasets/market-for-battery-cell-li-ion-lfp/>`__.
CLIC also provides `NMC622 <https://clic.psi.ch/datasets/market-for-battery-li-ion-nmc622/>`__, `NMC532 <https://clic.psi.ch/datasets/market-for-battery-li-ion-nmc532/>`__,
`NMC955 <https://clic.psi.ch/datasets/market-for-battery-li-ion-nmc955/>`__ and `LTO <https://clic.psi.ch/datasets/market-for-battery-li-ion-lto/>`__ battery records.
Li-S inventories can be found here: `market for battery, Li-sulfur, Li-S (GLO) <https://clic.psi.ch/datasets/market-for-battery-li-sulfur-li-s/>`__; `battery cell production, Li-S (GLO) <https://clic.psi.ch/datasets/battery-cell-production-li-s/>`__.
Li-O2 inventories can be found here: `market for battery, Li-oxygen, Li-O2 (GLO) <https://clic.psi.ch/datasets/market-for-battery-li-oxygen-li-o2/>`__; `battery cell production, Li-O2 (GLO) <https://clic.psi.ch/datasets/battery-cell-production-li-o2/>`__.
And SIB inventories can be found here: `market for battery, sodium-ion, SiB (GLO) <https://clic.psi.ch/datasets/market-for-battery-sodium-ion-sib/>`__; `battery module production, for SiB battery (GLO) <https://clic.psi.ch/datasets/battery-module-production-for-sib-battery/>`__.

From ecoinvent 3.9 onward, the legacy NMC111/811/NCA/LFP inventory source is skipped;
the curated LFP reference is imported separately with authored source version
3.12 through ordinary inventory migration. Native battery inventories retain
their original identities.

Solid-state chemistry mapping and reference inventories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `battery cell production, all-solid-state battery, sulfidic <https://clic.psi.ch/datasets/battery-cell-production-all-solid-state-battery-sulfidic/>`__; `battery cell production, all-solid-state battery, oxidic <https://clic.psi.ch/datasets/battery-cell-production-all-solid-state-battery-oxidic/>`__; `battery cell production, all-solid-state battery, polymer <https://clic.psi.ch/datasets/battery-cell-production-all-solid-state-battery-polymer/>`__.

The mobile source forecast distinguishes ``ASSB (polymer)``, ``ASSB (oxidic)``
and ``ASSB (sulfidic)`` from sodium-ion ``SIB``. In MIX in 2040, each has a
7.8% share, giving 7.8% sodium-ion and 23.4% solid-state batteries. The forecast
ends in 2040 and its endpoint is held thereafter. In PLiB, solid-state shares
total 36% at that endpoint. The SIB inventory represents a sodium-ion NMMT
design and is separate from these lithium solid-state batteries.

The three source identities are now retained and each has its own capacity
market: `polymer <https://clic.psi.ch/datasets/market-for-battery-capacity-all-solid-state-battery-polymer/>`__, `oxidic <https://clic.psi.ch/datasets/market-for-battery-capacity-all-solid-state-battery-oxidic/>`__ and `sulfidic <https://clic.psi.ch/datasets/market-for-battery-capacity-all-solid-state-battery-sulfidic/>`__. The published reference designs below
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

Inventory construction
^^^^^^^^^^^^^^^^^^^^^^^^^

The polymer and oxide cell materials and manufacturing energy follow the
baseline reference designs of
`Zhang et al. (2025) <https://doi.org/10.1016/j.jclepro.2025.146607>`__.
The accompanying `dimensioning model <https://github.com/Shan253/All_solid_state_battery_dimensioning_model>`__ supplies the cell geometry.
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

.. raw:: html

   <span id="argonne-anl-cse-24-1-2024"></span>
   <span id="wolf-et-al-2023"></span>
   <span id="configured-density-targets"></span>
   <span id="id9"></span>
   <span id="lithium-hydroxide-hydration-in-custom-nmc811-oxide"></span>
   <span id="nca-precursor-and-oxide-supply"></span>
   <span id="nmc955-dry-anode-and-collector-boundary"></span>
   <span id="nmc955-factory-energy-reference"></span>
   <span id="nmc955-manufacturing-yield-and-rejects"></span>
   <span id="nmc955-reference-capacity-and-fixed-hardware"></span>

See :doc:`battery inventories </methodology/battery-inventories>` for these inventory details.
