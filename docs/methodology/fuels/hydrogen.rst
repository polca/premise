Hydrogen
==========

.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

Within the ``fuels`` update, Premise regionalizes hydrogen production and
builds regional gaseous low-pressure hydrogen markets per kilogram. Imported
supply-route variants describe conditioning and delivery options; their
availability is distinct from the route used by a generated market.

.. figure:: /_static/process-diagrams/fuels-hydrogen.svg
   :class: process-diagram
   :alt: Hydrogen production and delivery: Regionalize hydrogen production; Adjust selected feedstock inputs; Build regional hydrogen markets; Add pipeline delivery service and relink.
   :align: center

Inputs and applicability
--------------------------

The hydrogen entries of ``fuels.yaml`` map inventories and IAM production
and efficiency series. ``premise/data/fuels/hydrogen_efficiency_parameters.yml`` supplies
feedstock selectors and external electrolysis assumptions. The fuels branch
runs when at least one petrol, diesel, natural-gas or hydrogen blend array is
available. Use ``ndb.update("fuels")`` after upstream energy updates.

Transformation
----------------

Electrolyser component recipes and their target-version adaptations are
documented under :ref:`electrolyser-component-references` below. The scenario
operations here adjust efficiency, compression and regional supply chains.

Hydrogen efficiency
~~~~~~~~~~~~~~~~~~~~

For a mapped IAM efficiency signal, the selected feedstock exchanges are
multiplied by the inverse efficiency change. Selection uses the configured
name substring and unit; other inputs are not automatically scaled.
Without an IAM signal, electrolysis uses the external electricity-requirement
trajectory. Other pathways keep their feedstock amount in this adjustment.
The energy-floor limitation below qualifies the IAM branch.

Vehicle tank compression
~~~~~~~~~~~~~~~~~~~~~~~~

The generated low-pressure hydrogen market includes the imported
`hydrogen supply, distributed by pipeline <https://clic.psi.ch/datasets/hydrogen-supply-distributed-by-pipeline/>`__ service. Its source comment
states delivery at **30 bar**, after transmission, storage and distribution.
Its electricity includes those upstream operations, not final preparation for
the refreshed vehicle's 700-bar tank. The legacy ``add_compression_electricity``
helper for other delivery routes is not called by this market-generation path.

The refreshed car/truck adapters apply the carculator_utils intercooled
compression model from 30 to 700 bar, using its default 300 K, three equal-ratio
stages, 0.56 isentropic efficiency and 0.92 motor efficiency. This requires
**2.450619 kWh/kg H2**, replacing the generic 25-bar assumption's 2.616238.
These are the model's tank-compression assumptions; precooling and fast-fill
overpressure are outside this calculation.

The activity-level ``hydrogen tank compression`` metadata identify the fuel,
electricity product, pressures, model and specific electricity requirement.
The corresponding electricity exchanges carry a ``Hydrogen tank compression``
comment. Exchange comments are retained during geographic relinking and form
part of the aggregation identity, so compression and other electricity uses
remain separate and self-describing even when they use the same supplier.
During transport efficiency adjustment, that electricity product is excluded
from generic scaling and the commented compression exchange is calculated once
from final hydrogen consumption. Validation reads the exchange directly rather
than relying on a duplicate activity-level balance record.

Legacy inventories without this explicit role do not acquire an inferred
compression burden. The upstream pipeline recipe is unchanged. Both the
pipeline service and vehicle tank compression therefore follow hydrogen demand
at their respective stages.

Hydrogen supply chains
~~~~~~~~~~~~~~~~~~~~~~~~

The imported hydrogen supply-route catalogue contains variants that differ by:

* the transport mode: truck, hydrogen pipeline, re-assigned CNG pipeline, ship,
* the distance: 500 km, 2000 km
* the state of the hydrogen: gaseous, liquid, liquid organic compound,
* the hydrogen production route: electrolysis, SMR, biomass gasifier (coal, woody biomass)

The supply chain is built in stages::

    Production -> conditioning -> transport and storage -> delivered hydrogen
                      |                  |
                   energy use        losses and energy use

Compare routes at the same delivery state and pressure. Transport losses
increase upstream hydrogen production per kilogram delivered; compression and
liquefaction add their own energy requirements. A plant-gate production score
therefore cannot validate the delivered route by itself.

Boil-off loss values during shipping are from `Hank <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00067a>`__ et al, 2020.
Losses when transporting H2 via re-assigned CNG pipelines are from `Cerniauskas <https://doi.org/10.1016/j.ijhydene.2020.02.121>`__ et al, 2020.
Losses along the pipeline are from `Schori <https://treeze.ch/fileadmin/user_upload/downloads/PublicLCI/Schori_2012_NaturalGas.pdf>`__ et al, 2012., but to be considered conservative, as those
are initially for natural gas (and hydrogen has a higher potential for leaking).

 ========================== ================= ======== ======= ============== =============== ====================
  _                          _                 truck    ship    H2 pipeline    CNG pipeline    reference flow
 ========================== ================= ======== ======= ============== =============== ====================
  gaseous                    compression       0.5%             0.5%           0.5%            per kg H2
  _                          storage buffer                     2.3%           2.3%            per kg H2
  _                          storage leak                       1.0%           1.0%            per kg H2
  _                          pipeline leak                      0.004%         0.004%          per kg H2, per km
  _                          purification                                      7.0%            per kg H2
  liquid                     liquefaction      1.3%     1.3%                                   per kg H2
  _                          vaporization      2.0%     2.0%                                   per kg H2
  _                          boil-off          0.2%     0.2%                                   per kg H2, per day
  liquid organic compound    hydrogenation     0.5%                                            per kg H2
 ========================== ================= ======== ======= ============== =============== ====================

Losses accumulate along the supply chain and depend on route and distance.
Use a consistent production or delivery basis when combining loss fractions.
The table below shows the example of 1 kg of hydrogen transport via re-assigned CNG pipelines,
as a gas, over 500 km.
The supplier input is 1.133 kg per kg delivered: 0.133 kg is lost per kg
delivered, approximately 11.7% of the hydrogen produced:

 =============================================================================== ============== ================ ===========
  Output                                                                          _              _                _
 =============================================================================== ============== ================ ===========
  producer                                                                        amount         unit             location
  hydrogen supply, from electrolysis, by CNG pipeline, as gaseous, over 500 km    1              kilogram         OCE
  Input
  supplier                                                                        amount         unit             location
  hydrogen production, gaseous, 25 bar, from electrolysis                         1.133          kilogram         OCE
  market group for electricity, low voltage                                       3.091          kilowatt hour    OCE
  market group for electricity, low voltage                                       0.516          kilowatt hour    OCE
  hydrogen embrittlement inhibition                                               1              kilogram         OCE
  geological hydrogen storage                                                     1              kilogram         OCE
  Hydrogen refuelling station                                                     1.14E-07       unit             OCE
  distribution pipeline for hydrogen, reassigned CNG pipeline                     1.56E-08       kilometer        RER
  transmission pipeline for hydrogen, reassigned CNG pipeline                     1.56E-08       kilometer        RER
 =============================================================================== ============== ================ ===========

The two direct electricity exchanges in this route illustration sum to
3.607 kWh per kg delivered. Additional requirements must be checked in the
linked distribution and storage activities. The illustration is not an
independent validation of the active scenario's delivery route.

Markets and downstream links
------------------------------

Generated hydrogen markets use mapped production volumes and the common
system-model supplier weighting. A regionalized
`hydrogen supply, distributed by pipeline <https://clic.psi.ch/datasets/hydrogen-supply-distributed-by-pipeline/>`__ service is added at 1 kg per kg
of market output. This does not mean all truck, ship and pipeline variants
listed in the inventory catalogue are deployed in every scenario.
Relinking connects the resulting suppliers to consuming activities.

Recipes explicitly named ``market-average hydrogen`` adopt this scenario
supply. During hydrogen-market creation, ``_regionalize_hydrogen_activities`` maps
their native liquid, generic gaseous, or medium-pressure merchant hydrogen
market identities to the generated low-pressure market before the recipes
are copied into IAM regions. Geographic relinking
then resolves the regional provider. Hydrogen quantities, uncertainty and
existing process-energy inputs are retained; the source inventories are unchanged.
This is an explicit choice of the documented 30-bar pipeline-delivery boundary,
not physical equivalence between native delivery routes. Other hydrogen
consumers and technology-specific production purchases are unaffected.

Assumptions and limitations
-----------------------------

Compare hydrogen at the same pressure, state and delivery boundary.
External electrolysis assumptions are distinct from IAM projections. When
checking energy floors, inspect actual exchanges as well as logged totals;
the exchange behavior described below applies.

PEM balance-of-plant treatment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The imported `treatment of electrolyzer balance of plant, 1MWe, PEM <https://clic.psi.ch/datasets/treatment-of-electrolyzer-balance-of-plant-1mwe-pem/>`__ has a
negative one-unit reference output. Consequently, demanding minus one unit
represents treatment of one used system. Interpret the reference-output sign before describing a treatment burden or credit. Numerical LCIA outputs are retained locally.

The current boundary includes a matched power-conversion/drive treatment kit.
It replaces displaced bulk-material retirement rather than adding a second
whole converter, and follows the documented equipment and replacement counts.
Packaging is treated with manufacture, not again as installed WEEE. The
separate 32 kg industrial-electronics plastic route accounts for the nominal
100 kg control assembly's housing; its duplicate mixed-plastic entry is zero.
The 764.6 kg direct polymer retirement remains separate. Drive ratings,
electronic partitioning, lifetime and actual WEEE fate retain the explicitly
documented engineering assumptions; mass/sign consistency does not validate
a complete industrial disposal route. Operating hydrogen electricity demand
is specified separately from these equipment boundaries.

Implementation limitation: energy floors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the IAM branch, feedstock exchanges use the inverse efficiency factor
without enforcing the configured minimum on their amounts. The reported
energy value can therefore differ from the sum of those exchanges. Use the
actual exchange sum when assessing energy demand.

Worked example and checks
---------------------------

If the mapped efficiency improves by 10%, a selected 55 kWh/kg electricity
input becomes 55/1.1 = 50 kWh/kg before any separate delivery requirements.
For a delivery route requiring 1.133 kg produced per kg delivered, the excess
is 0.133 kg per kg delivered, or about 11.7% of production. Check the loss
basis, transport electricity and the route actually linked to the consumer.

Sources and inventory details
-------------------------------

Source inventories: hydrogen
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Hydrogen production
^^^^^^^^^^^^^^^^^^^^^

*premise* imports inventories for hydrogen production. The table below
gives an overview of the different pathways and their assumed specific energy use
in 2020 and 2050.

CLIC links identify the published reference location when it differs from
the historical location column. Plant-gate gasification records are labelled
separately from the delivered-hydrogen routes discussed below.

.. list-table:: Hydrogen production inventories
   :header-rows: 1

   * - Dataset
     - Feedstock
     - U
     - 2020 avg
     - 2020 rng
     - 2050 avg
     - 2050 rng
     - Floor
     - Loc
     - Literature reference
     - CLIC dataset page
   * - hydrogen production, steam methane reforming
     - natural gas
     - m^3
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.5
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021
     - `Published reference (RER) <https://clic.psi.ch/datasets/hydrogen-production-steam-methane-reforming/>`__
   * - hydrogen production, steam methane reforming, with CCS
     - natural gas
     - m^3
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.5
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021
     - `Published reference (RER) <https://clic.psi.ch/datasets/hydrogen-production-steam-methane-reforming-with-ccs/>`__
   * - hydrogen production, steam methane reforming, from biomethane
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021
     - `Published reference (RER) <https://clic.psi.ch/datasets/hydrogen-production-steam-methane-reforming-from-biomethane/>`__
   * - hydrogen production, steam methane reforming, from biomethane, with CCS
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021
     - `Published reference (RER) <https://clic.psi.ch/datasets/hydrogen-production-steam-methane-reforming-from-biomethane-with-ccs/>`__
   * - hydrogen production, auto-thermal reforming, from biomethane
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021
     - `Published reference (RER) <https://clic.psi.ch/datasets/hydrogen-production-auto-thermal-reforming-from-biomethane/>`__
   * - hydrogen production, auto-thermal reforming, from biomethane, with CCS
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021
     - `Published reference (RER) <https://clic.psi.ch/datasets/hydrogen-production-auto-thermal-reforming-from-biomethane-with-ccs/>`__
   * - hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass with CCS
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021
     - `Plant-gate source (RER) <https://clic.psi.ch/datasets/hydrogen-production-gaseous-25-bar-from-heatpipe-reformer-gasification-of-woody-biomass-with-ccs-at-gasification-plant/>`__
   * - hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021
     - `Plant-gate source (RER) <https://clic.psi.ch/datasets/hydrogen-production-gaseous-25-bar-from-heatpipe-reformer-gasification-of-woody-biomass-at-gasification-plant/>`__
   * - hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, with CCS
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021
     - `Plant-gate source (RER) <https://clic.psi.ch/datasets/hydrogen-production-gaseous-25-bar-from-gasification-of-woody-biomass-in-entrained-flow-gasifier-with-ccs-at-gasification-plant/>`__
   * - hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021
     - `Plant-gate source (RER) <https://clic.psi.ch/datasets/hydrogen-production-gaseous-25-bar-from-gasification-of-woody-biomass-in-entrained-flow-gasifier-at-gasification-plant/>`__
   * - hydrogen production, coal gasification
     - hard coal
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 5.0
     - RER
     - `Wokaun <https://www.cambridge.org/core/books/transition-to-hydrogen/43144AF26ED80E7106B675A6E83B1579>`__, `Li <https://doi.org/10.1016/j.jclepro.2022.132514>`__
     - `Published reference (RoW) <https://clic.psi.ch/datasets/hydrogen-production-coal-gasification/>`__
   * - hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity
     - electricity
     - kWh
     - 54.0
     - 52.9–55.1
     - 48.9
     - 45.3–52.5
     - 45.3
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021
     - `View dataset <https://clic.psi.ch/datasets/hydrogen-production-gaseous-30-bar-from-pem-electrolysis-from-grid-electricity/>`__
   * - hydrogen production, gaseous, 20 bar, from AEC electrolysis, from grid electricity
     - electricity
     - kWh
     - 51.8
     - 48.7–54.9
     - 48.5
     - 47.1–49.9
     - 47.1
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021
     - `Published reference (CH) <https://clic.psi.ch/datasets/hydrogen-production-gaseous-20-bar-from-aec-electrolysis-from-grid-electricity/>`__
   * - hydrogen production, gaseous, 1 bar, from SOEC electrolysis, from grid electricity
     - electricity
     - kWh
     - 42.3
     - 41.2–43.4
     - 40.6
     - 40.0–41.2
     - 40.0
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021
     - `Published reference (CH) <https://clic.psi.ch/datasets/hydrogen-production-gaseous-1-bar-from-soec-electrolysis-from-grid-electricity/>`__
   * - hydrogen production, gaseous, 1 bar, from SOEC electrolysis, with steam input, from grid electricity
     - electricity
     - kWh
     - 42.3*
     - 41.2–43.4
     - 40.6
     - 40.0–41.2
     - 40.0
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021
     - `Published reference (CH) <https://clic.psi.ch/datasets/hydrogen-production-gaseous-1-bar-from-soec-electrolysis-with-steam-input-from-grid-electricity/>`__
   * - hydrogen production, gaseous, 25 bar, from thermochemical water splitting, at solar tower
     - solar
     - MJ
     - N/A
     - N/A
     - N/A
     - N/A
     - 180
     - RER
     - `Zhang2 <https://doi.org/10.1016/j.ijhydene.2022.02.150>`__ 2022
     - `View dataset <https://clic.psi.ch/datasets/hydrogen-production-gaseous-25-bar-from-thermochemical-water-splitting-at-solar-tower/>`__
   * - hydrogen production, gaseous, 100 bar, from methane pyrolysis
     - natural gas
     - m^3
     - N/A
     - N/A
     - N/A
     - N/A
     - 6.5
     - RER
     - `Al-Qahtani <https://doi.org/10.1016/j.apenergy.2020.115958>`__, `Postels <https://doi.org/10.1016/j.ijhydene.2016.09.167>`__
     - `View dataset <https://clic.psi.ch/datasets/hydrogen-production-gaseous-100-bar-from-methane-pyrolysis/>`__

Future efficiencies for electrolyzers are based on Studie `IndWEDe <https://www.now-gmbh.de/wp-content/uploads/2020/09/indwede-studie_v04.1.pdf>`__ (see p.176).
The SOEC inventory with a steam input uses the same performance assumptions as
the standard SOEC inventory because no separate performance data are available.

Hydrogen storage and distribution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A number of datasets relating to hydrogen storage and distribution are also imported.

They are necessary to model the distribution of hydrogen:

* via re-assigned transmission and distribution CNG pipelines, in a gaseous state
* via dedicated transmission and distribution hydrogen pipelines, in a gaseous state
* as a liquid organic compound, by hydrogenation
* via truck, in a liquid state
* hydrogen refuelling station

Small and large storage solutions are also provided:
* high pressure hydrogen storage tank
* geological storage tank

These datasets originate from the work of `Wulf <https://www.sciencedirect.com/science/article/pii/S095965261832170X>`__ et al. 2018, and can be
consulted here: `hydrogen refuelling station (GLO) <https://clic.psi.ch/datasets/hydrogen-refuelling-station/>`__; `high pressure hydrogen storage tank (GLO) <https://clic.psi.ch/datasets/high-pressure-hydrogen-storage-tank/>`__; `pipeline, hydrogen, low pressure distribution network (RER) <https://clic.psi.ch/datasets/pipeline-hydrogen-low-pressure-distribution-network/>`__. For re-assigned CNG pipelines, which require the hydrogen
to be mixed together with oxygen to limit metal embrittlement,
some parameters are taken from the work of `Cerniauskas <https://doi.org/10.1016/j.ijhydene.2020.02.121>`__ et al. 2020.

The datasets introduced are listed in the table below.

.. list-table::
   :header-rows: 1

   * - Hydrogen distribution
     - location
     - CLIC dataset page
   * - hydrogen refuelling station
     - GLO
     - `View dataset <https://clic.psi.ch/datasets/hydrogen-refuelling-station/>`__
   * - high pressure hydrogen storage tank
     - GLO
     - `View dataset <https://clic.psi.ch/datasets/high-pressure-hydrogen-storage-tank/>`__
   * - pipeline, hydrogen, low pressure distribution network
     - RER
     - `View dataset <https://clic.psi.ch/datasets/pipeline-hydrogen-low-pressure-distribution-network/>`__
   * - compressor assembly for transmission hydrogen pipeline
     - RER
     - `View dataset <https://clic.psi.ch/datasets/compressor-assembly-for-transmission-hydrogen-pipeline/>`__
   * - pipeline, hydrogen, high pressure transmission network
     - RER
     - `View dataset <https://clic.psi.ch/datasets/pipeline-hydrogen-high-pressure-transmission-network/>`__
   * - zinc coating for hydrogen pipeline
     - RER
     - `View dataset <https://clic.psi.ch/datasets/zinc-coating-for-hydrogen-pipeline/>`__
   * - hydrogenation of hydrogen
     - RER
     - `View dataset <https://clic.psi.ch/datasets/hydrogenation-of-hydrogen/>`__
   * - dehydrogenation of hydrogen
     - RER
     - `View dataset <https://clic.psi.ch/datasets/dehydrogenation-of-hydrogen/>`__
   * - dibenzyltoluene production
     - RER
     - `View dataset <https://clic.psi.ch/datasets/dibenzyltoluene-production/>`__
   * - solution mining for geological hydrogen storage
     - RER
     - `View dataset <https://clic.psi.ch/datasets/solution-mining-for-geological-hydrogen-storage/>`__
   * - geological hydrogen storage
     - RER
     - `View dataset <https://clic.psi.ch/datasets/geological-hydrogen-storage/>`__
   * - hydrogen embrittlement inhibition
     - RER
     - `View dataset <https://clic.psi.ch/datasets/hydrogen-embrittlement-inhibition/>`__
   * - distribution pipeline for hydrogen, reassigned CNG pipeline
     - RER
     - —
   * - transmission pipeline for hydrogen, reassigned CNG pipeline
     - RER
     - —

.. _hydrogen-pipeline-coatings:

Pipeline coatings
^^^^^^^^^^^^^^^^^

For ecoinvent **3.12 attributional targets (cutoff and EN15804)**, the default hydrogen-distribution import
uses two separate, complete hot-dip aluminium-zinc-silicon coating references
on CLIC: `distribution pipe <https://clic.psi.ch/datasets/aluminium-zinc-silicon-coating-production-hot-dip-on-distribution-pipe/>`__ and
`transmission pipe <https://clic.psi.ch/datasets/aluminium-zinc-silicon-coating-production-hot-dip-on-transmission-pipe/>`__. These reproduce the approved
CLIC distribution and transmission coating recipes (3652 and 3774), each per
kilogram of retained coating, including the original material losses,
utilities, direct emissions and qualified waste burdens.

The legacy hydrogen-distribution inventory source remains authored against **3.7**.
Its distribution and transmission construction activities still purchase
**75.25328256144047 kg** and **876.7114153394484 kg**, respectively. The import
profile changes only the supplier identity of these two purchases, preserving
their quantities, uncertainty parameters and all other parent exchanges. It
omits the now-unused legacy alloy producer from this modern import view;
the original source inventory is unchanged. The companion is prepared first
against its own 3.12 technosphere and biosphere references.

Older targets and consequential builds retain the qualified legacy recipe.
The modern coating is not reverse-compiled into a 3.7 recipe: reverse
aggregation loses ingredients and cannot establish equivalent burdens.
No global supplier alias, parent background-version change or native
background modification is introduced.

These remain **qualified engineering models**, not measured pipe-coating
factory inventories or verified hydrogen-barrier performance. Their original
assumptions include the 55/43.4/1.6 Al/Zn/Si composition, 95% metal retention,
geometry-dependent historical sheet-aluminising utility analogues, cleaning
make-up, protective gas and oxidation scenarios. External nitrogen,
wastewater and contained-residue supplies remain imperfect burden proxies.
The original comments describe gaps in chemistry, emission measurements,
classification, service life and uncertainty. No extra foreground coating
feed, standalone annealing, bath-energy allowance, retirement or recovery
credit is added by this selection. Pipeline geometry, freight, installation
and existing retirement assumptions are unchanged.

The original foreground compilation is CC BY 4.0. The companion retains
attribution, source qualifications, CLIC version identifiers and
snapshot digests. Referenced native inventories remain external and retain
their own terms. The default importer does not distribute their LCI bodies.

Hydrogen turbine
^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `electricity production, from hydrogen-fired one gigawatt gas turbine, for grid balancing <https://clic.psi.ch/datasets/electricity-production-from-hydrogen-fired-one-gigawatt-gas-turbine-for-grid-balancing/>`__.

A dataset for a hydrogen turbine is also imported, to model the production of electricity
from hydrogen, with an efficiency of 51%. The efficiency of the H2-fed gas turbine is based
on the parameters of `Ozawa <https://doi.org/10.1016/j.ijhydene.2019.02.230>`__ et al. (2019), accessible here: `electricity production, from hydrogen-fired one gigawatt gas turbine, for grid balancing (RER) <https://clic.psi.ch/datasets/electricity-production-from-hydrogen-fired-one-gigawatt-gas-turbine-for-grid-balancing/>`__.

.. raw:: html

   <span id="evangelisti-et-al-2017"></span>
   <span id="supplementary-inventory-tables"></span>

.. _electrolyser-component-references:

Electrolyser components
~~~~~~~~~~~~~~~~~~~~~~~

PTFE gaskets in alkaline electrolysers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `polytetrafluoroethylene gasket production, forming, resin supply proxy <https://clic.psi.ch/datasets/polytetrafluoroethylene-gasket-production-forming-resin-supply-proxy/>`__.

For ecoinvent 3.12, the additional AEC stack uses the same qualified PTFE
gasket reference as CLIC. Per kilogram of finished gasket, this reference
purchases **1/0.95 kg of native PTFE resin**, **3 MJ heat** and **0.2 kWh
electricity**. The 95% forming/cutting retention and utility quantities are
engineering assumptions, not factory measurements. The native emulsion-route
resin is a production-burden proxy, not a verified granular moulding grade.
The foreground represents forming, sintering and cutting; resin polymerisation
remains in the external supplier, so the legacy monomer purchase and local
polymerisation allowances are not added again.

The AEC stack still purchases **144.2 kg** of finished gaskets. Manufacturing
rejects use the existing qualified generic waste-plastic outlet once, and
finished-gasket retirement remains in the original stack treatment. This
does not certify fluoropolymer disposal or PFAS abatement.

The `native-resin gasket provider <https://clic.psi.ch/datasets/polytetrafluoroethylene-gasket-production-forming-resin-supply-proxy/>`__ is imported separately,
with source technosphere and biosphere
versions both 3.12. Before preparing the additional electrolysis inventory source for
that target, Premise removes its legacy gasket producer from the extracted
source and relinks only the AEC purchase. The electrolysis inventory source
keeps its original 3.9 source declaration. Targets 3.8–3.11 continue to use
the original monomer-based reference; no native-resin backport or broad
background-version change is implied. The source inventories, native datasets
and published CLIC versions are not rewritten by this import selection.

The foreground compilation is CC BY 4.0 with compiler attribution and source
qualifications recorded in the source inventory.  Referenced
publications and native supplier inventories retain their separate terms.

Shared PFSA membrane reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `perfluorosulfonic acid membrane production, ew1100, engineering reference <https://clic.psi.ch/datasets/perfluorosulfonic-acid-membrane-production-ew1100-engineering-reference/>`__.

The additional PEM electrolyser and flow-battery inventories share
`perfluorosulfonic acid membrane production, ew1100, engineering reference <https://clic.psi.ch/datasets/perfluorosulfonic-acid-membrane-production-ew1100-engineering-reference/>`__.
This shared CLIC reference is imported before its buyers with
source technosphere and biosphere versions both 3.9. Electrolysis and VRFB
applications purchase the same membrane reference. Normal import migration
targets the selected output version.

The PEM electrolyser stack still purchases **16 kg** membrane. The VRFB
`nafion membrane production <https://clic.psi.ch/datasets/nafion-membrane-production/>`__ wrapper still purchases **1 kg** of finished
membrane per kg wrapper output and retains its factory and freight inputs
once. The VRFB and hybrid-flow stacks still purchase **150 kg** and
**0.132 kg** of that wrapper, respectively. The separate fuel-cell membrane
recipe is not part of this consolidation.

This is a shared dry, unreinforced acid-form EW1100 engineering reference,
not proof of commercial-grade equivalence across applications. Existing
conversion, retention, process-water, energy and effluent assumptions remain
qualified; unmodelled purification, reinforcement, hydration, PFAS releases
and fluoride-specific abatement are not zero burdens. The shared reference
retains its CC BY-NC 4.0 foreground terms, compiler attribution and
source citations. Referenced publications and background inventories retain
their separate terms.

AEC diaphragm manufacturing rejects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `diaphragm production, alkaline electrolysis, zirconia-polysulfone with polyphenylene sulfide mesh <https://clic.psi.ch/datasets/diaphragm-production-alkaline-electrolysis-zirconia-polysulfone-with-polyphenylene-sulfide-mesh/>`__.

The zirconia–polysulfone diaphragm keeps its original formulation, utilities,
5% coating-reject assumption and **121.8 kg** stack purchase. The nominal
0.040912611614569605 kg coating reject per kg finished diaphragm is routed by
its assumed composition: **0.03477571987238416 kg zirconia** to the existing
inert-disposal proxy and **0.00613689174218544 kg polysulfone** to the
mixed-plastic market in DE. Both purchases follow the native negative waste
reference convention. Their exact supplier identities exist in the original
ecoinvent 3.9 catalogue, so source technosphere and biosphere versions stay 3.9.

This aligns the waste split with CLIC while preserving total reject mass,
the other eight exchanges and finished-diaphragm retirement. The routing is
a composition-based engineering treatment proxy; actual separation of mixed
coating rejects, disposal and hazardous classification are unverified.

SOEC glass-seal manufacturing rejects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `glass seal production, SOEC, barium alumino-borosilicate <https://clic.psi.ch/datasets/glass-seal-production-soec-barium-alumino-borosilicate/>`__.

The barium alumino-borosilicate seal reference uses the RoW hazardous-waste
underground-deposit market for its manufacturing rejects, aligned with CLIC.
The exact supplier exists in the original ecoinvent 3.9 catalogue; source
technosphere and biosphere declarations remain 3.9. The 95% retention and
**0.05263157894736836 kg reject per kg finished glass** are unchanged. The
SOEC stack inventory still purchases **25.6 kg** finished glass, corresponding
to **1.3473684210526302 kg** manufacturing reject. All four precursor amounts,
153.6 MJ heat and 25.6 kWh electricity remain unchanged on that stack basis.

Containment is a qualified treatment proxy. Actual disposal, hazardous
classification, leaching and impacts are unverified; inert disposal remains
a documented alternative at the same mass. Finished-seal retirement and
unquantified volatile losses are separate.

Lanthanum-reject fate in SOEC interconnect supply
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `lanthanum metal production, oxide electrolysis <https://clic.psi.ch/datasets/lanthanum-metal-production-oxide-electrolysis/>`__; `ferritic steel production, electrolysis interconnect, crofer 22 apu type <https://clic.psi.ch/datasets/ferritic-steel-production-electrolysis-interconnect-crofer-22-apu-type/>`__.

The oxide-electrolysis lanthanum helper retains its 95% yield, precursor
budget, graphite, reaction CO2 and **10 kWh electricity plus 3 MJ heat per kg
La**. Its nominal **0.05263157894736836 kg La-equivalent loss per kg output**
has no supported treatment destination. The loss and its treatment impacts
remain explicitly unassigned; zero recorded treatment does not establish
zero physical loss or zero environmental burden.

The Crofer-type alloy still purchases **0.001 kg La per kg alloy**. The
SOEC stack reference retains **8,976.1 kg alloy** and the same native
sheet-working purchase, including its existing stock makeup and uncertainty.
The unassigned nominal La-equivalent loss is **0.4724263157894732 kg** on
that reference. Both source declarations stay 3.9.

SiC wafer full-crystal material accounting
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**CLIC dataset pages:** `silicon carbide wafer production, physical vapour transport <https://clic.psi.ch/datasets/silicon-carbide-wafer-production-physical-vapour-transport/>`__.

The physical-vapour-transport wafer helper counts the historical reference's
full **100 mm crystal**, including its unusable regions. Forty-seven wafers
at **0.35 mm** thickness retain 16.45 mm of crystal, giving a mass retention
of **0.1645**. Per kg finished physical wafer, the reference therefore buys
**6.079027355623101 kg SiC** and sends **5.079027355623101 kg rejects** to
the existing qualified mineral-waste proxy once. Material outside the usable
section is included in those rejects. No recovery credit is
added; actual waste classification and destination remain unverified.

Substrate electricity stays **19,932.858629976276 kWh/kg physical wafer**.
This electricity reference is specified independently of the material yield.
This historical geometry is a reference scenario, not measured modern-factory
yield. Purification, charge losses, process gases, direct emissions and
industrial qualification remain unresolved. Technosphere and biosphere
source declarations both stay 3.9.

Transformer-core supply for ecoinvent 3.12
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The dedicated 1.1 MVA electrolysis transformer retains **947.32 kg core
steel**. When building with ecoinvent 3.12, it purchases the native RER
market for grain-oriented electrical steel, **0.23 mm finished coil**.
This replaces the six core inputs together: unalloyed steel,
metallurgical silicon, hot rolling, sheet rolling, annealing electricity
and furnace heat. All other transformer inputs and retirement quantities
remain unchanged. Native nominal silicon is **3.2%**, replacing the legacy
3% engineering-alloy assumption. RER procurement and grade/gauge suitability
remain qualified proxies; core cutting, stacking, insulation and
electrical-design coverage remain unverified.

The unchanged electrolysis inventory source retains its original technosphere and
biosphere source declarations at **3.9**. The default importer selects the
native core after normal migration and before linking, only for the
**3.12 target family**. Other targets retain the legacy recipe. An absent or
ambiguous native supplier, or a changed scientific recipe, fails validation
before replacement. Native background activities are never modified.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-hydrogen.inc

Detailed supply-chain names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These are Premise-generated delivery-route names. The production and
storage/distribution tables above link to their published CLIC source modules;
the IAM-region-specific combinations are generated during the update.

.. include:: /reference/generated/hydrogen-variants.inc

Vehicle hydrogen tank inventories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `fuel tank, compressed hydrogen gas, 700bar, with HDPE liner <https://clic.psi.ch/datasets/fuel-tank-compressed-hydrogen-gas-700bar-with-hdpe-liner/>`__; `fuel tank, compressed hydrogen gas, 700bar, with aluminium liner <https://clic.psi.ch/datasets/fuel-tank-compressed-hydrogen-gas-700bar-with-aluminium-liner/>`__.

The `HDPE-lined 700-bar tank activity <https://clic.psi.ch/datasets/fuel-tank-compressed-hydrogen-gas-700bar-with-hdpe-liner/>`__ produces one kg of
dry tank assembly, including tank balance of plant and excluding stored
hydrogen. Its quantities come from `Evangelisti et al. (2017)
<https://doi.org/10.1016/j.jclepro.2016.11.159>`_ and its
`supplementary inventory tables
<https://ars.els-cdn.com/content/image/1-s2.0-S0959652616320212-mmc1.docx>`_.

The paper's main text, scenario labels and supplementary output masses are
inconsistent. The retained recipe is the table containing HDPE. Its corrected
denominator is the sum of the listed materials, **87.71 kg**, rather than the
legacy 119 kg denominator. Material proportions therefore sum to one kg per kg
of dry assembly. The difference from the nominal output is approximately stored
hydrogen, but the paper does not state that boundary unambiguously. Dry-mass
normalization is an explicit modelling choice; no unidentified missing material
is added.

Manufacturing electricity is published as 12.87 **MJ per assembly**. Dividing
by 3.6 MJ/kWh and 87.71 kg gives 0.0407593 kWh/kg. Steel working is a processing service and does not add
material mass.

Existing supplier proxies include AlLi for aluminium. These source references
do not establish current vehicle composition, sizing or manufacturing yields.
