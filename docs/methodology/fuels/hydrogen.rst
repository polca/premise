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

For a mapped IAM efficiency signal, the selected feedstock exchanges are
multiplied by the inverse efficiency change. Selection uses the configured
name substring and unit; other inputs are not automatically scaled.
Without an IAM signal, electrolysis uses the external electricity-requirement
trajectory. Other pathways keep their feedstock amount in this adjustment.
The energy-floor limitation below qualifies the IAM branch.

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
``hydrogen supply, distributed by pipeline`` service is added at 1 kg per kg
of market output. This does not mean all truck, ship and pipeline variants
listed in the inventory catalogue are deployed in every scenario.
Relinking connects the resulting suppliers to consuming activities.

Assumptions and limitations
-----------------------------

Compare hydrogen at the same pressure, state and delivery boundary.
External electrolysis assumptions are distinct from IAM projections. When
checking energy floors, inspect actual exchanges as well as logged totals;
the exchange behavior described below applies.

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
   * - hydrogen production, steam methane reforming
     - natural gas
     - m^3
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.5
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021 [`LCI_SMR <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx>`__]
   * - hydrogen production, steam methane reforming, with CCS
     - natural gas
     - m^3
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.5
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021 [`LCI_SMR <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx>`__]
   * - hydrogen production, steam methane reforming, from biomethane
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021 [`LCI_SMR <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx>`__]
   * - hydrogen production, steam methane reforming, from biomethane, with CCS
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021 [`LCI_SMR <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx>`__]
   * - hydrogen production, auto-thermal reforming, from biomethane
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021 [`LCI_ATR <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx>`__]
   * - hydrogen production, auto-thermal reforming, from biomethane, with CCS
     - biomethane
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 3.2
     - CH
     - `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al. 2021 [`LCI_ATR <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx>`__]
   * - hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass with CCS
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021 [`LCI_woody <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-wood-gasification.xlsx>`__]
   * - hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021 [`LCI_woody <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-wood-gasification.xlsx>`__]
   * - hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, with CCS
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021 [`LCI_woody <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-wood-gasification.xlsx>`__]
   * - hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier
     - wood chips
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 7.0
     - CH
     - `Antonini2 <https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C>`__ et al. 2021 [`LCI_woody <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-wood-gasification.xlsx>`__]
   * - hydrogen production, coal gasification
     - hard coal
     - kg
     - N/A
     - N/A
     - N/A
     - N/A
     - 5.0
     - RER
     - `Wokaun <https://www.cambridge.org/core/books/transition-to-hydrogen/43144AF26ED80E7106B675A6E83B1579>`__, `Li <https://doi.org/10.1016/j.jclepro.2022.132514>`__ [`LCI_coal <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-coal-gasification.xlsx>`__]
   * - hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity
     - electricity
     - kWh
     - 54.0
     - 52.9–55.1
     - 48.9
     - 45.3–52.5
     - 45.3
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021 [`LCI_electrolysis <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-electrolysis.xlsx>`__]
   * - hydrogen production, gaseous, 20 bar, from AEC electrolysis, from grid electricity
     - electricity
     - kWh
     - 51.8
     - 48.7–54.9
     - 48.5
     - 47.1–49.9
     - 47.1
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021 [`LCI_electrolysis <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-electrolysis.xlsx>`__]
   * - hydrogen production, gaseous, 1 bar, from SOEC electrolysis, from grid electricity
     - electricity
     - kWh
     - 42.3
     - 41.2–43.4
     - 40.6
     - 40.0–41.2
     - 40.0
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021 [`LCI_electrolysis <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-electrolysis.xlsx>`__]
   * - hydrogen production, gaseous, 1 bar, from SOEC electrolysis, with steam input, from grid electricity
     - electricity
     - kWh
     - 42.3*
     - 41.2–43.4
     - 40.6
     - 40.0–41.2
     - 40.0
     - RER
     - `Gerloff <https://doi.org/10.1016/j.est.2021.102759>`__ 2021 [`LCI_electrolysis <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-electrolysis.xlsx>`__]
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
consulted here: `LCI_H2_distr <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-distribution.xlsx>`__. For re-assigned CNG pipelines, which require the hydrogen
to be mixed together with oxygen to limit metal embrittlement,
some parameters are taken from the work of `Cerniauskas <https://doi.org/10.1016/j.ijhydene.2020.02.121>`__ et al. 2020.

The datasets introduced are listed in the table below.

 ================================================================== ===========
  Hydrogen distribution                                              location
 ================================================================== ===========
  hydrogen refuelling station                                        GLO
  high pressure hydrogen storage tank                                GLO
  pipeline, hydrogen, low pressure distribution network              RER
  compressor assembly for transmission hydrogen pipeline             RER
  pipeline, hydrogen, high pressure transmission network             RER
  zinc coating for hydrogen pipeline                                 RER
  hydrogenation of hydrogen                                          RER
  dehydrogenation of hydrogen                                        RER
  dibenzyltoluene production                                         RER
  solution mining for geological hydrogen storage                    RER
  geological hydrogen storage                                        RER
  hydrogen embrittlement inhibition                                  RER
  distribution pipeline for hydrogen, reassigned CNG pipeline        RER
  transmission pipeline for hydrogen, reassigned CNG pipeline        RER
 ================================================================== ===========


Hydrogen turbine
^^^^^^^^^^^^^^^^^^

A dataset for a hydrogen turbine is also imported, to model the production of electricity
from hydrogen, with an efficiency of 51%. The efficiency of the H2-fed gas turbine is based
on the parameters of `Ozawa <https://doi.org/10.1016/j.ijhydene.2019.02.230>`__ et al. (2019), accessible here: `LCI_H2_turbine <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-hydrogen-turbine.xlsx>`__.

Source provenance and currency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-hydrogen.inc

Detailed supply-chain names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/hydrogen-variants.inc
