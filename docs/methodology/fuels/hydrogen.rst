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

PTFE gaskets in alkaline electrolysers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

The native-resin provider is imported separately from
``lci-ptfe-gasket-native-312.xlsx``, with source technosphere and biosphere
versions both 3.12. Before preparing the additional electrolysis workbook for
that target, Premise removes its legacy gasket producer from the extracted
source and relinks only the reviewed AEC purchase. The electrolysis workbook
keeps its original 3.9 source declaration. Targets 3.8–3.11 continue to use
the original monomer-based reference; no native-resin backport or broad
background-version change is implied. The source workbooks, native datasets
and published CLIC versions are not rewritten by this import selection.

The foreground compilation is CC BY 4.0 with compiler attribution and source
qualifications recorded in the workbook. Its pinned scientific reference is
``scripts/data/ptfe-gasket-native-312-adoption-20261027.json``. Referenced
publications and native supplier inventories retain their separate terms.

Shared PFSA membrane reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The additional PEM electrolyser and flow-battery inventories share
``perfluorosulfonic acid membrane production, ew1100, engineering reference``.
It is supplied by ``lci-pfsa-ew1100.xlsx``, imported before its buyers with
source technosphere and biosphere versions both 3.9. The two former
application-specific producers had identical supplier identities, quantities,
uncertainties and formulas; consolidation preserves all 12 external exchanges
and their quantities. The original electrolysis and VRFB workbooks retain
their 3.9 source declarations. Normal import migration targets the selected
output version.

The PEM electrolyser stack still purchases **16 kg** membrane. The VRFB
``nafion membrane production`` wrapper still purchases **1 kg** of finished
membrane per kg wrapper output and retains its factory and freight inputs
once. The VRFB and hybrid-flow stacks still purchase **150 kg** and
**0.132 kg** of that wrapper, respectively. The separate fuel-cell membrane
recipe is not part of this consolidation.

This is a shared dry, unreinforced acid-form EW1100 engineering reference,
not proof of commercial-grade equivalence across applications. Existing
conversion, retention, process-water, energy and effluent assumptions remain
qualified; unmodelled purification, reinforcement, hydration, PFAS releases
and fluoride-specific abatement are not zero burdens. The shared reference
retains its approved CC BY-NC 4.0 foreground terms, compiler attribution and
source citations. Referenced publications and background inventories retain
their separate terms. The pinned scientific reference is
``scripts/data/pfsa-ew1100-adoption-20261027.json``.

AEC diaphragm manufacturing rejects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
coating rejects, disposal and hazardous classification are unverified. The
pinned scientific reference and catalogue receipts are in
``scripts/data/aec-diaphragm-reject-adoption-20261027.json``.

SOEC glass-seal manufacturing rejects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
unquantified volatile losses are separate. The pinned reference is
``scripts/data/soec-glass-reject-adoption-20261027.json``.

Lanthanum-reject fate in SOEC interconnect supply
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The oxide-electrolysis lanthanum helper retains its 95% yield, precursor
budget, graphite, reaction CO2 and **10 kWh electricity plus 3 MJ heat per kg
La**. Its nominal **0.05263157894736836 kg La-equivalent loss per kg output**
has no supported treatment destination. The former inert-disposal purchase
is removed to align with CLIC; the loss and its treatment impacts remain
explicitly unassigned. This does not represent zero physical loss or an
environmental improvement.

The Crofer-type alloy still purchases **0.001 kg La per kg alloy**. The
SOEC stack reference retains **8,976.1 kg alloy** and the same native
sheet-working purchase, including its existing stock makeup and uncertainty.
The unassigned nominal La-equivalent loss is **0.4724263157894732 kg** on
that reference. Both source declarations stay 3.9. The pinned reference is
``scripts/data/lanthanum-reject-fate-adoption-20261027.json``.

SiC wafer full-crystal material accounting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The physical-vapour-transport wafer helper counts the historical reference's
full **100 mm crystal**, including its unusable regions. Forty-seven wafers
at **0.35 mm** thickness retain 16.45 mm of crystal, giving a mass retention
of **0.1645**. Per kg finished physical wafer, the reference therefore buys
**6.079027355623101 kg SiC** and sends **5.079027355623101 kg rejects** to
the existing qualified mineral-waste proxy once. The previous 66 mm usable
section omitted material lost outside that section. No recovery credit is
added; actual waste classification and destination remain unverified.

Substrate electricity stays **19,932.858629976276 kWh/kg physical wafer**.
The material correction does not rescale the separately derived electricity
reference or change downstream wafer, die, module or PCS coefficients.
This historical geometry is a reference scenario, not measured modern-factory
yield. Purification, charge losses, process gases, direct emissions and
industrial qualification remain unresolved. Technosphere and biosphere
source declarations both stay 3.9. The pinned reference is
``scripts/data/sic-full-boule-adoption-20261027.json``.

Transformer-core supply for ecoinvent 3.12
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dedicated 1.1 MVA electrolysis transformer retains **947.32 kg core
steel**. When building with ecoinvent 3.12, it purchases the native RER
market for grain-oriented electrical steel, **0.23 mm finished coil**.
This replaces the six reviewed core inputs together: unalloyed steel,
metallurgical silicon, hot rolling, sheet rolling, annealing electricity
and furnace heat. All other transformer inputs and retirement quantities
remain unchanged. Native nominal silicon is **3.2%**, replacing the legacy
3% engineering-alloy assumption. RER procurement and grade/gauge suitability
remain qualified proxies; core cutting, stacking, insulation and
electrical-design coverage remain unverified.

The unchanged electrolysis workbook retains its original technosphere and
biosphere source declarations at **3.9**. The default importer selects the
native core after normal migration and before linking, only for the
**3.12 target family**. Other targets retain the legacy recipe. An absent or
ambiguous native supplier, or a changed scientific recipe, fails validation
before replacement. Native background activities are never modified.
Imported-inventory caches for 3.12 are invalidated; native source caches and
older-target cache keys remain unchanged. The pinned authoring reference is
``scripts/data/transformer-native-core-adoption-20261027.json``.

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
``hydrogen supply, distributed by pipeline`` service. Its source comment
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
at their respective stages. Rebuild scenarios to receive the correction.

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

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-hydrogen.inc

Detailed supply-chain names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/hydrogen-variants.inc


Vehicle hydrogen tank inventories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The HDPE-lined 700-bar tank activity in ``lci-trucks.xlsx`` produces one kg of
dry tank assembly, including tank balance of plant and excluding stored
hydrogen. Its quantities come from `Evangelisti et al. (2017)
<https://doi.org/10.1016/j.jclepro.2016.11.159>`_ and its
`supplementary inventory tables
<https://ars.els-cdn.com/content/image/1-s2.0-S0959652616320212-mmc1.docx>`_.
The supplement was checked on 10 October 2026 (SHA256
``ca9470c504f61ef58e2e5cc3cad8c06d6cecf17073074ac2ce19dca3678ac49d``).

The paper's main text, scenario labels and supplementary output masses are
inconsistent. The retained recipe is the table containing HDPE. Its corrected
denominator is the sum of the listed materials, **87.71 kg**, rather than the
legacy 119 kg denominator. Material proportions therefore sum to one kg per kg
of dry assembly. The difference from the nominal output is approximately stored
hydrogen, but the paper does not state that boundary unambiguously. Dry-mass
normalization is an explicit modelling choice; no unidentified missing material
is added.

Manufacturing electricity is published as 12.87 **MJ per assembly**. Dividing
by 3.6 MJ/kWh and 87.71 kg gives 0.0407593 kWh/kg. Previously this MJ value had
been treated as kWh. Steel working is a processing service and does not add
material mass.

Dataset metadata and its comment record the source, formulas and boundary.
Existing supplier proxies, including AlLi for aluminium, are retained; this
correction does not establish current vehicle composition, sizing or
manufacturing yields.

``tests/test_hydrogen_tank_inventories.py`` checks material closure and energy
units after Brightway Excel import and its preparation strategies, without
writing a database, plus quantity preservation through migrations to ecoinvent
3.9--3.12. Existing generated databases must be rebuilt before their tank
inventories change.
