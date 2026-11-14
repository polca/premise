Generation technologies and efficiency adjustments
====================================================

.. raw:: html

   <span id="power-generation"></span>

.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``electricity`` update regionalizes mapped generation activities and
adjusts their performance for a fixed electricity output, normally 1 kWh.
Generation inventories describe production; :doc:`markets` describes how
those suppliers are combined. :doc:`photovoltaics` has its own area-based
transformation.

.. figure:: /_static/process-diagrams/electricity-generation.svg
   :class: process-diagram
   :alt: Electricity transformation: Prepare source activities; Regionalize generation and build markets; Apply available IAM efficiencies; Relink consumers and check.
   :align: center

Inputs and applicability
--------------------------

The electricity update is skipped when the scenario has no electricity mix.
If the mix exists but efficiency data are absent, market construction and
the source-specific adjustments still run; IAM efficiency scaling is skipped.

Inputs combine existing generation inventories, version-selected additional
inventories, and mapped IAM generation and efficiency series. The
``use_absolute_efficiency`` constructor option selects absolute targets or
relative efficiency changes. ``ndb.update("electricity")`` includes generation
and market transformations; there is no separate update call for this page.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("electricity")

Natural-gas extraction and distribution inventories are described under
:ref:`natural-gas-source-inventories`. The separate
:ref:`geothermal heat inventories <geothermal-heat-inventories>` belong to the
heat supply chapter.

Transformation
----------------

The update first prepares missing generation routes, coal factors, PV areas
and Swiss reservoir-water flows, then creates regional generation activities.
It constructs the voltage markets before applying the IAM combustion-efficiency
step and relinking suppliers. Aluminium-market adjustments run from 2020.

For a selected activity, the IAM efficiency step rescales **all technosphere
and biosphere exchanges**, leaving the production exchange unchanged. Fuel
filters determine the starting efficiency; they do not limit the exchanges
rescaled afterwards. Infrastructure, materials, water and direct non-CO2
emissions in that activity therefore also change with the same factor.

Relative efficiency targets are clipped to the configured technology bounds.
The absolute-target branch does not apply that clipping; it uses the target
when the source efficiency is not the fallback value of one and the target
is neither zero nor NaN. Activities
already carrying an efficiency-adjustment result are not scaled again by
this step.

Swiss reservoir hydropower activities receive the packaged water-flow
coefficients before regionalization. This is a source-specific water
adjustment, separate from combustion efficiency and PV area scaling.

Efficiency adjustment
~~~~~~~~~~~~~~~~~~~~~~~

The energy conversion efficiency of power plant datasets for specific technologies is adjusted
to align with the efficiency changes indicated by the IAM scenario.

Two approaches are possible (`use_absolute_efficiency`):

* application of a scaling factor to the inputs of the dataset relative to the current efficiency
* application of a scaling factor to the inputs of the dataset to match the absolute efficiency given by the IAM scenario

The first approach (default) applies the IAM's relative improvement to the
inventory's starting efficiency, subject to technology-specific bounds. The
second targets the IAM's absolute efficiency. Both derive the exchange scaling
factor from the old efficiency divided by the new efficiency; their difference
is the efficiency target, not a different rule for input shares.

Combustion-based powerplants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, *premise* adjust the efficiency of coal- and lignite-fired
power plants on the basis of the data published by `Oberschelp <https://www.nature.com/articles/s41893-019-0221-6>`__ et al. (2019),
to update some datasets in ecoinvent, which are, for some of them, several decades
old. More specifically, the data provides plant-specific efficiency
and emissions factors. Premise averages them by country and fuel type to obtain
volume-weighted factors. The efficiency of the following datasets is updated:

* electricity production, hard coal
* electricity production, lignite
* heat and power co-generation, hard coal
* heat and power co-generation, lignite

The data from `Oberschelp <https://www.nature.com/articles/s41893-019-0221-6>`__ et al. (2019) also allows us to update emissions of
SO2, NOx, CH4, and PMs.

Second, *premise* iterates through coal, lignite, natural gas, biogas, and wood-fired power plant datasets
in the LCI database to calculate their current efficiency (i.e., the ratio of output energy produced to primary fuel
energy entering the process, which is often 1 kWh).
If the IAM scenario anticipates a change in efficiency for these processes, the inputs of the
datasets are scaled up or down by the scaling factor to effectively reflect a change in
fuel input per kWh produced.

The origin of this scaling factor is the IAM scenario selected.

To calculate the old and new efficiency of the dataset, it is necessary to know
the net calorific content of the fuel. The table below shows the Lower Heating Value for
the different fuels used in combustion-based power plants.

 ================================================================== ===========================
  name of fuel                                                       LHV [MJ/kg, as received]
 ================================================================== ===========================
  hard coal                                                          26.7
  lignite                                                            11.2
  petroleum coke                                                     31.3
  wood pellet                                                        16.2
  wood chips                                                         18.9
  natural gas                                                        45
  gas, natural, in ground                                            45
  refinery gas                                                       50.3
  propane                                                            46.46
  heavy fuel oil                                                     38.5
  oil, crude, in ground                                              38.5
  light fuel oil                                                     42.6
  biogas                                                             22.73
  biomethane                                                         47.5
  waste                                                              14
  methane, fossil                                                    47.5
  methane, biogenic                                                  47.5
  methane, synthetic                                                 47.5
  diesel                                                             43
  gasoline                                                           42.6
  petrol, 5% ethanol                                                 41.7
  petrol, synthetic, hydrogen                                        42.6
  petrol, synthetic, coal                                            42.6
  diesel, synthetic, hydrogen                                        43
  diesel, synthetic, coal                                            43
  diesel, synthetic, wood                                            43
  diesel, synthetic, wood, with CCS                                  43
  diesel, synthetic, grass                                           43
  diesel, synthetic, grass, with CCS                                 43
  hydrogen, petroleum                                                120
  hydrogen, electrolysis                                             120
  hydrogen, biomass                                                  120
  hydrogen, biomass, with CCS                                        120
  hydrogen, coal                                                     120
  hydrogen, from natural gas                                                 120
  hydrogen, from natural gas, with CCS                                       120
  hydrogen, biogas                                                   120
  hydrogen, biogas, with CCS                                         120
  hydrogen                                                           120
  biodiesel, oil                                                     38
  biodiesel, oil, with CCS                                           38
  bioethanol, wood                                                   26.5
  bioethanol, wood, with CCS                                         26.5
  bioethanol, grass                                                  26.5
  bioethanol, grass, with CCS                                        26.5
  bioethanol, grain                                                  26.5
  bioethanol, grain, with CCS                                        26.5
  bioethanol, sugar                                                  26.5
  bioethanol, sugar, with CCS                                        26.5
  ethanol                                                            26.5
  methanol, wood                                                     19.9
  methanol, grass                                                    19.9
  methanol, wood, with CCS                                           19.9
  methanol, grass, with CCS                                          19.9
  liquified petroleum gas, natural                                   45.5
  liquified petroleum gas, synthetic                                 45.5
  uranium, enriched 3.8%, in fuel element for light water reactor    4199040
  nuclear fuel element, for boiling water reactor, uo2 3.8%          4147200
  nuclear fuel element, for boiling water reactor, uo2 4.0%          4147200
  nuclear fuel element, for pressure water reactor, uo2 3.8%         4579200
  nuclear fuel element, for pressure water reactor, uo2 4.0%         4579200
  nuclear fuel element, for pressure water reactor, uo2 4.2%         4579200
  uranium hexafluoride                                               709166
  enriched uranium, 4.2%                                             4579200
  mox fuel element                                                   4579200
  heat, from hard coal                                               1
  heat, from lignite                                                 1
  heat, from petroleum coke                                          1
  heat, from wood pellet                                             1
  heat, from natural gas, high pressure                              1
  heat, from natural gas, low pressure                               1
  heat, from heavy fuel oil                                          1
  heat, from light fuel oil                                          1
  heat, from biogas                                                  1
  heat, from waste                                                   1
  heat, from methane, fossil                                         1
  heat, from methane, biogenic                                       1
  heat, from diesel                                                  1
  heat, from gasoline                                                1
  heat, from bioethanol                                              1
  heat, from biodiesel                                               1
  heat, from liquified petroleum gas, natural                        1
  heat, from liquified petroleum gas, synthetic                      1
  bagasse, from sugarcane                                            15.4
  bagasse, from sweet sorghum                                        13.8
  sweet sorghum stem                                                 4.45
  cottonseed                                                         21.97
  flax husks                                                         21.5
  coconut husk                                                       20
  sugar beet pulp                                                    5.11
  cleft timber                                                       14.46
  rape meal                                                          31.1
  molasse, from sugar beet                                           16.65
  sugar beet                                                         4.1
  barkey grain                                                       19.49
  rye grain                                                          12
  sugarcane                                                          5.3
  palm date                                                          10.8
  whey                                                               1.28
  straw                                                              15.5
  grass                                                              17
  manure, liquid                                                     0.875
  manure, solid                                                      3.6
  kerosene, from petroleum                                           43
  kerosene, synthetic, from electrolysis, energy allocation          43
  kerosene, synthetic, from electrolysis, economic allocation        43
  kerosene, synthetic, from coal, energy allocation                  43
  kerosene, synthetic, from coal, economic allocation                43
  kerosene, synthetic, from natural gas, energy allocation           43
  kerosene, synthetic, from natural gas, economic allocation         43
  kerosene, synthetic, from biomethane, energy allocation            43
  kerosene, synthetic, from biomethane, economic allocation          43
  kerosene, synthetic, from biomass, energy allocation               43
  kerosene, synthetic, from biomass, economic allocation             43
 ================================================================== ===========================

Additionally, the biogenic and fossil CO2 emissions of the datasets are also
scaled up or down by the same factor, as they are proportional to the amount of fuel used.

See :ref:`generation-worked-example` for the relative and absolute
calculations, including the non-fuel exchanges scaled by this step.

The relative electricity branch applies technology-specific efficiency bounds
before calculating its final exchange factor. The absolute branch derives the
factor from the IAM target when valid. These checks should not be interpreted
as a general guarantee that every technology improves monotonically between
all scenario years.

.. note::

    You can check the efficiencies assumed in your scenarios by generating
    a scenario summary report, or a report of changes. Automatic report generation depends on ``generate_reports``; you can also
    request reports explicitly after an update:

.. code-block:: python

    ndb.generate_scenario_report()
    ndb.generate_change_report()

Markets and downstream links
------------------------------

Regionalized generation supplies the voltage-specific markets described in
:doc:`markets`. A technology's presence in the import catalogue does not
establish its scenario share. Follow a consuming activity through its market
to distinguish a generation-efficiency change from a change in supply mix.

Assumptions and limitations
-----------------------------

Efficiency bounds and selected exchanges differ by technology. CCS energy
requirements, non-combustion technologies and storage cannot be interpreted
using a single combustion scaling equation. Scenario efficiencies and source
inventory efficiencies may use different boundaries; use the adjustment
rules in **Transformation** when interpreting a result.

.. _generation-worked-example:

Worked example and checks
---------------------------

An illustrative efficiency calculation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Consider a process producing **1 kWh of electricity** (3.6 MJ) at **40%
efficiency**. It needs ``3.6 / 0.40 = 9 MJ`` of fuel. Suppose its other inputs
include 0.001 unit of infrastructure and its direct fossil CO2 is 0.5 kg.
These are illustrative coefficients, not a measured plant inventory.

* **Relative case:** a 10% efficiency improvement gives
  ``0.40 × 1.10 = 0.44``. Assume this target is within the configured bounds.
  The exchange factor is ``0.40 / 0.44 = 0.909091``.
* **Absolute case:** a supplied target of 50% gives an exchange factor of
  ``0.40 / 0.50 = 0.8``.

.. list-table:: Exchange amounts per 1 kWh of electricity produced
   :header-rows: 1
   :widths: 40 20 20 20

   * - Exchange
     - Before
     - Relative case
     - Absolute case
   * - Electricity production (kWh)
     - 1
     - 1
     - 1
   * - Fuel input (MJ)
     - 9
     - 8.181818
     - 7.2
   * - Infrastructure purchase (unit)
     - 0.001
     - 0.000909091
     - 0.0008
   * - Direct fossil CO2 (kg)
     - 0.5
     - 0.454545
     - 0.4

The production amount stays fixed. This routine scales every selected
activity's non-production exchange, so the infrastructure input follows the
same factor as fuel and direct emissions. The energy balances agree:
``3.6 / 8.181818 ≈ 44%`` and ``3.6 / 7.2 = 50%``.

In a scenario, compare the recorded old and new efficiencies with the final
exchanges in **Key Changes**. Inspect the supplying market separately: its
technology shares and upstream suppliers can also change the life-cycle
result. :ref:`interpretation-arithmetic-example` provides a runnable version
of the efficiency arithmetic.

Sources and inventory details
-------------------------------

Power plants with CCS
~~~~~~~~~~~~~~~~~~~~~

Datasets for power generation with Carbon Capture and Storage (CCS) are imported.
They originate from `Volkart <https://doi.org/10.1016/j.ijggc.2013.03.003>`__ et al. 2013, and can be consulted here: `electricity production, at hard coal-fired power plant, post, pipeline 200km, storage 1000m (RER) <https://clic.psi.ch/datasets/electricity-production-at-hard-coal-fired-power-plant-post-pipeline-200km-storage-1000m/>`__; `electricity production, at natural gas-fired combined cycle power plant, post, pipeline 200km, storage 1000m (RER) <https://clic.psi.ch/datasets/electricity-production-at-natural-gas-fired-combined-cycle-power-plant-post-pipeline-200km-storage-1000m/>`__.
An exception to this are the inventories for `biomass-based integrated gasification combined cycle power plants (BIGCCS) <https://clic.psi.ch/datasets/electricity-production-at-biomass-fired-igcc-power-plant-post-pipeline-200km-storage-1000m/>`__,
which are from `Briones-Hidrovo <https://doi.org/10.1016/j.jclepro.2020.125680>`__ et al, 2020.

The table below lists the names of the new activities (only production datasets are shown).

.. list-table::
   :header-rows: 1

   * - Power generation with CCS (activities list)
     - location
     - CLIC dataset page
   * - electricity production, at power plant/hard coal, IGCC, no CCS
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-hard-coal-fired-igcc-power-plant/>`__
   * - electricity production, at power plant/hard coal, PC, no CCS
     - RER
     - —
   * - electricity production, at power plant/hard coal, oxy, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-hard-coal-fired-power-plant-oxy-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/hard coal, oxy, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at power plant/hard coal, post, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-hard-coal-fired-power-plant-post-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/hard coal, post, pipeline 400km, storage 1000m
     - RER
     - —
   * - electricity production, at power plant/hard coal, post, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at power plant/hard coal, pre, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-hard-coal-fired-igcc-power-plant-pre-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/hard coal, pre, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at power plant/lignite, IGCC, no CCS
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-lignite-fired-igcc-power-plant/>`__
   * - electricity production, at power plant/lignite, PC, no CCS
     - RER
     - —
   * - electricity production, at power plant/lignite, oxy, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-lignite-fired-power-plant-oxy-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/lignite, oxy, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at power plant/lignite, post, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-lignite-fired-power-plant-post-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/lignite, post, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at power plant/lignite, pre, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-lignite-fired-igcc-power-plant-pre-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/lignite, pre, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at power plant/natural gas, ATR H2-CC, no CCS
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-hydrogen-fired-combined-cycle-power-plant-by-auto-thermal-reforming-of-natural-gas/>`__
   * - electricity production, at power plant/natural gas, NGCC, no CCS/kWh
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-natural-gas-fired-combined-cycle-power-plant/>`__
   * - electricity production, at power plant/natural gas, post, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-natural-gas-fired-combined-cycle-power-plant-post-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/natural gas, post, pipeline 400km, storage 1000m
     - RER
     - —
   * - electricity production, at power plant/natural gas, post, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at power plant/natural gas, pre, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-hydrogen-fired-combined-cycle-power-plant-by-auto-thermal-reforming-of-natural-gas-pre-pipeline-200km-storage-1000m/>`__
   * - electricity production, at power plant/natural gas, pre, pipeline 400km, storage 3000m
     - RER
     - —
   * - electricity production, at wood burning power plant 20 MW, truck 25km, no CCS
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-wood-burning-power-plant/>`__
   * - electricity production, at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m
     - RER
     - `View dataset <https://clic.psi.ch/datasets/electricity-production-at-wood-burning-power-plant-post-pipeline-200km-storage-1000m/>`__
   * - electricity production, at wood burning power plant 20 MW, truck 25km, post, pipeline 400km, storage 3000m
     - RER
     - —

.. _epr-cladding-reference:

EPR cladding supply
~~~~~~~~~~~~~~~~~~~

Only for ecoinvent 3.12 cut-off, the
`EPR M5 cladding alloy reference <https://clic.psi.ch/datasets/zirconium-cladding-alloy-production-epr-m5-type/>`__
(source 3.12) is imported immediately before the
`EPR electricity inventory <https://clic.psi.ch/datasets/electricity-production-nuclear-evolutionary-power-reactor-epr/>`__
(source 3.8). Ordinary
import duplicate checks retain one M5 alloy producer, purchasing 0.9884 kg of
native nuclear-grade zirconium sponge per kg alloy; its six other inputs stay
unchanged. The qualified custom Kroll provider remains an unused legacy
alternative, so its reagents, yield, utilities and residues are not added again.

The original EPR source and four fuel-fabrication buyers keep their authored
3.8 profiles and assembly doses. Other target versions and consequential builds
retain the existing source selection.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-electricity.inc

.. raw:: html

   <span id="geothermal"></span>
   <span id="source-inventories-geothermal"></span>
   <span id="natural-gas"></span>
   <span id="source-inventories-power-plants-with-ccs"></span>

Natural-gas supply is documented under :ref:`natural-gas-source-inventories`,
geothermal heat under :ref:`geothermal-heat-inventories`, and CCS plants in
the source inventory table above.
