Fuel markets
==============


.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

Within ``ndb.update("fuels")``, Premise combines mapped production routes
into regional liquid-fuel, gas and hydrogen markets, then updates affected
consumer links and carbon accounting. Each market retains a stated reference
product, unit and energy basis.

.. figure:: /_static/process-diagrams/fuels-markets.svg
   :class: process-diagram
   :alt: Regional fuel markets: Prepare hydrogen, liquid and gas routes; Select suppliers from production volumes; Convert energy shares to exchange amounts; Relink consumers and update CO2.
   :align: center

Inputs and applicability
--------------------------

Mapped scenario production supplies technology weights, inventories provide
suppliers, and fuel properties supply lower heating values and carbon factors.
The fuels branch requires at least one available blend array among petrol,
diesel, natural gas and hydrogen. Upstream biomass, electricity and hydrogen
preparation determines which prospective suppliers are available.

Transformation
----------------

Market construction translates energy-based shares to supplier exchange
amounts. The amount sum can differ from one for fuels with different heating
values. Consumer carbon dioxide handling uses the linked fuel composition;
the detailed examples below illustrate those separate operations.

CO2 emissions update
~~~~~~~~~~~~~~~~~~~~~~

*premise* iterates through activities that consume any of the newly created
fuel markets to update the way CO2 emissions are modelled. Based on the fuel
market composition, CO2 emissions within the fuel-consuming activity are
split between fossil and non-fossil emissions.

The table below shows the example where the CO2 emissions of a 3.5t truck
have been split into biogenic and fossil fractions after re-link to the
new diesel market of the REMIND region for India.

 ========================================== =========== ========== ================ ===========
  Output                                     before      after      _                _
 ========================================== =========== ========== ================ ===========
  producer                                   amount      amount     unit             location
  transport, freight, lorry, diesel, 3.5t    1           1          ton-kilometer    IND
  Input
  supplier                                   amount      amount     unit             location
  treatment of tyre wear emissions, lorry    -0.0009     -0.0009    kilogram         RER
  market for road maintenance                0.0049      0.0049     meter-year       RER
  market for road                            0.0041      0.0041     meter-year       GLO
  treatment of road wear emissions, lorry    -0.0008     -0.0008    kilogram         RER
  market for refrigerant R134a               2.84E-05    2.84E-05   kilogram         GLO
  treatment of brake wear emissions, lorry   -0.0005     -0.0005    kilogram         RER
  Light duty truck, diesel, 3.5t             1.39E-05    1.39E-05   unit             RER
  market for diesel, low-sulfur              0.1854      0.1854     kilogram         IND
  **Carbon dioxide, fossil**                 0.5840      0.5667     kilogram         _
  **Carbon dioxide, non-fossil**             0.0000      0.0173     kilogram         _
  Nitrogen oxides                            0.0008      0.0008     kilogram         _
  Nitrogen oxides                            0.0003      0.0003     kilogram         _
 ========================================== =========== ========== ================ ===========

Markets and downstream links
------------------------------

Influence of differing LHV on fuel market composition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because not all competing fuels of a same type have similar calorific values,
some adjustments are made. The table below shows the example of the market for
gasoline, for the IMAGE region of Western Europe in 2050.
The sum of fuel inputs is superior to 1 (i.e., 1.4 kg).
This is because the market dataset as "1 kg" as reference unit, and
methanol and bioethanol have low
calorific values comparatively to petrol
(i.e., 19.9 and 26.5 MJ/kg respectively, vs. 42.6 MJ/kg for gasoline).
Hence, their inputs are scaled up to reach an average calorific value
of 42.6 MJ/kg of fuel supplied by the market.

This is necessary as gasoline-consuming activities in the lCI database
are modelled with the calorific value of conventional gasoline.

 =================================================================== ========= =========== ===========
  Output                                                              _         _           _
 =================================================================== ========= =========== ===========
  producer                                                            amount    unit        location
  market for petrol, low-sulfur                                       1         kilogram    WEU
  Input
  supplier                                                            amount    unit        location
  petrol production, low-sulfur                                       0.550     kilogram    CH
  market for methanol, from biomass                                   0.169     kilogram    CH
  market for methanol, from biomass                                   0.148     kilogram    CH
  market for methanol, from biomass                                   0.122     kilogram    CH
  market for methanol, from biomass                                   0.122     kilogram    CH
  Ethanol production, via fermentation, from switchgrass              0.060     kilogram    WEU
  Ethanol production, via fermentation, from switchgrass, with CCS    0.053     kilogram    WEU
  Ethanol production, via fermentation, from sugarbeet                0.051     kilogram    WEU
  Ethanol production, via fermentation, from sugarbeet, with CCS      0.051     kilogram    WEU
  Ethanol production, via fermentation, from poplar, with CCS         0.041     kilogram    WEU
  Ethanol production, via fermentation, from poplar                   0.041     kilogram    WEU
 =================================================================== ========= =========== ===========

.. raw:: html

   <span id="system-model-rules"></span>

Assumptions and limitations
-----------------------------

Average and marginal mixes use different weighting rules. Waste-treatment
suppliers require their documented sign convention. Check energy closure
before interpreting a non-unit mass sum as an error. Carbon-origin corrections
must be evaluated together with fuel consumption and upstream capture.

Consequential fuel markets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Consequential markets use marginal production mixes calculated in
``premise/marginal_mixes.py``. Technologies registered as constrained suppliers,
including pathways dependent on residual or waste feedstocks, are set to zero before
the remaining marginal shares are normalized.

The constrained fuel pathways are maintained in
``premise/data/consequential/constrained_suppliers.yaml``. They currently include:

* ``bioethanol, from residues``;
* ``biodiesel, from used cooking oil, with CCS``;
* ``methane, from biomass``;
* ``biomass - residual``;
* ``liquefied petroleum gas, synthetic, from coal``; and
* ``liquefied petroleum gas, synthetic, from coal, with CCS``.


Cutoff fuel markets
~~~~~~~~~~~~~~~~~~~~~

Cutoff markets may legitimately use a waste-treatment activity as a fuel supplier.
Such activities are identified by an activity name beginning with ``treatment``.
After positive fuel shares have been normalized, the technosphere exchange to the
treatment supplier is written as a negative amount. This follows the ecoinvent waste
exchange convention and prevents the treatment activity from producing an unintended
negative fuel burden.

The sign rule is enabled for generated liquid-fuel, gas, and hydrogen markets. It is
not applied to consequential markets or to non-treatment suppliers.

.. raw:: html

   <span id="lcia-regression-baselines"></span>

Worked example and checks
---------------------------

For an illustrative market delivering 40 MJ, equal energy contributions
from fuels at 40 and 20 MJ/kg require 0.5 and 1 kg, respectively. Their total
mass is 1.5 kg while energy closes at 40 MJ. Inspect market heating values,
signed treatment inputs and fossil/non-fossil CO2 in one downstream consumer.


Sources and inventory details
-------------------------------

See the source inventories and mappings linked above, and
:doc:`/reference/inventories` for constructor import selection.
