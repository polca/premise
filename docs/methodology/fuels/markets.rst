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

Reference energy and carbon properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fuel markets normalize eligible IAM energy shares before converting them into
supplier quantities. For energy share ``s`` and supplier heating value ``H``,
the purchased quantity is ``s * H_reference / H``. It is not subsequently
normalized to one kilogram or one cubic metre. The reference values are
42.6 MJ/kg for petrol, 43 MJ/kg for diesel and kerosene, 36 MJ/m3 for natural
gas, 120 MJ/kg for hydrogen and 45.5 MJ/kg for LPG. Methane suppliers expressed
in kilograms use their 47.5 MJ/kg property; their conversion into the gas
market is based on energy, not an assumed density.

The ``premise fuel properties`` metadata record actual supplier quantities,
units, energy and full-oxidation fossil/non-fossil CO2 potential. For example,
a 50/50 energy mix of petrol and ethanol purchases 0.5 kg petrol plus
``0.5 * 42.6 / 26.5`` kg ethanol per reference kilogram. Consumer purchases
must not be adjusted a second time using the physical mixture's average LHV.
Market electricity, transport and infrastructure remain additional burdens.
World properties follow the production-weighted regional markets.

Synthetic-fuel carbon origin is traced through carbon-bearing inputs. Direct
air capture is atmospheric carbon, represented as non-fossil combustion CO2;
cement and waste-incineration capture retain the explicit biogenic shares of
the capture inventories. Coal-derived carbon remains fossil. The source of
hydrogen does not establish the origin of carbon. Unknown origins retain an
explicit unresolved CO2 amount and a validation diagnostic. These properties
describe oxidation potential separately from upstream capture credits and
other carbon-containing pollutants; they are not an elemental carbon closure.

Vehicle-grade natural gas
~~~~~~~~~~~~~~~~~~~~~~~~~

The kilogram-based vehicle-grade natural-gas market obtains compressed gas
from a regional ``natural gas production, medium pressure, vehicle grade``
activity. Premise regionalizes this compression provider before the market,
so its upstream high-pressure gas and compression electricity use the same
IAM region whenever those suppliers are available. The inherited gas-volume
conversion, electricity quantity, infrastructure requirements and direct losses
are preserved. Compression remains part of fuel supply rather than a second
electricity purchase in the vehicle inventory.

Original geographic production and market activities become forwarding
activities with one kilogram of the same product from their IAM replacement.
Global originals use the World replacement where available. This forwarding
balance is distinct from an energy-normalized fuel blend whose supplier masses
can legitimately sum to more than one kilogram.

The delivered CNG reference is **46.5 MJ/kg**, as documented by the ecoinvent
vehicle-grade gas market. The refreshed car/truck adapters preserve modelled
energy by dividing it by 46.5; their former 47.5 MJ/kg conversion understated
purchases by approximately 2.15%. This leaves the compression recipe unchanged.
The inherited 1.360544 m3/kg gas input represents about 48.98 MJ/kg at the
36 MJ/m3 reference. Metadata record this upstream/delivered boundary difference
explicitly; it is not attributed entirely to measured methane losses.

CO2 emissions update
~~~~~~~~~~~~~~~~~~~~~~

*premise* iterates through activities that consume any of the newly created
fuel markets to update the way CO2 emissions are modelled. Based on the fuel
market composition, CO2 emissions within the fuel-consuming activity are
split between fossil and non-fossil emissions.

For passenger-car and truck services, the common fuel-carbon helper runs after
fuel relinking and again after vehicle regionalization and efficiency changes.
It calculates each CO2 fraction as the sum of the **final purchased quantity
times the linked supplier's oxidation factor**. Carbon shares need not equal
IAM energy shares. A clone must use its actual supplier, rather than inherit
its template's regional split. The positive exhaust CO2 pool follows the
full-oxidation convention; separate CO, CH4 and NMVOC emissions and negative
capture credits remain unchanged. This is not an elemental carbon closure.

The ``premise vehicle fuel CO2`` record identifies the actual suppliers, their
quantities and factors, the reference production and the resulting emissions.
Unknown carbon origin remains an explicit diagnostic. LPG without a scenario
route uses the documented fossil fallback, 45.5 MJ/kg and 0.0631 kg CO2/MJ,
and records its actual supplier geography. Forwarding activities and vehicles
without direct combustion fuel do not receive a direct exhaust pool.

The existing reclassification helper handles the fossil/non-fossil transfer.
Stored transfer amounts scale with their exchanges, so a later update cannot
reuse a pre-efficiency amount. Reclassification itself does not rescale other
fuel transfers. Repeating the final vehicle-carbon calculation is idempotent.
Validation independently compares both CO2 fractions with the current links.
Industrial consumers retain their existing residual-emissions treatment,
including the limit on emissions that can be reclassified after CCS; the
vehicle-specific full-oxidation calculation does not apply to them.

The historical illustrative table below shows a classification-only change
for a 3.5t truck after linking to an Indian regional diesel market. Current
builds calculate the amounts from their actual linked fuel properties; total
CO2 can also change when those properties differ from the original inventory.

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
The fuel inputs total more than 1 kg (1.4 kg in this example).
This is because the market dataset as "1 kg" as reference unit, and
methanol and bioethanol have low
calorific values comparatively to petrol
(i.e., 19.9 and 26.5 MJ/kg respectively, vs. 42.6 MJ/kg for gasoline).
Their inputs are scaled up to reach an average calorific value
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
