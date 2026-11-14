Synthetic fuels
=================

.. raw:: html

   <span id="source-inventories-synthetic-fuels"></span>

.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

Premise imports synthetic-fuel routes and regionalizes mapped production
and supporting activities within ``ndb.update("fuels")``. The products include
liquid hydrocarbons, methanol and synthetic methane; their reference units and
delivery boundaries vary by activity.

.. figure:: /_static/process-diagrams/fuels-synthetic.svg
   :class: process-diagram
   :alt: Synthetic-fuel supply chains: Regionalize mapped synthesis routes; Connect hydrogen and CO2 supplies; Construct mapped liquid or gas markets; Relink consuming activities.
   :align: center

Inputs and applicability
--------------------------

Imported inventories define conversion processes and hydrogen/CO2 inputs.
``fuels.yaml`` maps scenario production to synthetic technologies, while
``premise/data/fuels/liquid_fuel_activities.yml`` selects supporting synthesis
and distillation activities. At least one available petrol, diesel,
natural-gas or hydrogen blend array activates the fuels branch.

Transformation
----------------

The routine regionalizes mapped synthetic technologies with production
volumes, then supporting methanol and other liquid-fuel activities. It does
not pass a synthesis-efficiency adjustment to those operations.
Hydrogen production can change through its separate efficiency routine, and
relinked energy inputs can change upstream burdens. These effects must not
be described as a direct improvement in every synthesis process.

Markets and downstream links
------------------------------

Liquid products supply the regional fuel blends where mapped scenario
production selects them; synthetic methane participates in the gas pathway.
The imported route catalogue is not the market composition. Follow
:doc:`markets` for heating-value conversion and fossil/non-fossil CO2 handling.

Assumptions and limitations
-----------------------------

Carbon diverted into fuel is not necessarily permanently stored. The
inventory carbon corrections below describe intermediate accounting; the
subsequent use and release of fuel carbon remain within the life-cycle
boundary. Compare routes with the same CO2 source, hydrogen supply and final
product before attributing changes to synthesis technology.

Worked example and checks
---------------------------

Suppose a hypothetical synthesis activity purchases **0.2 kg hydrogen per
kg fuel** before and after the update. To isolate the effect of hydrogen
supply, assume its supplier's impact changes from **10 to 2 kg CO2-eq per
kg hydrogen**, with all other contributions held fixed. These impact factors
are illustrative, not results for a published inventory.

.. list-table:: Hydrogen's contribution per kg of synthetic fuel
   :header-rows: 1
   :widths: 40 30 30

   * - Quantity
     - Before
     - After
   * - Hydrogen purchase
     - 0.2 kg
     - 0.2 kg
   * - Supplier impact per kg hydrogen
     - 10 kg CO2-eq
     - 2 kg CO2-eq
   * - Hydrogen contribution
     - 0.2 × 10 = 2 kg CO2-eq
     - 0.2 × 2 = 0.4 kg CO2-eq

The difference is **1.6 kg CO2-eq per kg fuel** in this contribution, despite
no change in the synthesis hydrogen amount. This is an upstream supplier
change, not a direct synthesis-efficiency improvement or the fuel's total
footprint. Inspect the actual supplier and use the same LCIA method when
making this comparison. Trace captured carbon through synthesis and final
combustion separately; using captured carbon in fuel is not permanent storage.

Sources and inventory details
-------------------------------

Process context
~~~~~~~~~~~~~~~~~

*premise* imports inventories for the synthesis of hydrocarbon fuels
following three pathways:

* *Fischer-Tropsch*: it uses hydrogen and CO (from CO2 via a reverse water gas
  shift process) to produce "syncrude", which is distilled into diesel, kerosene,
  naphtha and lubricating oil and waxes. Inventories are from van der `Giesen <https://pubs.acs.org/doi/abs/10.1021/es500191g>`__ et al. 2014.
* *Methanol-to-liquids*: methanol is synthesized from hydrogen and CO2, and further
  distilled into gasoline, diesel, LGP and kerosene. Synthetic methanol inventories
  are from `Hank <https://doi.org/10.1039/C9SE00658C>`__ et al. 2019. The methanol to fuel process specifications are from
  `FVV <https://www.fvv-net.de/fileadmin/user_upload/medien/materialien/FVV-Kraftstoffstudie_LBST_2013-10-30.pdf>`__ 2013.
* *Electro-chemical methanation*: methane is produced from hydrogen and CO2 using
  a Sabatier methanation reactor. Inventories are from `Zhang <https://doi.org/10.1039/C9SE00986H>`__ et al, 2019.

In their default configuration, these fuels use hydrogen from electrolysis and CO2
from direct air capture (DAC). However, *premise* builds different configurations
(i.e., CO2 and hydrogen sources) for these fuels, for each IAM region:

The CLIC column links the published allocation variants of each source
route. IAM-region variants are created by Premise.

.. list-table::
   :header-rows: 1

   * - Fuel production dataset
     - location
     - source
     - CLIC dataset page
   * - Diesel production, synthetic, from Fischer Tropsch process, hydrogen from coal gasification, at fuelling station
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-coal-gasification-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-coal-gasification-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from Fischer Tropsch process, hydrogen from coal gasification, with CCS, at fuelling station
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-coal-gasification-with-ccs-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-coal-gasification-with-ccs-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from Fischer Tropsch process, hydrogen from electrolysis, at fuelling station
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-electrolysis-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-electrolysis-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, at fuelling station
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-wood-gasification-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-wood-gasification-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, with CCS, at fuelling station
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-wood-gasification-with-ccs-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-fischer-tropsch-process-hydrogen-from-wood-gasification-with-ccs-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from methanol, hydrogen from coal gasification, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-coal-gasification-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-coal-gasification-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from methanol, hydrogen from coal gasification, with CCS, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-coal-gasification-with-ccs-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-coal-gasification-with-ccs-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from methanol, hydrogen from electrolysis, CO2 from cement plant, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-cement-plant-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-cement-plant-energy-allocation-at-fuelling-station/>`__
   * - Diesel production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-dac-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/diesel-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-dac-energy-allocation-at-fuelling-station/>`__
   * - Gasoline production, synthetic, from methanol, hydrogen from coal gasification, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-coal-gasification-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-coal-gasification-energy-allocation-at-fuelling-station/>`__
   * - Gasoline production, synthetic, from methanol, hydrogen from coal gasification, with CCS, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-coal-gasification-with-ccs-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-coal-gasification-with-ccs-energy-allocation-at-fuelling-station/>`__
   * - Gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from cement plant, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-cement-plant-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-cement-plant-energy-allocation-at-fuelling-station/>`__
   * - Gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-dac-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/gasoline-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-dac-energy-allocation-at-fuelling-station/>`__
   * - Kerosene production, from methanol, hydrogen from coal gasification
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-from-methanol-hydrogen-from-coal-gasification-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-from-methanol-hydrogen-from-coal-gasification-energy-allocation/>`__
   * - Kerosene production, from methanol, hydrogen from electrolysis, CO2 from cement plant
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-from-methanol-hydrogen-from-electrolysis-co2-from-cement-plant-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-from-methanol-hydrogen-from-electrolysis-co2-from-cement-plant-energy-allocation/>`__
   * - Kerosene production, from methanol, hydrogen from electrolysis, CO2 from DAC
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-from-methanol-hydrogen-from-electrolysis-co2-from-dac-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-from-methanol-hydrogen-from-electrolysis-co2-from-dac-energy-allocation/>`__
   * - Kerosene production, synthetic, Fischer Tropsch process, hydrogen from coal gasification
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-coal-gasification-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-coal-gasification-energy-allocation/>`__
   * - Kerosene production, synthetic, Fischer Tropsch process, hydrogen from coal gasification, with CCS
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-coal-gasification-with-ccs-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-coal-gasification-with-ccs-energy-allocation/>`__
   * - Kerosene production, synthetic, Fischer Tropsch process, hydrogen from electrolysis
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-electrolysis-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-electrolysis-energy-allocation/>`__
   * - Kerosene production, synthetic, Fischer Tropsch process, hydrogen from wood gasification
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-energy-allocation/>`__
   * - Kerosene production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, with CCS
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-with-ccs-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/kerosene-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-with-ccs-energy-allocation/>`__
   * - Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from coal gasification
     - all IAM regions
     - van der Giesen et al. 2014
     - `energy allocation <https://clic.psi.ch/datasets/lubricating-oil-production-synthetic-fischer-tropsch-process-hydrogen-from-coal-gasification-energy-allocation/>`__
   * - Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from electrolysis
     - all IAM regions
     - van der Giesen et al. 2014
     - `energy allocation <https://clic.psi.ch/datasets/lubricating-oil-production-synthetic-fischer-tropsch-process-hydrogen-from-electrolysis-energy-allocation/>`__
   * - Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from wood gasification
     - all IAM regions
     - van der Giesen et al. 2014
     - `energy allocation <https://clic.psi.ch/datasets/lubricating-oil-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-energy-allocation/>`__
   * - Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, with CCS
     - all IAM regions
     - van der Giesen et al. 2014
     - `energy allocation <https://clic.psi.ch/datasets/lubricating-oil-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-with-ccs-energy-allocation/>`__
   * - Methane, synthetic, gaseous, 5 bar, from coal-based hydrogen, at fuelling station
     - all IAM regions
     - Zhang et al, 2019
     - `View dataset <https://clic.psi.ch/datasets/methane-synthetic-gaseous-5-bar-from-coal-based-hydrogen-at-fuelling-station/>`__
   * - Methane, synthetic, gaseous, 5 bar, from electrochemical methanation (H2 from electrolysis, CO2 from DAC using heat pump heat), at fuelling station, using heat pump heat
     - all IAM regions
     - Zhang et al, 2019
     - —
   * - Methane, synthetic, gaseous, 5 bar, from electrochemical methanation (H2 from electrolysis, CO2 from DAC using waste heat), at fuelling station, using waste heat
     - all IAM regions
     - Zhang et al, 2019
     - —
   * - Methane, synthetic, gaseous, 5 bar, from electrochemical methanation, at fuelling station
     - all IAM regions
     - Zhang et al, 2019
     - —
   * - Naphtha production, synthetic, Fischer Tropsch process, hydrogen from coal gasification
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-coal-gasification-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-coal-gasification-energy-allocation/>`__
   * - Naphtha production, synthetic, Fischer Tropsch process, hydrogen from electrolysis
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-electrolysis-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-electrolysis-energy-allocation/>`__
   * - Naphtha production, synthetic, Fischer Tropsch process, hydrogen from wood gasification
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-energy-allocation/>`__
   * - Naphtha production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, with CCS
     - all IAM regions
     - van der Giesen et al. 2014
     - `economic allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-with-ccs-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/naphtha-production-synthetic-fischer-tropsch-process-hydrogen-from-wood-gasification-with-ccs-energy-allocation/>`__
   * - Liquefied petroleum gas production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station
     - all IAM regions
     - Hank et al, 2019
     - `economic allocation <https://clic.psi.ch/datasets/liquefied-petroleum-gas-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-dac-economic-allocation-at-fuelling-station/>`__; `energy allocation <https://clic.psi.ch/datasets/liquefied-petroleum-gas-production-synthetic-from-methanol-hydrogen-from-electrolysis-co2-from-dac-energy-allocation-at-fuelling-station/>`__

In the case of wood and coal gasification-based fuels, the CO2 needed to produce methanol
or syncrude originates from the gasification process itself. This also implies
that in the methanol and/or RWGS process, a carbon balance correction is applied to reflect the
fact that a part of the CO2 from the gasification process is redirected into
the fuel production process.

If the CO2 originates from:

* a gasification process without CCS, a negative carbon correction is added to
  reflect the fact that part of the CO2 has not been emitted but has ended in the fuel instead.
* the gasification process with CCS, no carbon correction is necessary, because the source intermediate accounts for carbon retained in the
  fuel rather than released at that stage. This is not equivalent to permanent
  geological storage: subsequent fuel combustion must remain in the accounting.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-synthetic.inc
