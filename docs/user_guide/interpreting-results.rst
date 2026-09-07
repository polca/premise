Interpreting changes and uncertainty
======================================

A changed GWP score can reflect a changed technology, its supply mix, its
operating performance or its upstream background. Inspect these separately
before concluding that a change is an error or an improvement.

Start with a comparable functional unit
-----------------------------------------

Match activity name, reference product, unit and geography. Compare the same
LCIA method and system model. Distinguish electricity at a plant from delivered
electricity with storage and distribution losses. Confirm that zero means a
measured/modelled zero rather than a missing match.

A historical PV comparison
----------------------------

The archived REMIND SSP2-NPi 2025 comparison used ecoinvent 3.12 cut-off.
Across matched residential country datasets, the median score changed from
26.25 to 13.10 g CO2-eq/kWh. These are historical regression results, not a
newly executed calculation or an external benchmark for every PV system.

The comparison separated a material-update interaction from the intended PV
inventory change. After correction, module efficiency, installation burdens,
manufacturing mixes and effective yields contributed to the remaining
differences. A lower score alone cannot prove that module efficiency explains
the entire change.

Download the :download:`recorded comparison summary </examples/pv-comparison-summary.json>`
and :download:`run metadata </examples/pv-comparison-metadata.json>`.
The source revision and fix-patch checksum identify the historical calculation;
they are not a claim that the current checkout reproduces it exactly.

Trace a change
----------------

1. Inspect changed supplier shares in the market.
2. Compare energy and material amounts per reference output.
3. Check yield, lifetime and efficiency assumptions in the technology dataset.
4. Inspect contributions from the suppliers before and after the change.
5. Retain a residual for effects not explained by the selected contributions.

Read :doc:`reports` for the workbook and Parquet views. Contributions already
include upstream supply chains: summing a parent contribution and its child
contributions counts the same burden twice.

An executable sensitivity example
-----------------------------------

The following arithmetic uses illustrative inputs. It tests the direction and
size of efficiency/yield changes while holding other assumptions fixed; it
does not calculate an LCA or replace the archived PV scores.

.. literalinclude:: /examples/interpret_changes.py
   :language: python

Expected results are 9.000 to 8.182 MJ fuel/kWh for the efficiency example,
and 33.33 versus 25.64 g CO2-eq/kWh for the two illustrative PV yields.

Three kinds of uncertainty
----------------------------

.. list-table::
   :header-rows: 1

   * - Kind
     - Example
     - Suitable analysis
   * - Exchange uncertainty
     - Distribution on a material input
     - Retain supported distributions and sample them in an LCA calculation.
   * - Scenario uncertainty
     - Different pathways, years or IAMs
     - Compare separately built scenarios with consistent functional units.
   * - Structural uncertainty
     - Proxy geography, allocation or deployment assumptions
     - Rebuild explicit alternatives and explain the changed boundary.

An uncertainty flag does not create distributions for IAM assumptions. Scaling
an exchange with a retained distribution is not equivalent to modelling
uncertainty in the scaling factor. Report scenario ranges as scenario ranges,
not statistical confidence intervals.
