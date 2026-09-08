Regionalization of end-use heating datasets
=============================================

.. raw:: html

   <span id="final-energy-adjustments"></span>

.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``final energy`` update regionalizes mapped end-use heating activities
and their suppliers. It does not recalculate every final-energy demand in the
economy. Each activity retains its reference product and unit.

.. figure:: /_static/process-diagrams/final-energy.svg
   :class: process-diagram
   :alt: End-use heating regionalization: Select mapped heating activities; Create regional variants; Relink suppliers; Retain the activity reference unit.
   :align: center

Inputs and applicability
--------------------------

The update requires ``final_energy_use`` scenario data; otherwise it is
skipped. Activity selectors come from the final-energy mapping. Select it
with ``ndb.update("final energy")`` or use the default sequence.

Transformation
----------------

Mapped heating activities pass through the common regionalization routine
with IAM production volumes, followed by supplier relinking. This operation
has no general operation for changing all end-use efficiencies.

Markets and downstream links
------------------------------

The :doc:`heat` update separately constructs delivered-heat markets and
redirects their consumers. A regionalized heating activity is not evidence
that its heat-market composition or every consuming industry's energy demand
has changed.

Assumptions and limitations
-----------------------------

Coverage follows mapped activities and available final-energy data.
Regional geography alone does not make an existing heating technology
locally representative.

Worked example and checks
---------------------------

Compare a mapped heating activity's source and regional versions. Inspect
its location and energy suppliers separately from its direct energy amounts.
Then inspect the heat-market route used by a consuming activity. If the data
are absent, confirm the recorded skip rather than expecting new markets.

Sources and inventory details
-------------------------------

The implementation is ``premise/final_energy.py``; activity selection uses
``InventorySet.generate_final_energy_map``. See :doc:`/reference/iam-variables`
for mapping conventions and :doc:`/user_guide/updates` for execution order.
