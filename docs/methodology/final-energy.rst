Final-energy heating and transport activities
===============================================

.. raw:: html

   <span id="final-energy-adjustments"></span>
   <span id="regionalization-of-end-use-heating-datasets"></span>

.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``final energy`` update regionalizes mapped end-use heating activities
and their suppliers, and rebuilds marked transport energy-use activities from
the scenario's vehicle inventories. These transport activities supply one MJ
of purchased traction energy, including energy-specific equipment. Each
activity retains its reference product and unit.

.. figure:: /_static/process-diagrams/final-energy.svg
   :class: process-diagram
   :alt: Final-energy activities: Check final-energy data; Regionalize mapped heating; Rebuild marked vehicle energy-use activities per MJ; Relink suppliers.
   :align: center

Inputs and applicability
--------------------------

The update requires ``final_energy_use`` scenario data; otherwise it is
skipped. Activity selectors come from the final-energy mapping. Select it
with ``ndb.update("final energy")`` or use the default sequence.

Transport activities must carry ``energy use source`` metadata and match an
explicit representative vehicle class. Run the relevant transport, battery
and energy-supply updates first; the default sequence supplies this ordering.
Activities without this metadata retain their existing recipes.

Transformation
----------------

Mapped heating activities pass through the common regionalization routine
with IAM production volumes. Marked transport activities are then rebuilt
from their corresponding regional vehicle service and production inventories,
followed by supplier relinking.

For transport, the denominator is the final purchased traction energy per
vehicle service, converted to MJ using the delivered fuel's heating value or
3.6 MJ/kWh. Energy-specific equipment, battery capacity and direct emissions
are divided by this denominator. Vehicle production links already carry
lifetime and occupancy/payload allocation, so those factors are applied once.
Electricity use is 1/3.6 kWh per MJ; hydrogen compression is an additional
auxiliary input rather than part of the hydrogen-energy denominator.

The retained equipment boundary excludes chassis, body, roads and whole-vehicle
maintenance. Battery purchases follow the scenario vehicle's sizing and
replacement policy. The later emissions update refreshes marked activities'
direct emissions from their source services, including when GAINS data are
absent. This update does not recalculate every final-energy demand in the economy.

Markets and downstream links
------------------------------

The :doc:`heat` update separately constructs delivered-heat markets and
redirects their consumers. A regionalized heating activity is not evidence
that its heat-market composition or every consuming industry's energy demand
has changed.

Transport activities retain their original reference identities and link to
the available prospective energy and equipment suppliers. Their one-MJ output
represents energy use with scoped equipment, rather than a complete
vehicle-kilometre or transport-service footprint.

Assumptions and limitations
-----------------------------

Coverage follows mapped activities and available final-energy data.
Regional geography alone does not make an existing heating technology
locally representative.

Transport classes are explicit representative proxies. Inspect the retained
``energy use source`` metadata for the selected service, carrier, boundary and
normalization. Alternative fuels retain supplier mass shares while their total
purchase is normalized to one MJ. Source powertrain proxies remain relevant to
exhaust factors; matching energy alone does not establish combustion equivalence.

Worked example and checks
---------------------------

Compare a mapped heating activity's source and regional versions. Inspect
its location and energy suppliers separately from its direct energy amounts.
Then inspect the heat-market route used by a consuming activity. If the data
are absent, confirm the recorded skip rather than expecting new markets.

For an electric passenger-car activity, verify 1/3.6 kWh per MJ and compare the
battery-capacity input with the source car's lifetime capacity purchases divided
by its lifetime purchased electricity in MJ. Check the source location and
equipment boundary before comparing it with a complete transport service.

Sources and inventory details
-------------------------------

The implementation is ``premise/final_energy.py``; activity selection uses
``InventorySet.generate_final_energy_map``. Transport equipment selection and
normalization are implemented in ``premise/vehicle_energy_use.py``. See
:doc:`/reference/iam-variables`
for mapping conventions and :doc:`/user_guide/updates` for execution order.
