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

Consider a hypothetical electric car that purchases **30,000 kWh of grid
electricity over its life**. It uses a 60 kWh pack with one full replacement,
so lifetime battery purchases are **120 kWh of capacity**. Its purchased
traction-energy denominator is ``30,000 × 3.6 = 108,000 MJ``.

.. list-table:: Inputs per 1 MJ of purchased traction energy
   :header-rows: 1
   :widths: 30 30 40

   * - Input
     - Lifetime purchase
     - Input per MJ
   * - Grid electricity
     - 30,000 kWh
     - 30,000 / 108,000 = 0.277778 kWh
   * - Battery capacity
     - 120 kWh of capacity
     - 120 / 108,000 = 0.00111111 kWh of capacity

The electricity coefficient equals ``1 / 3.6``. The battery coefficient
already includes the replacement; multiplying it by two again would count the
replacement twice. Equivalent source amounts per vehicle-kilometre give the
same ratio when both use the same lifetime and occupancy basis.

This example covers the retained traction-energy and battery inputs. Chassis,
body, roads and whole-vehicle maintenance stay outside this energy-use
activity. It is not the footprint of a complete transport service.

For heating, compare a mapped source activity with its regional copy: check
its location, unchanged or changed direct amounts, and relinked suppliers
separately. Confirm which heat market a consumer uses. Without the required
final-energy data, expect the recorded skip rather than new regional activities.

Sources and inventory details
-------------------------------

The implementation is ``premise/energy/final_energy.py``; activity selection uses
``InventorySet.generate_final_energy_map``. Transport equipment selection and
normalization are implemented in ``premise/transport/vehicle_energy_use.py``. See
:doc:`/reference/iam-variables`
for mapping conventions and :doc:`/user_guide/updates` for execution order.
