Transport overview
====================

.. raw:: html

   <span id="road-vehicles"></span>


.. raw:: html

   <span id="source-inventories-road-vehicles"></span>


.. raw:: html

   <span id="key-outputs"></span>


.. raw:: html

   <span id="transport"></span>


The transport transformation combines mode-specific vehicle inventories with
IAM fleet data where available. It can create regional fleet-average transport
datasets and update their energy supply. Vehicle inventories and electricity
or fuel markets remain separate parts of the supply chain.

Inputs and coverage
---------------------

Road-mode sources are documented in :doc:`two-wheelers`, :doc:`passenger-cars`,
:doc:`trucks` and :doc:`buses`. Fleet composition and availability depend on the
IAM model, scenario, year and mapped activities; a mode's presence in the update
API does not guarantee complete fleet data in every scenario.

Using the transport updates
-----------------------------

After the setup in :doc:`/getting_started/first-scenario`, individual modes can
be selected as follows:

.. code-block:: python

   ndb.update(["two_wheelers", "cars", "trucks", "buses", "trains", "ships"])

These calls use the suppliers already available in the scenario. Run all
default updates when electricity and fuel suppliers must also be prospective;
see :doc:`/user_guide/updates` for dependency order.

Common transformation and outputs
-----------------------------------

Current vehicle inventories provide the starting point for prospective
vehicles. Mapped efficiency changes adjust energy use, while fleet data can
combine powertrains and vehicle classes into regional transport services.
The truck page describes its size classes, haul types and consumer relinking.
Generated car and two-wheeler markets use kilometres, bus markets use
person-kilometres, and freight markets use tonne-kilometres. Fleet weights
must be interpreted in those units. The configured existing-consumer rewrite
exists for trucks, rail and shipping; cars, buses and two-wheelers have no
corresponding replacement map.

Rail and shipping
-------------------

The ``trains`` update consumes ``rail_freight_fleet`` data and ``ships`` consumes
``sea_freight_fleet`` in ``premise/transport.py``. When fleet data are missing,
the update marks that absence rather than treating it as a measured zero fleet.
The implementation can still process available vehicle inventories; regional
fleet-average creation depends on the available data and mapping.

Their mapping files are ``transport_rail_freight.yaml`` and
``transport_sea_freight.yaml``. See :doc:`rail` and :doc:`shipping` for their scope, mapped technologies,
consumer replacement rules and checks.

Assumptions and limitations
-----------------------------

The IAM does not describe every vehicle size, powertrain or driving cycle.
Available inventory variants and their restrictions are listed on the mode
pages. Battery production is described in :doc:`/methodology/batteries`;
electricity supply and fuel markets have their own losses and boundaries.

.. raw:: html

   <span id="two-wheelers"></span>


.. raw:: html

   <span id="passenger-cars"></span>


.. raw:: html

   <span id="medium-and-heavy-duty-trucks"></span>


.. raw:: html

   <span id="buses"></span>


Source provenance and currency
--------------------------------

.. include:: /reference/generated/source-transport.inc
