Shipping
==========

.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``ships`` update regionalizes mapped freight inventories and creates
regional markets per tonne-kilometre when fleet data are available. Vehicle
manufacturing and transport service are distinct activities. The generic
transport workflow is described in :doc:`overview`.

.. figure:: /_static/process-diagrams/transport-shipping.svg
   :class: process-diagram
   :alt: Shipping transformation: Regionalize mapped transport activities; Apply available efficiency factors; If fleet data exist: build service markets; Redirect eligible freight consumers.
   :align: center

Inputs and applicability
--------------------------

Inputs are the source database, ``lci-ships.xlsx``, and the model aliases
in ``transport_sea_freight.yaml``. The update uses ``sea_freight_fleet`` to determine
fleet-market availability and ``sea_freight_efficiencies`` for energy changes.
``ndb.update("ships")`` uses the upstream suppliers available at that point;
run the default sequence for prospective electricity and fuels.

Transformation
----------------

When an efficiency factor is applied, **all biosphere exchanges** in the
selected transport activity are rescaled along with the filtered energy
inputs. Other technosphere exchanges, such as vehicle manufacture and road
infrastructure, are not selected by this efficiency operation. This
biosphere scaling precedes the additional car/truck exhaust adjustment.

Mapped activities are regionalized even if fleet data are absent. With a
mapped efficiency signal, fuel/energy exchanges selected by technology filters
receive the inverse efficiency change. A missing efficiency array or variable
leaves that adjustment unchanged. Fleet markets are created only when the
mode's fleet data exist; technology shares use mapped production volumes.

Markets and downstream links
------------------------------

The generated market family is ``market for transport, freight, sea, container ship``.
Existing consumer replacement uses the exact ``old`` mappings in
``premise/data/transport/vehicles_map.yaml`` and the tonne-kilometre exchange
unit. If the target market cannot be resolved, the original input remains.
The rewrite skips consuming datasets whose unit already contains
``kilometer``. Inspect actual consumer links rather than assuming every
freight-related activity was redirected.

Assumptions and limitations
-----------------------------

The mapped service is container-ship freight, not a complete representation
of all marine vessels and cargos. IAM mappings can aggregate domestic and
international shipping differently; inspect the selected aliases before
comparing regional results. Source load factors, vessel characteristics and
infrastructure are not re-simulated by regionalization.

Worked example and checks
---------------------------

For an illustrative 20% efficiency increase, a mapped 0.03 MJ/tkm input
becomes 0.03/1.2 = 0.025 MJ/tkm. Inspect that direct exchange and its fuel
supplier independently. Check that fleet supplier shares close, that the
reference unit is tonne-kilometres, and that a chosen existing consumer reaches
the intended regional market. A missing fleet should not be interpreted as
zero freight demand.

Sources and inventory details
-------------------------------

The constructor selects ``lci-ships.xlsx`` as documented in
:doc:`/reference/inventories`. The current mapping defines the following
technology categories; these are selectors, not a guarantee of nonzero
scenario deployment:

* ``ship, marine oil``
* ``ship, diesel``
* ``ship, biodiesel``
* ``ship, electric``
* ``ship, fuel cell``
* ``ship, ammonia``
* ``ship, methanol``
* ``ship, synthetic methane``
* ``ship, synthetic LPG``

Source metadata should be read in the selected workbook.
