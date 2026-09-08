Two-wheelers
==============


.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``two_wheelers`` update regionalizes mapped two-wheelers inventories and, when
fleet data exist, builds regional transport markets. The generated service is
expressed per kilometre; vehicle manufacturing and battery capacity have
separate reference units.

.. figure:: /_static/process-diagrams/transport-two-wheelers.svg
   :class: process-diagram
   :alt: Two-wheelers transformation: Regionalize mapped transport activities; Apply available efficiency factors; If fleet data exist: build service markets; Retain consumer selection.
   :align: center

Inputs and applicability
--------------------------

The source inventories below provide vehicle variants. ``two_wheelers_fleet``
controls fleet-market availability; ``two_wheelers_efficiencies`` provides mapped
energy-efficiency changes. Both depend on the selected IAM and scenario data.
Use ``ndb.update("two_wheelers")`` after energy-supply updates, or run the default
sequence. Import availability does not establish a future fleet share.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("two_wheelers")

Transformation
----------------

When an efficiency factor is applied, **all biosphere exchanges** in the
selected transport activity are rescaled along with the filtered energy
inputs. Other technosphere exchanges, such as vehicle manufacture and road
infrastructure, are not selected by this efficiency operation. This
biosphere scaling precedes the additional car/truck exhaust adjustment.

Premise regionalizes mapped transport activities even when fleet data are
absent. For an available efficiency signal, it applies the inverse efficiency
change to technosphere inputs selected by the technology's fuel-name filters.
If the efficiency array or mapped variable is missing, that energy adjustment
is unchanged. Regional markets are built only when fleet data are available.


Markets and downstream links
------------------------------

Regional fleet markets use mapped transport production volumes and the
common system-model weighting. This mode has no ``old`` consumer-replacement
rules in ``vehicles_map.yaml``. Creating its market does not automatically
redirect ordinary database consumers to it. A study using this transport
service must establish which activity it actually demands.

Assumptions and limitations
-----------------------------

Inventory driving conditions, vehicle sizes and powertrains constrain
coverage. A missing fleet is not a zero fleet, and a regional proxy is not a
new vehicle simulation. Fleet weights describe transport service, not an
unqualified count of vehicles. Do not equate vehicle-kilometres with person-kilometres without occupancy information.

Worked example and checks
---------------------------

For an illustrative 10% efficiency improvement, a selected 0.1 kWh per
kilometre input becomes 0.1/1.1 = 0.0909 kWh. Inspect the changed fuel
or electricity exchange and its supplier separately. For an available fleet
market, check service weights and units, then inspect an actual consumer link.
Use ``Market Changes`` and ``Key Changes`` to distinguish composition,
energy use and any exhaust-factor corrections.

Sources and inventory details
-------------------------------

Source inventories and coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following datasets for two-wheelers are imported.
Inventories are from `Sacchi <https://zenodo.org/records/5720779>`__ et al. 2022. The vehicles are available
for different years and emission standards. *premise* will only
import vehicles which production year is equal or inferior to
the scenario year considered. The inventories can be consulted
here: `LCItwowheelers <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-two_wheelers.xlsx>`__.


 ================================================= ==================
  Two-wheeler datasets                              location
 ================================================= ==================
  transport, Kick Scooter, electric, <1kW           all IAM regions
  transport, Bicycle, conventional, urban           all IAM regions
  transport, Bicycle, electric (<25 km/h)           all IAM regions
  transport, Bicycle, electric (<45 km/h)           all IAM regions
  transport, Bicycle, electric, cargo bike          all IAM regions
  transport, Moped, gasoline, <4kW, EURO-5          all IAM regions
  transport, Scooter, gasoline, <4kW, EURO-5        all IAM regions
  transport, Scooter, gasoline, 4-11kW, EURO-5      all IAM regions
  transport, Scooter, electric, <4kW                all IAM regions
  transport, Scooter, electric, 4-11kW              all IAM regions
  transport, Motorbike, gasoline, 4-11kW, EURO-5    all IAM regions
  transport, Motorbike, gasoline, 11-35kW, EURO-5   all IAM regions
  transport, Motorbike, gasoline, >35kW, EURO-5     all IAM regions
  transport, Motorbike, electric, <4kW              all IAM regions
  transport, Motorbike, electric, 4-11kW            all IAM regions
  transport, Motorbike, electric, 11-35kW           all IAM regions
  transport, Motorbike, electric, >35kW             all IAM regions
 ================================================= ==================

This mode has no configured existing consumer-replacement rules; downstream
use must be checked in the study supply chain.
