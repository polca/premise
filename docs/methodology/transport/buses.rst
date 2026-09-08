Buses
========


.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``buses`` update regionalizes mapped buses inventories and, when
fleet data exist, builds regional transport markets. The generated service is
expressed per person-kilometre; vehicle manufacturing and battery capacity have
separate reference units.

.. figure:: /_static/process-diagrams/transport-buses.svg
   :class: process-diagram
   :alt: Buses transformation: Regionalize mapped transport activities; Apply available efficiency factors; If fleet data exist: build service markets; Retain consumer selection.
   :align: center

Inputs and applicability
--------------------------

The source inventories below provide vehicle variants. ``bus_fleet``
controls fleet-market availability; ``bus_efficiencies`` provides mapped
energy-efficiency changes. Both depend on the selected IAM and scenario data.
Use ``ndb.update("buses")`` after energy-supply updates, or run the default
sequence. Import availability does not establish a future fleet share.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("buses")

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
person-kilometre input becomes 0.1/1.1 = 0.0909 kWh. Inspect the changed fuel
or electricity exchange and its supplier separately. For an available fleet
market, check service weights and units, then inspect an actual consumer link.
Use ``Market Changes`` and ``Key Changes`` to distinguish composition,
energy use and any exhaust-factor corrections.

Sources and inventory details
-------------------------------

Source inventories and coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following datasets for city and coach buses are imported.

 =================================================================================================================== ==================
  Bus datasets                                                                                                        location
 =================================================================================================================== ==================
  transport, passenger bus, battery electric - overnight charging 9m midibus                                          all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 9m midibus                          all IAM regions
  transport, passenger bus, fuel cell electric, 9m midibus                                                            all IAM regions
  transport, passenger bus, diesel hybrid, 9m midibus, EURO-VI                                                        all IAM regions
  transport, passenger bus, diesel, 9m midibus, EURO-VI                                                               all IAM regions
  transport, passenger bus, compressed gas, 9m midibus, EURO-VI                                                       all IAM regions
  transport, passenger bus, battery electric - overnight charging 13m single deck urban bus                           all IAM regions
  transport, passenger bus, battery electric - battery-equipped trolleybus, LTO battery, 13m single deck urban bus    all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 13m single deck urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 13m single deck urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m single deck urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m single deck urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m single deck urban bus, EURO-VI                                        all IAM regions
  transport, passenger bus, fuel cell electric, 13m single deck coach bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m single deck coach bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m single deck coach bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m single deck coach bus, EURO-VI                                        all IAM regions
  transport, passenger bus, battery electric - overnight charging 13m double deck urban bus                           all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 13m double deck urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 13m double deck urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m double deck urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m double deck urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m double deck urban bus, EURO-VI                                        all IAM regions
  transport, passenger bus, fuel cell electric, 13m double deck coach bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m double deck coach bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m double deck coach bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m double deck coach bus, EURO-VI                                        all IAM regions
  transport, passenger bus, battery electric - overnight charging 18m articulated urban bus                           all IAM regions
  transport, passenger bus, battery electric - battery-equipped trolleybus, LTO battery, 18m articulated urban bus    all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 18m articulated urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 18m articulated urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 18m articulated urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 18m articulated urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 18m articulated urban bus, EURO-VI                                        all IAM regions
 =================================================================================================================== ==================

Inventories are from `Sacchi <https://zenodo.org/records/5720779>`__ et al. 2021. The vehicles are available
for different years and emission standards and for each IAM region.

*premise* creates fleet markets where the required scenario data are available. The inventories can be consulted
here: `LCIbuses <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-buses.xlsx>`__.

This mode has no configured existing consumer-replacement rules. Its
presence in the inventory does not establish downstream use.
