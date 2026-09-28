Trucks
========


.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``trucks`` update regionalizes mapped trucks inventories and, when
fleet data exist, builds regional transport markets. The generated service is
expressed per tonne-kilometre; vehicle manufacturing and battery capacity have
separate reference units.

.. figure:: /_static/process-diagrams/transport-trucks.svg
   :class: process-diagram
   :alt: Trucks transformation: Regionalize mapped transport activities; Apply available efficiency factors; If fleet data exist: build markets and set batteries; Redirect eligible freight consumers; Apply matched exhaust factors.
   :align: center

Inputs and applicability
--------------------------

The source inventories below provide vehicle variants. ``road_freight_fleet``
controls fleet-market availability; ``road_freight_efficiencies`` provides mapped
energy-efficiency changes. Both depend on the selected IAM and scenario data.
Use ``ndb.update("trucks")`` after energy-supply updates, or run the default
sequence. Import availability does not establish a future fleet share.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("trucks")

See :doc:`overview` for transport coverage and energy-supply dependencies.

Transformation
----------------

When fleet data are available, the truck-market routine also adjusts battery
capacity in matching battery-electric truck manufacturing activities. It sets
the selected battery exchanges to the size-specific projected capacity, with
triangular bounds. Capacity is interpolated between configured years and held
at endpoint values outside the range; it is not simply scaled by vehicle
fuel efficiency.

For matched combustion powertrains and emissions classes, the exhaust step
compares air-pollutant totals with packaged factors and calculated fuel use.
A zero total or a value within 50% of the expected total is left unchanged.
A larger deviation is clamped to the 90–110% interval around that expectation.
This can modify pollutant totals independently of the IAM efficiency factor.

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
Cars and trucks also receive the implemented exhaust-factor
normalization for matched fuel, size and emissions-class cases. This is
separate from IAM efficiency changes and the GAINS emissions update.


Markets and downstream links
------------------------------

Truck markets combine mapped transport production volumes; the routine also
creates size-specific markets where corresponding production variables exist.
Existing freight inputs are redirected using the exact configured names and
tonne-kilometre unit. Unresolved target markets leave the original exchange
in place. The separate battery-size adjustment targets battery-electric truck
manufacturing activities.

Fleet average trucks
~~~~~~~~~~~~~~~~~~~~~~

Fleet composition coverage depends on the model aliases and the actual
scenario file; consult :doc:`/reference/coverage` and the scenario report.

The fleet data is expressed in "ton-kilometers" performed by each
type of vehicle for freight transport, in a given region and year.

*premise* uses the fleet data to produce fleet average trucks for each
IAM region, and more specifically:

* a fleet average truck, all powertrains and size classes considered
* a fleet average truck, all powertrains considered, for a given size class

The current market builder names its general market
``market for transport, freight, lorry`` and appends a size where supported.
The following inventory examples describe size and haul variants; their names
should not be treated as the exact output names of every current build:

 ========================================================================================= =============================================================
  truck transport dataset name                                                              description
 ========================================================================================= =============================================================
  transport, freight, lorry, 3.5t gross weight, unspecified powertrain, long haul           fleet average, for 3.5t size class, long haul
  transport, freight, lorry, 7.5t gross weight, unspecified powertrain, long haul           fleet average, for 7.5t size class, long haul
  transport, freight, lorry, 18t gross weight, unspecified powertrain, long haul            fleet average, for 18t size class, long haul
  transport, freight, lorry, 26t gross weight, unspecified powertrain, long haul            fleet average, for 26t size class, long haul
  transport, freight, lorry, 40t gross weight, unspecified powertrain, long haul            fleet average, for 40t size class, long haul
  transport, freight, lorry, unspecified, long haul                                         fleet average, all powertrain types, all size classes
 ========================================================================================= =============================================================

The mapping file linking IAM variables to the truck datasets is available
here: https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/iam_variables_mapping/transport_road_freight.yaml

Relinking
~~~~~~~~~~~

Regarding trucks, *premise* re-links truck transport-consuming activities
to the newly created fleet average truck datasets.

The following table shows the correspondence between the original
truck transport datasets and the new ones replacing them:

+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| Transport Type                                            | REMIND               | IMAGE                | TIAM-UCL             |
+===========================================================+======================+======================+======================+
| transport, freight, lorry 16-32 metric ton, EURO1         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO3       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, EURO5         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, EURO1           | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO4       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry, all sizes, EURO1 to market     | unspecified long haul| unspecified long haul| unspecified long haul|
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO6        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO1        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry, all sizes, EURO3 to market     | unspecified long haul| unspecified long haul| unspecified long haul|
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, EURO6         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO2        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO3        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO4        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, EURO2         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, EURO6           | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO2       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO1       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry, all sizes, EURO2 to market     | unspecified long haul| unspecified long haul| unspecified long haul|
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, unregulated   | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, unregulated     | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, EURO3           | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, unregulated | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO5        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO6       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, unregulated  | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspec. powertrain   | unspec. powertrain   | unspec. powertrain   |
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+

Assumptions and limitations
-----------------------------

Inventory driving conditions, vehicle sizes and powertrains constrain
coverage. A missing fleet is not a zero fleet, and a regional proxy is not a
new vehicle simulation. Fleet weights describe transport service, not an
unqualified count of vehicles. Load factors and haul assumptions matter when comparing tonne-kilometres.

Worked example and checks
---------------------------

For an illustrative 10% efficiency improvement, a selected 0.1 kWh per
tonne-kilometre input becomes 0.1/1.1 = 0.0909 kWh. Inspect the changed fuel
or electricity exchange and its supplier separately. For an available fleet
market, check service weights and units, then inspect an actual consumer link.
Use ``Market Changes`` and ``Key Changes`` to distinguish composition,
energy use and any exhaust-factor corrections.

Sources and inventory details
-------------------------------

Process context
~~~~~~~~~~~~~~~~~

The following size classes of medium and heavy duty trucks are imported:

- 3.5t
- 7.5t
- 18t
- 26t
- 40t

These weights refer to the vehicle gross mass (the maximum weight the vehicle is
allowed to reach, fully loaded).

Each truck is available for a variety of powertrain types:

- fuel cell electric
- battery electric
- diesel hybrid
- plugin diesel hybrid
- diesel
- compressed gas

but also for different driving cycles, to which a range autonomy
of the vehicle is associated:

- urban delivery (required range autonomy of 150 km)
- regional delivery (required range autonomy of 400 km)
- long haul (required range autonomy of 800 km)

Those are driving cycles developed for the software `VECTO <https://ec.europa.eu/clima/eu-action/transport-emissions/road-transport-reducing-co2-emissions-vehicles/vehicle-energy-consumption-calculation-tool-vecto_en>`__,
which have become standard in measuring the CO2 emissions of trucks.

The truck vehicle model is from `Sacchi <https://pubs.acs.org/doi/abs/10.1021/acs.est.0c07773>`__ et al, 2021.

Source inventories
~~~~~~~~~~~~~~~~~~~~

The following datasets for medium and heavy-duty trucks are imported.

 ================================================================================== ==================
  Truck datasets                                                                     location
 ================================================================================== ==================
  transport, freight, lorry, battery electric 3.5t gross weight                      all IAM regions
  transport, freight, lorry, fuel cell electric, 3.5t gross weight                   all IAM regions
  transport, freight, lorry, diesel hybrid, 3.5t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-VI                      all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-VI              all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 3.5t gross weight, EURO-VI        all IAM regions
  transport, freight, lorry, battery electric 7.5t gross weight                      all IAM regions
  transport, freight, lorry, fuel cell electric, 7.5t gross weight                   all IAM regions
  transport, freight, lorry, diesel hybrid, 7.5t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-VI                      all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-VI              all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 7.5t gross weight, EURO-VI        all IAM regions
  transport, freight, lorry, battery electric 18t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 18t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 18t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 18t gross weight, EURO-VI         all IAM regions
  transport, freight, lorry, battery electric 26t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 26t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 26t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 26t gross weight, EURO-VI         all IAM regions
  transport, freight, lorry, battery electric 32t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 32t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 32t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 32t gross weight, EURO-VI         all IAM regions
  transport, freight, lorry, battery electric 40t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 40t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 40t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 40t gross weight, EURO-VI         all IAM regions
 ================================================================================== ==================


Inventories are from `Sacchi3 <https://pubs.acs.org/doi/abs/10.1021/acs.est.0c07773>`__ et al. 2021. The vehicles are available
for different years and emission standards and for each IAM region.

When doing:

.. code-block:: python

    ndb.update("trucks")

*premise* will create fleet average vehicles for each IAM region. The inventories can be consulted
here: `LCItrucks <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-trucks.xlsx>`__.
