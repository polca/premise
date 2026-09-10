Passenger cars
================


.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``cars`` update regionalizes mapped passenger cars inventories and, when
fleet data exist, builds regional transport markets. The generated service is
expressed per kilometre; vehicle manufacturing and battery capacity have
separate reference units.

.. figure:: /_static/process-diagrams/transport-passenger-cars.svg
   :class: process-diagram
   :alt: Cars transformation: Regionalize mapped transport activities; Apply available efficiency factors; If fleet data exist: build service markets; Retain consumer selection; Apply matched exhaust factors.
   :align: center

Inputs and applicability
--------------------------

The source inventories below provide vehicle variants. ``passenger_car_fleet``
controls fleet-market availability; ``passenger_car_efficiencies`` provides mapped
energy-efficiency changes. Both depend on the selected IAM and scenario data.
Use ``ndb.update("cars")`` after energy-supply updates, or run the default
sequence. Import availability does not establish a future fleet share.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("cars")

Transformation
----------------

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


Provisional combustion-car energy floor
-----------------------------------------

Mapped conventional diesel, gasoline and compressed-gas cars with an available
IAM efficiency signal receive a configurable minimum fuel-energy demand after
the IAM adjustment and before exhaust normalization. Battery-electric, fuel-cell,
plug-in hybrid and non-car transport datasets are outside this floor's scope.
Missing efficiency data do not trigger a source-inventory correction.

The configuration is in ``data/transport/car_energy_floor.yaml``. Its default
is **0.852 MJ LHV per vehicle-kilometre**, inherited from the previous validator's
2 kg diesel-equivalent/100 km and 42.6 MJ/kg convention. This is a provisional
compatibility guardrail, **not a demonstrated physical minimum**. Powertrain and
size overrides and an ``enabled`` switch are supported. Configuration and fuel
properties are cached per process; restart after changing them.

For projected energy demand ``E`` and minimum ``E_min``, the final demand is
``max(E, E_min)``. When binding, fuel inputs are multiplied by ``E_min/E``.
Their proportions and existing fossil/non-fossil CO2 shares are preserved;
vehicle manufacture, road infrastructure and non-exhaust burdens are not
scaled by this additional correction. Ambiguous particulate flows are not
assumed to be exhaust. The existing subsequent exhaust-factor normalization
remains in place. The older IAM adjustment's all-biosphere scaling convention
is unchanged by this additional floor.

Fuel selection explicitly recognizes petrol as well as gasoline markets,
including market groups. This fixes cases where IAM scaling previously changed
emissions but missed petrol inputs. The same old alias could wrongly select
``Passenger car, gasoline, ...`` manufacture inputs; these are now left at
their source recipe amounts instead of following fuel-efficiency changes.
Gas is supported in kg (47.5 MJ/kg, existing
transport convention) or m3 (packaged fuel properties); liquid fuels use their
packaged MJ/kg values. Unsupported units, invalid reference production, missing
supported fuel inputs and non-finite or negative quantities fail explicitly.

The car CO2 validator now runs independently of the efficiency-range check.
It compares total direct fossil plus biogenic CO2 with fuel-specific packaged
complete-combustion factors, retaining a 10% tolerance. It no longer applies a
diesel carbon factor to energy-equivalent natural gas. Generic fuel-market
labels do not establish blend composition, so this check does **not** validate
the fossil/biogenic split or resolve synthetic-fuel carbon attribution.
Validation is read-only and does not force emissions to match its expectation.

Each binding floor is recorded in ``log parameters / car energy floor`` and
transport provenance: projected and final energy demand, threshold, correction,
affected exchanges, year, region, pathway and original IAM scaling. Such a car
no longer exactly reproduces the unconstrained IAM efficiency; demand and fleet
shares are not changed to compensate. Reapplying the floor alone is idempotent.

Runtime acceptance
~~~~~~~~~~~~~~~~~~~~

The project acceptance limit is a maximum 0.1% increase in complete scenario-build
runtime. Fuel lookups are cached, and the implementation adds no database-wide
scan. Acceptance requires repeated interleaved baseline/patched builds with the
same inputs, cache policy, export mode and warning handling. A one-sided 95%
upper confidence bound on the paired runtime ratio must not exceed 1.001.
An inconclusive benchmark is not a pass; microbenchmarks alone cannot establish
compliance with the whole-build limit.

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

The following datasets for passenger cars are imported.

 =============================================================================== ==================
  Passenger car datasets                                                          location
 =============================================================================== ==================
  transport, passenger car, gasoline, Large                                       all IAM regions
  transport, passenger car, diesel, Large                                         all IAM regions
  transport, passenger car, compressed gas, Large                                 all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large                         all IAM regions
  transport, passenger car, plugin diesel hybrid, Large                           all IAM regions
  transport, passenger car, fuel cell electric, Large                             all IAM regions
  transport, passenger car, battery electric Large                                all IAM regions
  transport, passenger car, gasoline hybrid, Large                                all IAM regions
  transport, passenger car, diesel hybrid, Large                                  all IAM regions
  transport, passenger car, gasoline, Large SUV                                   all IAM regions
  transport, passenger car, diesel, Large SUV                                     all IAM regions
  transport, passenger car, compressed gas, Large SUV                             all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large SUV                     all IAM regions
  transport, passenger car, plugin diesel hybrid, Large SUV                       all IAM regions
  transport, passenger car, fuel cell electric, Large SUV                         all IAM regions
  transport, passenger car, battery electric Large SUV                            all IAM regions
  transport, passenger car, gasoline hybrid, Large SUV                            all IAM regions
  transport, passenger car, diesel hybrid, Large SUV                              all IAM regions
  transport, passenger car, gasoline, Lower medium                                all IAM regions
  transport, passenger car, diesel, Lower medium                                  all IAM regions
  transport, passenger car, compressed gas, Lower medium                          all IAM regions
  transport, passenger car, plugin gasoline hybrid, Lower medium                  all IAM regions
  transport, passenger car, plugin diesel hybrid, Lower medium                    all IAM regions
  transport, passenger car, fuel cell electric, Lower medium                      all IAM regions
  transport, passenger car, battery electric Lower medium                         all IAM regions
  transport, passenger car, gasoline hybrid, Lower medium                         all IAM regions
  transport, passenger car, diesel hybrid, Lower medium                           all IAM regions
  transport, passenger car, gasoline, Medium                                      all IAM regions
  transport, passenger car, diesel, Medium                                        all IAM regions
  transport, passenger car, compressed gas, Medium                                all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium                        all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium                          all IAM regions
  transport, passenger car, fuel cell electric, Medium                            all IAM regions
  transport, passenger car, battery electric Medium                               all IAM regions
  transport, passenger car, gasoline hybrid, Medium                               all IAM regions
  transport, passenger car, diesel hybrid, Medium                                 all IAM regions
  transport, passenger car, gasoline, Medium SUV                                  all IAM regions
  transport, passenger car, diesel, Medium SUV                                    all IAM regions
  transport, passenger car, compressed gas, Medium SUV                            all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium SUV                    all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium SUV                      all IAM regions
  transport, passenger car, fuel cell electric, Medium SUV                        all IAM regions
  transport, passenger car, battery electric Medium SUV                           all IAM regions
  transport, passenger car, gasoline hybrid, Medium SUV                           all IAM regions
  transport, passenger car, diesel hybrid, Medium SUV                             all IAM regions
  transport, passenger car, battery electric Micro                                all IAM regions
  transport, passenger car, gasoline, Mini                                        all IAM regions
  transport, passenger car, diesel, Mini                                          all IAM regions
  transport, passenger car, compressed gas, Mini                                  all IAM regions
  transport, passenger car, plugin gasoline hybrid, Mini                          all IAM regions
  transport, passenger car, plugin diesel hybrid, Mini                            all IAM regions
  transport, passenger car, fuel cell electric, Mini                              all IAM regions
  transport, passenger car, battery electric Mini                                 all IAM regions
  transport, passenger car, gasoline hybrid, Mini                                 all IAM regions
  transport, passenger car, diesel hybrid, Mini                                   all IAM regions
  transport, passenger car, gasoline, Small                                       all IAM regions
  transport, passenger car, diesel, Small                                         all IAM regions
  transport, passenger car, compressed gas, Small                                 all IAM regions
  transport, passenger car, plugin gasoline hybrid, Small                         all IAM regions
  transport, passenger car, plugin diesel hybrid, Small                           all IAM regions
  transport, passenger car, fuel cell electric, Small                             all IAM regions
  transport, passenger car, battery electric Small                                all IAM regions
  transport, passenger car, gasoline hybrid, Small                                all IAM regions
  transport, passenger car, diesel hybrid, Small                                  all IAM regions
  transport, passenger car, gasoline, Van                                         all IAM regions
  transport, passenger car, diesel, Van                                           all IAM regions
  transport, passenger car, compressed gas, Van                                   all IAM regions
  transport, passenger car, plugin diesel hybrid, Van                             all IAM regions
  transport, passenger car, fuel cell electric, Van                               all IAM regions
  transport, passenger car, battery electric Van                                  all IAM regions
  transport, passenger car, gasoline hybrid, Van                                  all IAM regions
  transport, passenger car, diesel hybrid, Van                                    all IAM regions
 =============================================================================== ==================

Inventories are from `Sacchi2 <https://www.psi.ch/en/media/72391/download>`__ et al. 2022. The vehicles are available
for different years and emission standards and for each IAM region.


NOx and PM 2.5 emissions are further updated based on
remote-sensing data from `Sjodin <https://www.bafu.admin.ch/dam/en/sd-web/lggkMIGXeKNu/real-driving-emissions-from-diesel-passenger-cars-measured-by-remote-sensing-and-as-compared-with-pems-and-chassis-dynamometer-measurements-conox-task-2-report.pdf>`__ et al. 2018.

*premise* creates fleet markets where the required scenario data are available. The inventories can be consulted
here: `LCIpasscars <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-pass_cars.xlsx>`__.

At the moment, these inventories do not supply inputs to other activities in the LCI database.
