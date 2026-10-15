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

The current replacement workbook uses 2020 Regional delivery vehicles, with
400 km design range and cycle-specific payload and mileage. See
:doc:`/development/regional-truck-refresh-2026` for battery specifications,
proxy assumptions and the controlled EF 3.1 comparison. Historical long-haul
labels in the market examples below do not define this workbook's driving cycle.

.. figure:: /_static/process-diagrams/transport-trucks.svg
   :class: process-diagram
   :alt: Trucks transformation: Regionalize mapped transport activities; Apply available efficiency factors; If fleet data exist: build markets and set batteries; Redirect eligible freight consumers; Check emission plausibility without changing amounts.
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

The legacy selector uses manufacturing names containing ``truck``. The current
workbook uses ``lorry production`` names, which it does not match. Consequently,
these activities retain nominal capacity times one plus lifetime replacements;
all 42 production activities were checked in the 2020 build. The projected-size
mechanism should not be assumed to have overwritten their physical batteries.

Transport emission checks are read-only. Exact air-flow aliases and complete
EURO-class labels select the packaged hot-exhaust references; methane does not
include NMVOC, and EURO-VI trucks do not use EURO-V coefficients. Recognized
fuel markets and market groups provide fuel energy in MJ per reference activity.
Invalid matched emission masses, units and supported fuel inputs remain errors.
A missing fuel reference produces a diagnostic instead of a zero-fuel assumption.

For comparable named flows, the inherited 0.5–2 times hot-reference screen
produces warnings without changing inventory amounts or uncertainty metadata.
This is a heuristic, not an empirical bound or a EURO certification check.
Cold starts, evaporation, deterioration and model calibration are not
reconstructed from the final exchanges, so a disagreement alone does not prove
an inventory error. Residual NMVOC is not compared with parent NMHC, and missing
THC aggregates are reported as unavailable checks. Use the source model's
component inventory to investigate hot, cold-start and evaporation terms.

Optional ``energy_intensity_aliases`` provide technology-specific final-energy
intensities in MJ/vehicle-km. REMIND and REMIND-EU use these when available,
with an efficiency index equal to intensity(2020) divided by intensity(year).
A positive modeled 2020 intensity remains usable even when fleet activity is
zero. The original REMIND reports describe fleet intensities, not the physical
specification of a newly manufactured vehicle or motor efficiency alone.
The current default still caps the index at 1.25 (at most 20% less energy)
and prevents deterioration over time.

A direct series must have a finite positive 2020 value. Gaps in other years
can use the same series' nearest available values. If direct data are absent,
an available regional energy/service pair provides the fallback. Otherwise
the index is one, explicitly marked as unavailable in the array's
``vehicle efficiency sources`` attribute and the transformed vehicle's
``log parameters / IAM efficiency source``. Missing regions or classes never
inherit a direct trend from neighbouring technologies. In particular, EUR's
Large Car and SUV category is not automatically assigned to separate Large
Car inventories, nor duplicated in fleet volumes.

Other IAMs without direct-intensity mappings retain their existing
energy/service calculation. Its denominator matters: IMAGE reports passenger-
and tonne-kilometres, while the inspected TIAM-UCL distance series use
vehicle-kilometres. Such service intensities can incorporate occupancy/load
changes. Missing MESSAGE vehicle mappings do not create a synthetic vehicle
projection. Do not substitute pooled electricity or liquid-fuel demand for
individual technologies without accounting for hybrid contributions.

The original REMIND preprocessing must retain ``Energy intensity`` variables
in addition to ``Efficiency``. ``scripts/restore_vehicle_intensities.py`` can
append the mapped rows to an encrypted scenario from its matching original.
It verifies model/pathway and existing transport values, preserves the existing
plaintext bytes, and writes a new encrypted file plus hashes for review.
Existing scenario archives without these rows use the documented fallback.

Vehicle energy inputs are selected by their fuel/electricity product and unit,
in addition to the IAM fuel mapping. A name containing a fuel, such as lorry
production, diesel, does not make manufacture or equipment an energy input.
Efficiency improvements therefore do not reduce the number of vehicles
manufactured per unit of transport service.

Energy scaling changes fuel-derived CO2, sulphur emissions and generic-air
methane leakage. Compartmented HBEFA hot, cold-start and evaporative emissions
retain their source factors: NOx, exhaust PM, CO, NMVOC, formaldehyde and PAHs
are not reduced merely because an IAM uses less energy per kilometre.
Refrigerant losses, wear, vehicle manufacture and road infrastructure also
retain their own physical basis. Generic-air methane leakage is distinguished
from HBEFA combustion methane in urban/rural/highway air subcompartments.
These rules follow the refreshed carculator inventory equations; unknown flows
are not assigned a fuel relationship. They do not supply new future EURO-class
or fuel-blend-dependent non-CO2 emission factors.

The fuel module subsequently recomputes fossil/non-fossil combustion CO2 from
the final regional suppliers and fuel purchases. The emission plausibility
checks remain read-only.

Premise regionalizes mapped transport activities even when fleet data are
absent. For an available efficiency signal, it applies the inverse efficiency
change to technosphere inputs selected by the technology's fuel-name filters.
If the efficiency array or mapped variable is missing, that energy adjustment
is unchanged. Regional markets are built only when fleet data are available.
Cars and trucks receive read-only emission diagnostics for matched fuel, size
and emissions-class cases. These checks are separate from IAM efficiency
changes and the GAINS emissions update; they never normalize pollutant amounts.


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
For the current names and available size markets, inspect the generated
``market for transport, freight, lorry`` activities and
``iam_variables_mapping/transport_road_freight.yaml``. The regenerated vehicle
inputs use Regional delivery; old long-haul labels are not the current output
catalogue. Fleet averages remain conditional on available IAM service volumes.

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
energy use and supply changes. Review the validation report separately for
read-only emission diagnostics.

Sources and inventory details
-------------------------------

Process context and source inventories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The refreshed truck catalogue contains 52 RER services, 42 manufacturing recipes
and 33 supporting components. Gross-mass classes are 3.5, 7.5, 18, 26, 32, 40 and
60 tonnes. Native powertrains include diesel, compressed gas, diesel hybrid,
plug-in diesel hybrid, battery electric and fuel-cell electric. Ten gasoline/LPG
variants use explicit energy-preserving manufacture/combustion proxies.

All regenerated templates use the **Regional delivery** cycle, a **400 km**
baseline design range and 2020 defaults under Swiss climate. The source model
also supports Urban delivery and Long haul, but this workbook does not contain
three cycle variants. Combustion templates use EURO-VI; electric vehicles have
no EURO class. Transport is expressed per tonne-kilometre, using modelled cargo
rather than gross vehicle mass.

The current catalogue is in
`lci-trucks.xlsx <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-trucks.xlsx>`_
and its generated parameter snapshot is retained with the test data. Exact names are preserved from
the selected service catalogue. Only mapped technologies are regionalized;
unmatched 32 t/60 t services do not acquire fleet data simply because templates
exist. The separate legacy-component workbook preserves 26 compatibility
recipes for other vehicle families.

HEVs now have coupled high-power buffer sizing. FCEV trucks use the approved
AKASOL reference with charging/discharge checks; 3.5/7.5 t motors use documented
57/129 kW proxies. Premise purchases nominal kWh through its MIX capacity market,
with the physical manufacturing-mass mismatch recorded explicitly. Corrected
2020 FCEV hardware remains fixed in future scenarios. See
:doc:`campaign-closeout` for the source references, final checks and limitations.

.. include:: battery-sizing.inc

.. include:: lpg-fallback.inc

.. include:: vehicle-provenance.inc
