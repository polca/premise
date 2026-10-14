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
   :alt: Cars transformation: Regionalize mapped transport activities; Apply available efficiency factors; If fleet data exist: build service markets; Retain consumer selection; Check emission plausibility without changing amounts.
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
an available regional energy/service pair provides the fallback. An explicit
``energy_intensity_fallback_aliases`` mapping can then provide a same-region
proxy when both native calculation paths are unavailable. Otherwise the index
is one, explicitly marked as unavailable in the array's
``vehicle efficiency sources`` attribute and the transformed vehicle's
``log parameters / IAM efficiency source``. No other technology's trend is
inferred without an explicit mapping.

For REMIND/REMIND-EU Large cars, the configured proxy is the same powertrain's
``Large Car and SUV`` direct intensity, normalized to its own positive 2020
reference. Native Large Car trends take priority wherever available. The proxy
changes only relative efficiency: the original 2020 vehicle and the native
fleet-volume mappings remain in place, without duplicating SUV fleet volumes.
The usual cap and monotonicity rules apply. A missing or invalid proxy reference
leaves the signal unavailable. The source log and dataset comment explicitly
identify the proxy series, region and reason. This is a modelling assumption
about relative trends, not evidence that Large cars and SUVs have identical
absolute energy consumption or hardware.

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


Provisional combustion-car energy floor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mapped conventional diesel, gasoline and compressed-gas cars with an available
IAM efficiency signal receive a configurable minimum fuel-energy demand after
the IAM adjustment and before the read-only emission checks. Battery-electric, fuel-cell,
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
scaled by this additional correction. Both the IAM step and this floor apply the same fuel-derived emission
selection described above. Neither scales HBEFA distance/start/evaporation
factors or assumes ambiguous particulate flows are fuel-proportional.
Subsequent hot-factor diagnostics do not alter emissions.

Fuel selection explicitly recognizes petrol as well as gasoline markets,
including market groups. This fixes cases where IAM scaling previously changed
emissions but missed petrol inputs. The same old alias could wrongly select
``Passenger car, gasoline, ...`` manufacture inputs; these are now left at
their source recipe amounts instead of following fuel-efficiency changes.
Gas is supported in kg (46.5 MJ/kg, documented delivered CNG) or m3 (packaged fuel properties); liquid fuels use their
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
^^^^^^^^^^^^^^^^^^^^

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
energy use and supply changes. Review the validation report separately for
read-only emission diagnostics.

Sources and inventory details
-------------------------------

Source inventories and coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2020 refresh contains 74 RER transport services, 73 manufacturing recipes
and 15 supporting components. Services retain the selected catalogue's exact
names and use one vehicle-kilometre; manufacturing uses one vehicle. Combustion
variants use EURO-6. Historical EURO classes are excluded; electric vehicles
have no EURO class. The model uses Swiss climate, WLTC, 12,000 km/year and the
approved 1.8 cold starts/hot soaks per day.

The current catalogue is in
`lci-pass_cars.xlsx <https://github.com/polca/premise/blob/trals/premise/data/additional_inventories/lci-pass_cars.xlsx>`_
and its ``lci-pass_cars.parameters.csv`` companion. Only technologies matched by
``transport_passenger_cars.yaml`` are regionalized; RER coverage does not imply
that every variant has IAM fleet data. Regionalized RER entries can become
forwarding activities. LPG retains its explicit CNG manufacturing/efficiency
proxy and fossil supply policy.

PHEVs use one full physical pack across both modes, calibrated from 2020
manufacturer references and documented size/density proxies (8.8--24 kWh).
FCEV buffers use the approved late-2020 Mirai reference, scaled to required
buffer power; the Medium car has 1.60 kWh and 57.3 kg. The ten-second discharge
rating does not establish charging capability. See :doc:`campaign-closeout`
for retained limitations and the scope of the completed verification.

.. include:: battery-sizing.inc

.. include:: lpg-fallback.inc

.. include:: vehicle-provenance.inc
