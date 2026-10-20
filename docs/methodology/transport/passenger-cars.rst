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

The inherited 4.26 MJ/vehicle-km upper fuel-energy screen produces a warning.
It is a plausibility heuristic rather than a physical maximum; historical IAM
adjustments or vehicle size can exceed it. Fuel amounts are preserved, while
violations of the configured energy floor and fuel-specific CO2 mismatches
remain errors.

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
leaves the signal unavailable. The structured source log identifies the proxy
series, region and reason. This is a modelling assumption about relative trends,
not evidence that Large cars and SUVs have identical absolute energy consumption
or hardware.

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

Electric mechanical-powertrain inventory correction
----------------------------------------------------

Carculator's imported BEV and fuel-cell cars previously assigned their
mechanical-powertrain mass to ``market for internal combustion engine,
passenger car``. That supplier represents a whole combustion drivetrain,
including starting and exhaust equipment. A medium BEV's 89.879898 kg proxy
therefore purchased about 0.523 g platinum and 0.098 g palladium directly from
its catalyst recipe, despite already having separate electric-motor and control
inventories. These are direct source ingredients, not the car's full impact.

The 17 BEV/FCEV production variants now use
``lci-electric-car-mechanical-powertrain.xlsx``. Its mechanical recipe draws
only on the electric gearbox and cooling components in
`Notter et al. (2010), supporting Table S3
<https://pstorage-acs-6854636.s3.amazonaws.com/4414579/es903729a_si_001.pdf>`__
(PDF page 6). That table's caption repeats ICE, but its heading and component
rows describe electric equipment. The 25 kg retained reference comprises
steel/aluminium gearbox parts and cooling materials. Source waste factors
produce 1.2212 kg gross material purchases per kg equipment; manufacturing
scrap removes 0.2212 kg. Motor, inverter, converter, charger, PDU, traction
battery and fuel-cell links remain separate. The source's motor, controller,
charger and cable rows are not copied into this mechanical recipe.

Assembly energy, water, transport and factory inputs use a mass-allocated
proxy from the complete 97.51 kg historical electric reference. This is not
measured gearbox-only manufacturing. The existing model component mass is
retained; neither torque sizing nor a modern bill of materials is validated.
The updated material shares are an explicit historical technology proxy.

A dedicated mechanical end-of-life process sends retained steel and aluminium
to native scrap markets. Manufacturing aluminium scrap uses the RoW market, since no
global market exists; post-consumer aluminium and unsorted iron use their
global markets. Signed scrap quantities are retained across system models.
Polymers/rubber use mixed-plastic
treatment, coolant uses hazardous-waste incineration and a generic manual
facility rate supplies dismantling infrastructure. Retained end-of-life flows
sum to one kg. These are stated treatment proxies. The motor/electronics
scrap fractions are not applied to mechanical equipment, and no new
primary/secondary scenario or future recycling efficiency is imposed.

Common auxiliary battery and mass accounting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The old source's starter-battery ingredients are isolated as a provisional
common auxiliary-battery input in full vehicles. On its 275 kg reference basis,
that historical component contains 13 kg lead, 4 kg case material and 1 kg
acid. Its case retains the original PPS material proxy and gross loss factor;
these do not establish a modern battery chemistry or composition.

For original nominal proxy demand ``M`` kg, full vehicle production now buys
``M * 257/275`` kg mechanical equipment and ``M * 18/275`` kg of the historical
auxiliary proxy. Their sum remains ``M``; this is a bookkeeping split, not a
new measured battery specification. Direct lead remains
``M * 18/275 * 13/18 = M * 13/275`` kg. For the medium BEV this gives
83.996850 kg mechanical equipment and 5.883048 kg historical battery-equivalent
input, preserving 4.248868 kg direct lead in the full vehicle.

The labelled common component is excluded when constructing energy-specific
per-MJ inventories, as are the common glider and road. This does not claim zero
lead throughout EV supply chains. Battery voltage/capacity, actual mass,
calendar replacement life and common-system consistency across combustion
vehicles remain unresolved. Existing whole-vehicle maintenance is retained;
no independent auxiliary-battery replacement schedule is inferred.

Both electric and hydrogen passenger-car final-energy wrappers use the
mechanical fraction divided by their actual lifetime purchased-energy basis.
Motor, controls, traction-capacity and fuel-cell quantities, fuel supply,
emissions, occupancy, lifetime and performance parameters are preserved.
Their source metadata records the common component as excluded and retains
the original model provenance together with the local correction.

Reproduction and limits
~~~~~~~~~~~~~~~~~~~~~~~~

``data/transport/electric_mechanical_powertrain.json`` records the source,
component masses, waste factors, allocation basis and treatment limitations.
After a compatible car inventory refresh, run
``python scripts/update_electric_car_mechanical_inventory.py``. The script
builds the component workbook, patches the 17 electric vehicle production
activities and changes ten cells in the two final-energy wrappers. All 459
final-energy formulas and their cached values are retained. Existing
combustion/hybrid activities and all other source activities remain unchanged.

The new suppliers load before passenger cars, and their workbook participates
in the import-cache fingerprint. Rebuild scenarios to apply the correction;
saved databases, indicators and dashboards are not regenerated. Source
allocation checks validate units, material balance and the per-MJ arithmetic;
they do not certify a contemporary EV design. The background combustion-engine
recipe's remaining mass-accounting limitations are described below.

Historical combustion-drivetrain aluminium correction
------------------------------------------------------

Source basis and quantities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For ecoinvent 3.10, 3.11 and 3.12 cutoff, source cleaning corrects the recognized
native ``internal combustion engine production, passenger car`` recipe and its
linked shredding treatment together. The nominal reference is the historical
275 kg complete combustion drivetrain, not a measured contemporary bare engine.

The aluminium component rows in
`Notter et al. (2010), supporting Table S2
<https://pstorage-acs-6854636.s3.amazonaws.com/4414579/es903729a_si_001.pdf>`__
(PDF page 5) sum to 49.5 kg retained aluminium. Their 1.25 gross purchase factor
gives 61.875 kg input and 12.375 kg manufacturing scrap. This agrees with the
grouped aluminium input in Althaus and Gauch (2010), Table 80. The native
recipe instead purchases 170.07 kg and exports 34.014 kg manufacturing scrap
on that same reference. The excess gross purchase equals the recipe's
108.195 kg low-alloy steel purchase, but this arithmetic is not proof of how
the error arose.

The correction uses a single factor ``61.875/170.07`` for gross aluminium,
manufacturing aluminium scrap and post-consumer aluminium scrap:

.. list-table:: Aluminium flows per nominal kg of combustion drivetrain
   :header-rows: 1
   :widths: 48 26 26

   * - Flow (kg)
     - Native
     - Corrected
   * - Cast-alloy input
     - 0.618436364
     - 0.225
   * - Manufacturing scrap (outflow magnitude)
     - 0.123687273
     - 0.045
   * - Retained after manufacturing allowance
     - 0.494749091
     - 0.18
   * - Post-consumer scrap (outflow magnitude)
     - 0.408771
     - 0.148719384

Post-consumer recovery retains the native ratio of about 82.62188% of retained
aluminium. This is an inherited, unvalidated treatment assumption, not a new
recycling-efficiency estimate. Negative cutoff scrap and waste-reference signs
are preserved. No primary/secondary market shares change.

Guards, cache handling and limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The correction requires unique GLO kilogram production, waste-market and
shredding activities, their original signed reference outputs and waste links,
and all three original aluminium coefficients. The raw production activity
UUID is checked when available. Exact cutoff scrap supplier identities exclude
other system-model recipes. Changed formulas, unrecognized uncertainty types,
ambiguous suppliers and unsupported versions leave the entire chain unchanged.
Float32 source-storage rounding is tolerated. Rescaling preserves relative
uncertainty and existing comments; appended comments identify the source and
the retained treatment assumption. Repeated cleaning does not rescale again.

Affected source-cache names include ``ice_aluminium_v1``. This also invalidates
compact checkpoints that refer to the previous source cache, forcing source
cleaning before new scenarios use it. Import caches are not rewritten. Source
Brightway databases, vendor files and saved scenario results are not modified.

This is an aluminium correction, not complete mass closure. Steel purchases and
manufacturing scrap still have a separate source-factor discrepancy. The
nominal 275 kg includes fuel/operating fluids in the source; native manufacture
omits the initial fuel. Other treatment outputs and generic shredder emissions
are retained. Named treatment outflows total approximately 0.745090 kg per
nominal kg after removing the excess aluminium, versus 1.005142 kg before.
The missing mass is explicitly unresolved; no invented material is assigned to
residue, and no universal ``275/220`` rescaling is applied. Vehicle component
demands and performance specifications are unchanged. The separate electric
mechanical/auxiliary inventories above are not targets of this correction.

Sources and inventory details
-------------------------------

Source inventories and coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2020 refresh contains 74 RER transport services and 73 manufacturing recipes.
Supporting components reuse the existing canonical inventories shared with
trucks, two-wheelers, fuel cells and carbon-fibre production, rather than
duplicating them under model-specific names. Their original quantities,
uncertainty and source-background declarations are retained; normal import
migrations adapt them to the selected background. Removed model-specific
provenance remains in the development audit manifest and consumer comments.
Services retain the selected catalogue's exact
names and use one vehicle-kilometre; manufacturing uses one vehicle. Combustion
variants use EURO-6. Historical EURO classes are excluded; electric vehicles
have no EURO class. The model uses Swiss climate, WLTC, 12,000 km/year and the
approved 1.8 cold starts/hot soaks per day.

The current catalogue is in
`lci-pass_cars.xlsx <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-pass_cars.xlsx>`_
and its generated parameter snapshot is retained with the test data. Only technologies matched by
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
