Regional truck refresh, October 2026
====================================

The truck workbook now uses current 2020 carculator_truck defaults, Swiss
climate, and the Regional delivery cycle with a 400 km design range. The 52
service identities are retained. Combustion vehicles use the model's 2020
EURO-VI class. Payload and annual mileage use the cycle-specific defaults;
these differ from the previous 800 km, mostly 2050 vehicle descriptions.

Battery-electric specifications
--------------------------------

.. list-table::
   :header-rows: 1

   * - Gross size (t)
     - Nominal battery (kWh)
     - Payload (t)
   * - 3.5
     - 133.8
     - 0.75
   * - 7.5
     - 203.2
     - 1.75
   * - 18
     - 333.1
     - 3.2
   * - 26
     - 405.6
     - 6.3
   * - 32
     - 518.2
     - 10.3
   * - 40
     - 550.9
     - 10.3
   * - 60
     - 725.2
     - 19.3

BEVs use 80% depth of discharge and NMC622 physical sizing defaults. The model
returns zero lifetime BEV battery replacements. Premise's capacity markets
supply the scenario chemistry mix and include production and disposal.
Capacity exchanges use nominal capacity times one plus modelled replacements.
The old projected-capacity selector matches ``truck`` manufacturing names,
not these ``lorry production`` activities; the regenerated quantities remain
intact in the tested build.

For PHEVs, nominal capacity is recovered from the full physical pack mass,
cell mass share and cell energy density. The shared model reports an
energy-capacity parameter weighted by electric utility factor while retaining
full pack mass. Using that weighted parameter would undercount manufacture.
The 40 t PHEV has a 77.32 kWh nominal pack and 102.32 kWh lifetime capacity
purchases with the model's 0.3233 replacement count. The replacement-count
aggregation and capacity/range reporting remain subjects for a separate
shared-model review. The inventory adapter does not change model code.

Import and supply contracts
---------------------------

``lci-trucks.xlsx`` contains 52 services, 42 vehicle-production activities and
33 qualified supporting components and is imported as ecoinvent 3.12.
``lci-truck-legacy-components.xlsx`` retains 26 old shared component suppliers
for other inventories and keeps its ecoinvent 3.7 migration. The qualified
lorry frame receives the same explicit per-kilogram metal conversion as the
original frame. This avoids changing other vehicle recipes through shared
component names.

The ten gasoline/LPG variants retain documented diesel/diesel-hybrid and CNG
proxies. Conversions preserve modelled fuel energy, using 42.6 MJ/kg gasoline
and 45.5 MJ/kg LPG, with reference CO2 factors of 0.0737 and 0.0631 kg/MJ.
LPG omits generic-air CNG leakage. Manufacture and non-CO2 combustion emissions
remain proxy assumptions. Regional scenario markets determine fuel blends and
electricity; hydrogen compression electricity is retained.

``lci-trucks.parameters.csv`` records all service specifications.
``lci-trucks.noise.csv`` retains 1,248 custom noise exchanges omitted from the
Premise import. ``lci-trucks.provenance.json`` records source and workbook hashes.

Controlled comparison
---------------------

The full REMIND SSP2-NPi 2020 comparison uses ecoinvent 3.12 cutoff and the
same updated passenger cars in both databases. All 52 RER truck catalogue
services and the 20 EUR services are compared across 16 EF 3.1 headline
categories: 1,152 score pairs, per tonne-kilometre, without weighting.
RER forwarding activities are resolved before contribution analysis.

The EUR 40 t BEV climate result changes from 0.071151 to 0.093807 kg CO2-eq/tkm
(+31.8%). Its payload falls from 13.8 to 10.3 t; multiplying each score by its
own payload gives a 1.6% decrease per vehicle-kilometre. For the 18 t BEV the
tkm result doubles, while the vkm result falls 13.5%, with payload falling
from 7.4 to 3.2 t. These are unit diagnostics, not common-payload simulations.

The largest ozone-depletion changes trace mainly to newly included refrigerant
supply and increased fuel demand per tkm. The old 18 t LPG fuel purchase implies
4.65 MJ/vkm despite a comment reporting 9.289 MJ/vkm; the corrected proxy uses
9.03 MJ/vkm. Its large climate increase partly corrects that old inconsistency
and the old fossil/biogenic carbon split. It is not simply a technology trend.

The build has zero validation errors and 401 warnings, the baseline warning
count. All 3,429 non-production template exchanges link; all 38 selectors,
42 production battery quantities and 205 EUR non-CO2 checks pass. The affected
metals suite passes 81 tests. No existing non-truck activity recipes change;
33 qualified supporting activities are added. Upstream LCIA results can still
change because freight transport is used throughout the database.

Independent SuperLU solves reproduce the scores within 9.92e-8 relative
difference, with maximum demand residual 3.11e-15. The original solver's
conditioning warning remains documented. Proxy emission factors and PHEV
replacement aggregation are not independently validated by these checks.

The reproducible scripts and detailed report are in the sibling
``carculator_utils`` checkout, under ``dev/`` and
``results/premise_trucks_regional_20261013/``. The persistent comparison project
is ``premise-cars-2020-ef31-20261012``. The new database is
``premise-remind-SSP2-NPi-2020-trucks-new-20261013-regional``; the baseline
``premise-remind-SSP2-NPi-2020-cars-new-20261013-coldstart18-r2`` is retained.
The installation receipt contains saved-database score checks and installed
file hashes.

Delivered CNG reference correction
----------------------------------

The 13 October fuel-accounting correction uses the ecoinvent vehicle-grade gas
market's documented 46.5 MJ/kg. CNG service purchases increase by 47.5/46.5
while retaining model energy, hardware, other emissions and exact identities.
The companion provenance records the approved conversion and frozen raw model
export. Compression quantities remain unchanged and their larger upstream gas
energy is recorded as a separate boundary. Updated blend-energy/carbon ledgers
are available in scenario metadata.

Final vehicle fuel-carbon accounting
------------------------------------

The subsequent 13 October correction applies those properties after final
vehicle linking and efficiency adjustment, using the fuel module's common
CO2 helper. Regional clones no longer retain the template's carbon split.
Stored transfer amounts scale with their exchanges; repeated updates preserve
the corrected inventory. Validation checks fossil and non-fossil emissions
separately against actual purchases and suppliers. The convention is full
oxidation, with separate CO/CH4/NMVOC and negative credits unchanged.

Both isolated 2020 and 2050 REMIND SSP2-NPi / ecoinvent 3.12 cutoff builds pass
all 495 direct-fuel car/truck checks, including 222 truck services. Neither has
validation errors; the existing 401/398 warnings are unchanged. For example,
the 2050 MEA diesel truck previously inherited EUR's 56.52% non-fossil fraction;
its actual supplier implies about 0.00036%. Correcting this classification
does not change its total CO2 or fuel purchase.

All numerical changes relative to the preceding fuel-accounting builds are
car/truck CO2 exchanges. The focused suite passes 342 tests, including 18 new
carbon-accounting cases. Reproduction and aggregate diagnostics are in the
sibling checkout under ``dev/build_premise_vehicle_carbon.py``,
``dev/audit_premise_vehicle_carbon.py`` and
``results/premise_vehicle_carbon_20261013``. Battery hardware, future resizing
and the final controlled EF 3.1 comparison remain open.

Hydrogen tank-compression correction
-------------------------------------

All seven FCEV truck templates now carry an explicit compression contract and
use the approved 30-bar pipeline delivery boundary, with 2.450619 kWh/kg H2
for their existing 700-bar tanks. Premise couples this electricity to final
hydrogen purchases. The EUR 40 t truck now uses 0.01519764 kWh/tkm in 2020 and
0.01215811 in 2050, matching the 20% hydrogen-demand reduction. Previously its
electricity stayed at 0.01622473 kWh/tkm in both years. The total correction
combines the documented inlet-pressure change with the scaling repair.

The upstream pipeline service's compression remains in the fuel market; it
ends at 30 bar and does not replace the separate vehicle tank stage. All
147 marked hydrogen car/truck services pass in each full build, with zero
validation errors and unchanged existing warning identities. The suite passes
365 tests, including 23 new hydrogen cases. Report and reproduction scripts
are in ``carculator_utils/results/premise_vehicle_hydrogen_20261013`` and
``dev/``. PHEV physical-capacity aggregation is the next planned issue.
