Passenger-car inventory refresh, October 2026
=============================================

The passenger-car workbook now represents 74 transport services at the 2020
baseline, using carculator 1.9.7 and carculator_utils 1.3.7 from the local
October 2026 source revisions. It contains 73 vehicle-production datasets and
15 supporting component datasets. The three other vehicle workbooks are unchanged.

Generation choices
------------------

* Latest 2020 engineering and usage defaults; Swiss climate and default cycle.
* EURO-6 combustion cars; battery-electric and fuel-cell cars retained.
* Existing premise service and product identities retained; historical EURO
  variants removed from this supplementary workbook.
* Regional premise fuel and electricity markets. Model fuel energy is preserved
  using reference LHVs of 42.6 MJ/kg petrol, 43 diesel, 46.5 CNG, 120 hydrogen,
  and 45.5 LPG. Complete-oxidation reference CO2 uses premise's fuel factors.
  This is not a closed elemental carbon balance including other pollutants.
* Battery capacity markets receive nominal kWh times one plus lifetime
  replacements. Both original battery-mass purchases and battery disposal are
  removed because the capacity market includes both.
* Hydrogen compression is retained at 2.616238 kWh/kg and uses regional electricity.
* LPG: CNG efficiency, manufacture including its CNG tank, maintenance and
  non-CO2 combustion factors; LPG-specific fuel consumption and CO2. CNG-specific
  pump-to-tank methane leakage is excluded (zero in the current model defaults).
* Noise is omitted from premise imports and retained in ``lci-pass_cars.noise.csv``.
  ``lci-pass_cars.parameters.csv`` and ``lci-pass_cars.provenance.json`` record
  model values, replacement counts, source revisions and workbook hash.

Custom supporting components carry a passenger-car suffix to prevent old
bus/truck component imports from replacing corrected car recipes. The workbook
is declared as ecoinvent 3.12 so normal premise migrations use the correct source
version. This exercise validates the 3.12 cutoff build; it does not certify all
other versions or system models.

Integration changes
-------------------

The initial integration regionalized vehicle-grade natural-gas market wrappers,
and gas carbon reclassification accepts kg as well as cubic metres. The later
routing audit found that compression still used RoW/World suppliers; its
correction is described below. The initial comparison therefore did not validate
the intended regional compression chain.
FCEV energy aliases include refuelling electricity. The vehicle validator does
not apply battery-traction electricity bounds to hydrogen-compression inputs.
Both old and updated comparison builds use these identical integration changes.
Thus the comparison measures the inventory replacement, rather than mixing
that change with a different premise implementation.

Controlled EF 3.1 comparison
----------------------------

Both full databases use REMIND SSP2-NPi, year 2020, ecoinvent 3.12 cutoff,
identical encrypted IAM input, all default sector updates and deterministic
inventories. Only the passenger-car workbook and its declared source version
differ. Scores use one vehicle-kilometre and the 16 headline indicators from
``ecoinvent-3.12 / EF v3.1`` (including long-term emissions). No normalization
or weighting is applied, and climate/toxicity subcategories are not double-counted.

All 74 RER services and the 30 EUR services regionalized by REMIND were matched
uniquely in both databases: 1,664 paired indicator scores. Some RER activities
forward to regionalized services after a full premise update; these are scores
of the written databases, not raw workbook-only scores. All 35 mapping selectors
still match exactly one car template across the supported IAM mappings.

Both builds passed validation with zero errors and the same 89 background
warnings (76 CO2 plausibility and 13 electricity-efficiency warnings); none
concern passenger cars. The 99 focused regression tests passed. All 5,289
non-production workbook exchanges link exactly. All 73 written production
activities retain the intended battery-capacity amounts.

The change is not uniformly a reduction. Across RER cars, median climate change
falls 3.89%, with individual changes from -24.41% to +22.64%. Category medians
below describe the unweighted catalogue and are not fleet-average impacts.

.. list-table:: RER EF 3.1 changes, percent
   :header-rows: 1

   * - Indicator
     - Minimum
     - Median
     - Maximum
   * - acidification
     - -39.22
     - -3.28
     - 11.20
   * - climate change
     - -24.41
     - -3.89
     - 22.64
   * - ecotoxicity: freshwater
     - -23.09
     - -0.50
     - 6.56
   * - energy resources: non-renewable
     - -22.92
     - -6.04
     - 23.06
   * - eutrophication: freshwater
     - -24.11
     - -1.14
     - 6.77
   * - eutrophication: marine
     - -38.48
     - -3.87
     - 18.81
   * - eutrophication: terrestrial
     - -46.46
     - -6.51
     - 13.10
   * - human toxicity: carcinogenic
     - -21.67
     - 10.28
     - 83.45
   * - human toxicity: non-carcinogenic
     - -21.05
     - 5.29
     - 18.66
   * - ionising radiation: human health
     - -28.75
     - -1.74
     - 6.68
   * - land use
     - -12.55
     - 0.32
     - 16.97
   * - material resources: metals/minerals
     - -10.64
     - 0.09
     - 10.82
   * - ozone depletion
     - -37.88
     - 4.25
     - 12.18
   * - particulate matter formation
     - -24.96
     - -4.99
     - 6.85
   * - photochemical oxidant formation: human health
     - -36.19
     - -0.52
     - 32.81
   * - water use
     - -26.25
     - -0.63
     - 14.63

.. list-table:: Medium cars, RER climate change (g CO2-eq/vehicle-km)
   :header-rows: 1

   * - Vehicle
     - Old
     - Updated
     - Change, percent
   * - battery electric
     - 213.8
     - 198.2
     - -7.3
   * - compressed gas
     - 267.2
     - 261.6
     - -2.1
   * - diesel hybrid
     - 257.1
     - 253.7
     - -1.3
   * - diesel
     - 283.6
     - 271.6
     - -4.2
   * - fuel cell electric
     - 289.7
     - 220.8
     - -23.8
   * - gasoline hybrid
     - 302.9
     - 272.4
     - -10.1
   * - gasoline
     - 328.7
     - 362.2
     - +10.2
   * - liquefied petroleum gas
     - 287.7
     - 288.3
     - +0.2
   * - plugin diesel hybrid
     - 237.0
     - 222.7
     - -6.0
   * - plugin gasoline hybrid
     - 250.8
     - 231.5
     - -7.7

For the Medium BEV, reduced electricity demand saves 17.85 g CO2-eq/km and
maintenance/road/wear supply saves 4.71 g. Increased vehicle/battery manufacture
adds 6.85 g. The Medium FCEV manufacturing contribution falls 53.87 g and its
hydrogen supply falls 22.83 g, while explicitly retained compression adds 7.67 g.
The Medium gasoline increase is dominated by greater fuel use: direct emissions
add 28.06 g and fuel supply adds 9.09 g, partly offset by 3.62 g less manufacture.
These are additive first-tier contribution differences, checked against total LCIA.

The largest relative increase is Mini gasoline carcinogenic toxicity (+83.45%).
The contribution audit attributes almost all of the increase to direct emissions, principally formaldehyde;
this is reported rather than hidden by an overall score. Proxy assumptions and
premise's post-generation pollutant normalization remain relevant to interpretation.

Premise's post-generation processing changes 43 non-CO2 emission totals across
12 of the 30 updated EUR cars. The template-to-EUR audit records these changes
separately from the model replacement. Mini gasoline formaldehyde is unchanged
by that processing, so its toxicity increase originates in the vehicle inventory.
The old-workbook audit also contains biosphere-name migrations from ecoinvent 3.7;
its missing-name matches must not all be interpreted as emission scaling.

Reproduction and artifacts
--------------------------

In the sibling ``carculator_utils`` repository:

* ``dev/regenerate_premise_cars.py`` generates the workbook with a matched
  Python 3.12 carculator environment and the frozen original audit manifest.
* ``dev/validate_premise_cars.py`` checks boundaries and exact ecoinvent 3.12 links.
* ``dev/build_premise_car_comparison.py`` builds isolated old/new databases with
  ``PREMISE_KEY`` and ``BRIGHTWAY2_DIR`` set; it refuses existing database names.
* ``dev/compare_premise_car_ef31.py`` writes the full score workbook, CSVs,
  metadata and plot; ``dev/explain_premise_car_changes.py`` writes contributions.

Results are in ``results/premise_cars_2020_20261012/comparison/``. The main file is
``ef31_comparison.xlsx``; CSVs include every absolute and percentage change,
method tuple, activity key and foreground exchange diagnostics. Build JSON files
record workbook and encrypted IAM hashes. ``medium_cars_climate.png`` and its
SVG version provide a standalone figure. Full proprietary background inventories
remain in Brightway, outside the source repositories.

Saved Brightway comparison project: ``premise-cars-2020-ef31-20261012``.
The final databases are ``premise-remind-SSP2-NPi-2020-cars-old-20261012-final``
and ``premise-remind-SSP2-NPi-2020-cars-new-20261012-final``.


Evaporative speciation correction, 13 October 2026
---------------------------------------------------

The car workbook now keeps soak, diurnal and running-loss emissions separate
from exhaust speciation. Existing evaporation-specific benzene is retained and
the remaining mass is generic NMVOC. Vehicle defaults, operating energy,
production inputs and cold-start factors are unchanged.

In a fresh REMIND SSP2-NPi 2020 / ecoinvent 3.12 cutoff build, Mini gasoline
carcinogenic toxicity falls from 8.83878e-11 to 7.60697e-11 CTUh/vehicle-km
relative to the initial refresh (-13.94%). Its increase over the original
workbook is now 57.88%, versus 83.45% before this correction. Cold-start
assumptions remain to be validated. All 1,664 paired EF 3.1 scores were
recalculated; the build has zero errors and the same 89 background warnings.

Source qualification and accounting checks are documented in the shared
carculator_utils emission audit. The installed provenance sidecar records
the updated workbook and implementation hashes.

PAH identity correction, October 13
--------------------------------------

Transport normalization and validation now match aggregate Hydrocarbon or
Hydrocarbons by exact flow name, preventing their coefficients from being
applied to PAH or chlorinated hydrocarbons. Only the selected air biosphere
exchanges are rescaled. Other pollutant matching and factors are unchanged.
The input car workbook is unchanged from the evaporation correction.

Both old- and new-workbook databases were rebuilt with the corrected code,
REMIND SSP2-NPi 2020 and ecoinvent 3.12 cutoff. All workbook and IAM hashes
match their preceding builds. Full database comparisons find only 156 PAH
exchange changes in the updated database, and 234 PAH plus 234 chlorinated
hydrocarbon changes in the old database. These affect 78 gasoline activities
(six sizes in thirteen IAM regions), with unchanged technosphere exchanges.
The old chlorinated-hydrocarbon identities remain a legacy limitation.

Mini gasoline EUR PAH falls from 3.69160 to 0.00148516 mg/km. Across the
updated RER/EUR catalogue, EF 3.1 freshwater ecotoxicity decreases by up to
0.3566%, and non-carcinogenic toxicity by up to 0.1905%; the other fourteen
headline indicators are unchanged beyond roundoff. The installed EF methods
characterize generic PAH for those two categories, not carcinogenic toxicity.
This does not imply absence of carcinogenic hazard.

Both builds have zero validation errors and 167 warnings: 89 background
warnings and 78 newly exposed missing aggregate Hydrocarbon-flow warnings.
The latter show that total HC cannot be checked by substituting PAH. They
were retained, not suppressed. Full validation of speciated exhaust totals
and cold-start-sensitive normalization remains separate work.

The six PAH regressions fail before the correction and pass after it; 64
focused Premise tests pass. An independent carculator_utils audit checks
car, truck, bus and two-wheeler PAH through model, matrix and Excel exports.
Multiplying diagnostic total HC by 1000 leaves PAH and all biosphere exports
unchanged. Eleven shared hot-emission tests pass; no shared runtime fix was
needed for this issue.

Results and reproducible scripts are in the sibling carculator_utils checkout,
under ``results/premise_cars_pah_20261013`` and ``dev/``. The saved comparison
project is ``premise-cars-2020-ef31-20261012``, with databases ending in
``cars-old-20261013-pah`` and ``cars-new-20261013-pah``.


Read-only pollutant diagnostics, 13 October 2026
--------------------------------------------------

This follow-up supersedes the remaining normalization described above. The
transport update no longer clamps emissions to hot-factor expectations. Exact
species aliases prevent methane from matching NMVOC and preserve the PAH/THC
separation. Complete EURO labels select VI rather than V for trucks and the
packaged key 6 for older car 6ab factors. No fuel coefficients are changed.

Hot-only discrepancies are diagnostic warnings, with explicit qualifications
about cold starts, evaporation, deterioration and calibration. Missing THC and
residual-NMVOC boundaries are reported without synthesizing aggregate flows.
Invalid matched masses, units and supported fuel inputs remain errors. Fuel
market groups are recognized and missing fuel references do not imply zero
energy. IAM efficiency scaling and the existing car energy floor are unchanged.

The review does not validate the inherited cold-start assumptions or NH3/N2O
calibration multipliers. Those remain separate modelling questions. Results,
source hashes, regression logs and the paired EF 3.1 comparison are recorded in
``carculator_utils/results/premise_cars_emission_checks_20261013``.

Both final builds have zero validation errors: 323 warnings for the old workbook
and 453 for the new workbook. These include the unchanged 89 background warnings
plus explicit aggregate-scope and hot-factor diagnostics; none are suppressed.
All 125 focused tests pass, and 528 template-to-EUR non-CO2 comparisons preserve
the new car emissions. All 3,328 before/after EF 3.1 score changes are independently
reconstructed from changed emission masses and characterization factors.

Full database comparison finds 936 changed biosphere exchanges in 78 old-car
activities and 962 in 156 new-car activities across all IAM regions. Technosphere
inputs and truck inventories do not change in this scenario. Removing the clamp
raises new Mini gasoline photochemical oxidant formation by 12.07%; the largest
old-car correction effect is +1.38% non-carcinogenic toxicity. In the corrected
old/new comparison, Mini gasoline carcinogenic toxicity is +61.82%, retaining the
need to validate cold-start assumptions. The final saved database names end in
``cars-old-20261013-emission-checks-r2`` and
``cars-new-20261013-emission-checks-r2``; earlier comparisons remain available.


Cold-start activity update, 13 October 2026
-------------------------------------------

The regenerated 2020 cars now explicitly use 1.8 starts/stops per day, selected
by the user from Switzerland's National Inventory Document 2026, Table 3-90,
2020 passenger-car column. Combustion-car annual mileage remains 12,000 km.
The electric Micro's 6,000 km default is unchanged. The shared model exposes
``starts_per_day`` and retains 2.3 as its general default; the generation script
passes 1.8. Both cold-start and hot-soak additions change by 1.8/2.3; hot
exhaust, diurnal evaporation, running losses and per-event factors are retained.
PHEV emissions keep the existing combustion-mode weighting.

The 1.7% formaldehyde fraction agrees with EMEP/EEA's June 2026 Euro-6 petrol
profile (Table 3-86d). The source supports applying the profile to total exhaust
NMVOC; this audit found no unit-conversion error in the cold-start calculation.
The combustion-car dataset comments and shared provenance record:
"the original LCA model, carculator, uses emission factors for cold start,
evaporative and running emissions from the HBEFA 5.1 emissions handbook". Detailed
export settings are not separately recorded; the CH option does not recompute
these fixed coefficients.

The workbook retains 162 activities. Only 1,221 biosphere exchanges in 57 car
services change; physical parameters, energy demand, manufacturing and battery
exchanges are preserved. The original old-car baseline is reused with the same
corrected Premise code and REMIND SSP2-NPi 2020 settings. Mini gasoline EUR
formaldehyde falls from 1.14240 to 0.90314 mg/km. Its carcinogenic-toxicity score
falls by 8.50%, leaving a 48.07% increase versus the old vehicle (previously
61.82%). The per-start source attribution is resolved; the exhaust-benzene
accounting question remains a separate issue.

The new database ends in ``cars-new-20261013-coldstart18-r2``. Results, source
references, process contributions, tests and comparison spreadsheets are in
``carculator_utils/results/premise_cars_cold_start_20261013``. Earlier saved
comparisons remain available.


Regional CNG supply-chain correction, 13 October 2026
-----------------------------------------------------

The fuel transformation now regionalizes vehicle-grade compression before
creating regional CNG markets. Compression purchases high-pressure gas and
electricity from the corresponding IAM region. The source m3/kg conversion,
compression demand, infrastructure and direct losses are preserved. Original
geographic activities forward one kilogram to their IAM replacement, including
World; the previous global market forwarded only 12/13 kg in REMIND.

This correction applies to all consumers of those CNG suppliers, including cars
and trucks. It does not settle the separate blend-energy/carbon accounting or
post-regionalization tailpipe CO2 questions. Earlier saved LCIA comparisons
remain historical results and will be rebuilt after the remaining corrections.

Focused regressions cover supplier paths, forwarding balances and repeated
execution. Isolated full 2020 and 2050 REMIND SSP2-NPi / ecoinvent 3.12 cutoff
builds and the regional routing audit are reproduced by
``carculator_utils/dev/build_premise_cng_routing.py`` and
``carculator_utils/dev/audit_premise_cng_routing.py``. Results are recorded in
``carculator_utils/results/premise_cng_routing_20261013``.

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

The isolated 2020 and 2050 REMIND SSP2-NPi / ecoinvent 3.12 cutoff builds each
pass all 495 direct-fuel car/truck checks, including 273 car services. Neither
has validation errors; the existing 401/398 warnings are unchanged. All
numerical changes relative to the preceding fuel-accounting builds are car/
truck CO2 exchanges. The focused suite passes 342 tests, including 18 new
carbon-accounting cases. Mutation checks confirm that removing the final
transport refresh or transfer scaling makes the corresponding tests fail.

Reproduction and aggregate diagnostics are in the sibling checkout under
``dev/build_premise_vehicle_carbon.py``, ``dev/audit_premise_vehicle_carbon.py``
and ``results/premise_vehicle_carbon_20261013``. Saved LCIA comparisons remain
historical until the planned final controlled rebuild. Hydrogen compression
electricity was subsequently corrected as described below; hardware and future
sizing remain open.

Hydrogen tank-compression correction
-------------------------------------

All eight FCEV car templates now carry an explicit compression contract and
use the approved 30-bar pipeline delivery boundary. With the existing 700-bar
tank and compressor assumptions, electricity is 2.450619 kWh/kg H2 instead of
2.616238. Premise preserves that specific requirement through efficiency changes
and keeps other electricity uses separate. The upstream pipeline preparation
is already included in the hydrogen market and remains unchanged.

The full 2020/2050 builds each pass 147 marked hydrogen-vehicle checks (80 cars
and 67 trucks), with zero validation errors and the same existing warning
identities. The focused suite passes 365 tests, including 23 new hydrogen
cases and real Excel import. Only the identified vehicle compression electricity
coefficients change. Report and reproduction scripts are in
``carculator_utils/results/premise_vehicle_hydrogen_20261013`` and ``dev/``.
