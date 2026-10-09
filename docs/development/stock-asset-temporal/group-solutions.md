# Proposed solution and data choice for each stock-asset group

Status: recommended implementation decisions, 9 October 2026. These are recipes
to implement and validate, not calibrated temporal defaults. They refine the
[campaign](data-search-campaign.md) and [asset review](asset-review.md).

## Common decisions

Use three reusable models: a cohort table for registered/surveyed stocks; an IAM
cohort adapter for modelled stocks; and a component-renewal model for replacements.
Generate explicit annual weights from these models. Retain parametric survival
where supported; stop using an assumed lifetime or mean age directly as a
parametric stock-age distribution.

For the first revision, preserve each existing exchange total and record
`common_amortisation` where its lifetime-service denominator is retained. Form
timing weights from service contributions within the matched asset class. Equal
current use and common lifetime service reduce this to stock shares. Capacity,
area, length and vehicle counts are different weighting bases; conversions and
equal-use assumptions must be explicit. Do not introduce individual-lifetime
reweighting simply because an asset is old.

Every recipe below requires the caller/supplier role check. A new pump supplied
to a new plant is manufactured near that construction event. An existing pump
providing service has its own installation cohort. A replacement pump is dated
to its replacement event. These cannot share one supplier-wide age profile.

Source IDs S01–S29 resolve in the [source register](data-source-register.md),
which records what was actually inspected. File paths in the local-prior table
refer to the repository. Selecting a source here does not imply its full data
have been downloaded or access to operator records has been obtained.

The countries below define first pilots, not global defaults. Aggregate only
compatible country records to IAM regions, weighted by the relevant stock or
service quantity. Transferring a regional profile to another population is a
labelled proxy with sensitivity, not additional empirical coverage.

## Initial-stock and future rules

For an observed cohort at year `t0`, evolve its survivors conditionally:

```text
N(c,t) = N(c,t0) * S_c(t-c) / S_c(t0-c), for c <= t0 and t >= t0
```

This requires positive survival at the observed age and compatible retirement
definitions. Add new cohorts and used-asset transfers separately. Never apply
survival again to the observed stock at `t0`, or discard observed old assets
because an assumed fixed lifetime is shorter than their age.

For finite observed age bands, use uniform within-band splitting as the initial
annualisation assumption, preserving band totals and testing alternative shapes.
Handle the oldest open bin using documented historical bounds/survival tails;
retain unknown ages as an explicit residual. Neither is automatically zero.

For the future, use this order:

1. Validated native IAM cohorts, preserving exact scenario and reporting periods.
2. Observed initial cohorts plus scenario gross additions and compatible survival.
3. A stock-demand path with documented service-to-stock conversion, solving for
   additions and retirement consistently; investigate declines/idle capacity.
4. For mature assets without a stock-demand path, a **constant-stock renewal
   scenario**: retain and age initial cohorts and replace losses. This is a
   labelled scenario, with growth/decline sensitivity, not an IAM forecast.

Keep a reproduction of native IAM cohorts distinct from an IAM-constrained
model reinitialised with observed cohorts. Changing initial vintages can change
future retirement and required additions; reconcile those changes rather than
silently splice incompatible histories.

In a stock-driven model, do not interpret negative inferred additions as physical
demolition. Resolve early retirement, utilisation, transfers and source revisions.
Do not scale roads or grids automatically with car sales or generated electricity.
For rapidly growing technologies, obtain deployment information before approving
a constant-stock fallback.

If no initial age data exist, a stationary fallback has age weights proportional
to the survival function, `S(a)`, not the lifetime probability density. If only a
defensible mean life exists, a fixed-life stationary baseline gives uniform ages
over that life. This is a declared prior; absence of a defensible lifetime or
asset boundary leaves the match unresolved. No numeric default is invented here.

## Selected solutions

### 01. Buildings — `buildings`

**Solution:** observed construction cohorts for the structural shell, split by
residential/non-residential type. Keep renovation and building equipment on
separate event histories. Use floor area by cohort when the inventory represents
area; building counts require size conversion before that use.

**Data to use:** S11 Swiss FSO construction-period tables as the first Swiss
baseline; S13 US CBECS for a commercial-building pilot with survey weights and
floorspace. Use S10 EU Building Stock Observatory only where a populated,
downloadable construction-age series is verified. L1 supplies survival candidates.

**Future/fallback:** initial cohorts plus construction/demolition or a floor-area
demand path if acquired. Otherwise use constant-stock renewal and sensitivity.
An industrial shell without matching observations gets a labelled shell prior,
not a residential building profile presented as observed industrial evidence.
**Gate:** match shell type, floor-area totals, oldest cohorts and demolition
definition; heating-system installation is not building construction.

### 02. Roads and pavements — `roads_pavements`

**Solution:** split subgrade/foundation construction from wearing-course renewal.
Use construction cohorts for the former and last resurfacing/replacement dates
for the latter. Split exchanges or lifecycle components before assigning dates.

**Data to use:** S14 Canadian `Highways`, `Arterial roads`, `Collector roads` and
`Local roads` construction bands as the first regional civil-stock pilot. Confirm
reported physical measures before conversion to lane area or LCI road units.
For surfaces, use road-authority resurfacing logs when available; meanwhile L2
has surface/base and traffic-class lifetime leads to verify against originals.

**Future/fallback:** constant physical network plus component renewal unless
documented expansion is supplied. Use traffic-dependent surface intervals only
where the source supports them. **Gate:** a resurfacing event replaces only its
material layer; the asphalt recycling-yield workbook supplies no age parameters.

### 03. Rail infrastructure — `rail_infrastructure`

**Solution:** component renewal for rails, sleepers, ballast, electrification and
signalling; separate long-lived civil works. The first general implementation
should be an explicit component-prior model, pending operator cohort extracts.

**Data to use:** S18 ORR/Network Rail studies as dated component-age evidence and
routes to operator records; L2 for class-specific life assumptions and original
citations. S14 Canadian `Tracks` construction bands can test transit civil-stock
handling, but cannot supply the last rail/sleeper replacement date.

**Future/fallback:** renewal on the existing track network with separate capacity
expansion if obtained. **Gate:** preserve track-length units and traffic duty;
do not transfer rolling-stock age or a century-old route opening to modern rails.
No current national component histogram is claimed available.

### 04. Bridges, tunnels and civil structures — `bridges_tunnels_civil_structures`

**Solution:** original structural cohorts with rehabilitation as separate events.
Start with a US bridge model and a Canadian tunnel/bridge age-band model.

**Data to use:** S15 FHWA National Bridge Inventory construction/reconstruction
fields; S14 contains road-bridge classes, `Tunnels`, `Pedestrian tunnels` and
transit-exclusive structures. Use L2 only for survival/extrapolation. Convert
counts to deck area, length or another inventory-compatible unit where needed.

**Future/fallback:** cohort survival plus renewal/expansion scenarios; a structural
rehabilitation does not reset the entire asset. **Gate:** audit why the old
review assigned no rows to this group, then validate the actual matched assets.
Do not spend acquisition effort on an empty or incorrectly mapped class.

### 05. Waterways, canals and harbours — `waterways_canals_harbours`

**Solution:** site/cohort civil structures, separate lock machinery and harbour
equipment, with dredging represented as maintenance events. For now use bounded
case-study models or explicit civil/component priors rather than a global curve.

**Data to use:** S19 USACE lock database is an acquisition lead whose current
download is unresolved. Use documented construction and renewal dates from a
selected canal/port operator when obtainable. L2 and the matched inventory's
documented life are candidates only where the actual structure boundary matches.

**Future/fallback:** retain the existing civil stock and renew identified equipment;
add expansion only from a documented scenario. **Gate:** no generic harbour age
from lock data and no assumed access to an operator asset register. If no matching
life or dates can be defended, retain unresolved status for that subasset.

### 06. Pipelines and buried networks — `pipelines_buried_pipe_networks`

**Solution:** installation-decade cohorts by application, material and size.
Separate energy pipelines from potable-water, wastewater and stormwater pipes.

**Data to use:** S16 PHMSA decade inventories for US energy pipelines. S14 provides
Canadian water/sewer construction bands, including `Local water pipes`,
`Transmission pipes`, `Sanitary forcemains` and diameter-specific sewer categories.
L2 offers material-specific water-pipe survival candidates.

**Future/fallback:** evolve installed lengths with replacements and any verified
network-expansion path. Use material-specific stationary priors only for gaps.
**Gate:** reconcile length/diameter/material to the LCI unit; relining dates do
not necessarily replace the full pipe. Gas-to-hydrogen conversion is not proof
of a newly manufactured pipeline.

### 07. Electricity grids and substations — `electricity_grids_substations`

**Solution:** component cohorts for lines/cables, transformers, switchgear and
civil substations, separated by voltage. Begin with a scoped UK operator pilot
if completed age returns can be obtained; otherwise use component priors.

**Data to use:** S17 Ofgem AP1 installation-year/asset-class returns are the
preferred acquisition target; published guidance alone is not a populated
dataset. L3 supplies transformer/line life candidates, with S29 for appropriately
matched switchgear survival. Obtain length or MVA by vintage and voltage.

**Future/fallback:** explicit network expansion and renewal scenarios. REMIND
generation capacity is a demand context, not a grid commissioning history.
**Gate:** separate circuit-km, cable mass, MVA and equipment counts; electricity
delivered alone cannot date or size the network without an engineering model.

### 08. General power plants — `power_plants_general`

**Solution:** generator/unit cohorts by technology, joined to output; evolve with
native REMIND cohorts or its capacity/addition/survival model after reconciliation.
Keep hydro civil works and replacement turbines, and major thermal retrofits,
separate where the inventory permits.

**Data to use:** S04 GEM for broad large-unit coverage; S05 EIA-860/860M and EIA-923
for the US commissioning/retirement/output pilot; S07 for Swiss operating plants.
The [IAM assessment](iam-scenario-assessment.md) specifies REMIND `Cap`, `New Cap`,
technology lifetime, early-retirement and period inputs. L3 is the non-IAM prior.

**Future/fallback:** REMIND is preferred for supported scenario technologies;
IMAGE requires expanded exports. Use a clearly external stock model for other
scenarios. **Gate:** reconcile MW and MWh by technology, planned/idle/operating
status, historical retired units and inventory plant size. Preserve the native
survival rule rather than treating reported mean life as a hard cutoff.

### 09. Wind — `wind_farms_turbines`

**Solution:** commissioning cohorts weighted by generation within turbine classes;
distinguish full repowering, replacement components and retained foundations.

**Data to use:** S08 Danish turbine connection/decommissioning and output records
as the service-weighting pilot, after resolving the current release; S06 MaStR
for Germany; S04 for larger global projects. REMIND capacity cohorts provide
scenario evolution. L3 supplies alternative survival assumptions where required.

**Future/fallback:** native IAM gross additions and retirement, or a documented
installation history plus survival. Capacity weighting is a labelled interim
approximation where generation is absent. **Gate:** duplicate unit/site records,
onshore/offshore distinctions and repowering must not double-count equipment or
reset all civil works to the new turbine year.

### 10. Photovoltaics — `pv_systems`

**Solution:** module installation cohorts by rooftop/utility class; separate
inverter replacement and structures. Use vintage-specific generation when
available, otherwise a declared capacity/yield/degradation conversion.

**Data to use:** S06 MaStR as the German pilot, S07 Swiss operating units, S04 for
large-project extension and S09 IRENA as a capacity-total check. Use REMIND PV
capacity/additions/cohorts for the future and L3 for survival sensitivity.

**Future/fallback:** reconstruct from gross installations if native cohorts are
unavailable. **Gate:** AC/DC units, small-system coverage, old retirements and
unknown commissioning dates. Do not use net capacity differences as gross builds
or a stationary age distribution for a rapidly growing PV fleet.

### 11. Industrial machinery and plants — `industrial_machinery_plant`

**Solution:** split large process installations from generic machinery. Use
observed unit cohorts for the former and class-specific additions/survival or
declared stationary-survival priors for the latter.

**Data to use:** S20 GEM Iron and Steel Tracker for dated furnaces and relining;
L4 POSTED for matched process-life inputs; L5/S29 for machinery-class survival.
Use REMIND physical energy-conversion capacities only for processes that match
the supplier boundary. IAM industrial tonnes of output are not installation dates.

**Future/fallback:** capacity-driven process cohorts where capacity and utilisation
are supported; otherwise class-specific renewal with activity-growth sensitivity.
**Gate:** furnace relining, catalyst replacement, ancillary pumps and whole-plant
replacement have different dates; no single industrial-machinery profile.

### 12. Water and wastewater equipment — `water_wastewater_treatment_equipment`

**Solution:** civil facility cohorts plus independent pumps, aerators, controls,
membranes and other equipment renewals. Weight service by treated volume or load
consistent with the inventory.

**Data to use:** S14 `Water treatment facilities`, `Wastewater treatment plants`
and pump-station construction bands for civil assets. S25 EPA equipment-life
guidance and L2/L5 are provisional component priors. Operator installation and
replacement logs are the desired equipment evidence, not already available data.

**Future/fallback:** facility renewal/expansion and component cycles. A station's
construction date does not date its pump. **Gate:** use counts as a provisional
stock basis only until plant-size/treatment-volume conversion is established;
reconcile maintenance already included in the inventory.

### 13. HVAC — `hvac_systems`

**Solution:** equipment-age cohorts by technology and building/use class, not
building-age cohorts. Separate heating, cooling and water heating.

**Data to use:** S12 RECS 2020 microdata with `EQUIPAGE`, `ACEQUIPAGE`, `WHEATAGE`,
equipment type, survey/replicate weights and imputation flags; the codebook has
been inspected, the microdata still need extraction. S24 ASHRAE supports
commercial-equipment survival, not a nationally representative stock histogram.

**Future/fallback:** additions/replacements from a technology-stock scenario when
available; otherwise class-specific constant-stock renewal. **Gate:** distinguish
household prevalence from numbers of units/capacity, and fuel consumption from
delivered heat. Changing annual weather does not imply a newly installed stock.

### 14. Replaceable filters — `filters_replaceable`

**Solution:** replacement-event timing for disposable media/cartridges; model
durable filter housings separately. A generic industrial filter lifetime is not
a cartridge replacement interval.

**Data to use:** the matched inventory's maintenance specification, equipment
manuals and operator logs: filter class, replacement hours/calendar interval,
loading/pressure-drop criterion, quantity and maintenance event date. No suitable
population-age dataset or specific manual has been established for this group.

**Future/fallback:** event schedules driven by operating hours; if service is
provided by currently installed media and its age is unknown, use a declared
renewal-phase model over the supported interval. A same-year pulse is appropriate
only for an event in that year or an explicit annual-resolution approximation.
**Gate:** do not apply L5's 18.1-year `filters` machinery row to disposable media;
without a defensible interval/class, leave the match unresolved.

### 15. UV lamps and equipment — `uv_lamps_equipment`

**Solution:** lamp replacement in operating hours, separate from reactor, sleeve,
ballast and electrical equipment life. Record lamp manufacture/install time
relative to the maintenance event or existing lamp service.

**Data to use:** S26 EPA UV guidance for ageing/performance/maintenance structure;
the actual lamp model's manual and inventory maintenance assumptions for hours
and replacement criteria, plus operating-hour records when available.

**Future/fallback:** supported renewal schedule with operating-hours sensitivity;
for unknown phase use a declared stationary renewal model. **Gate:** annual
binning must conserve event amount, and lamp replacements must not be added a
second time if already included in the reactor's lifetime inventory. No generic
lamp number from a lighting-fixture dataset is approved as a UV lamp lifetime.

### 16. Cars, vans and buses — `vehicles_cars_vans_buses`

**Solution:** observed surviving first-use cohorts by class/powertrain, linked to
use; native EDGE-Transport construction-year stock and service for future REMIND
scenarios. Keep cars, vans and buses distinct.

**Data to use:** S02 UK VEH1111 as the detailed historical pilot; S01 Eurostat car
age bands for country-total validation; S28 MOT as a mileage-by-age candidate
within its covered classes; S03 for transfers/survival interpretation. S14 bus
classes can test a Canadian transit fleet. Obtain exact-run EDGE-T cohort data;
local REMIND Stock/Sales/ES are already reconciliation targets. L6 supports
survival only after separating actual lifetimes from observed fleet mean ages.

**Future/fallback:** if native cohorts cannot be obtained, reconstruct from sales,
initial stock and class-specific survival/transfers, after verifying sales-period
semantics. **Gate:** passenger-km versus vehicle-km, first use versus national
registration, imports/exports, and age-dependent mileage. MOT does not establish
all bus/HGV utilisation. Unsupported classes get separate priors, not car shares.

### 17. Heavy-duty trucks — `vehicles_trucks_heavy_duty`

**Solution:** cohorts by truck weight/class and powertrain, with kilometre/load
conversion; use corresponding EDGE-T freight cohorts for REMIND evolution.

**Data to use:** S02 surviving heavy-vehicle first-use records for the UK pilot;
local REMIND freight Stock/Sales/ES, including the inspected 40-tonne truck leaf;
exact-run vehicle survival, mileage and payload from EDGE-T. Supplement with
class-specific national freight-activity surveys when retrieved. L6 is only a
matched truck-life fallback.

**Future/fallback:** stock/sales reconstruction with class-specific survival and
transfers; declared equal use within class until measured utilisation is obtained.
**Gate:** distinguish tonnes carried from tonne-km and vehicle-km; empty running,
load factors and original cohorts across exports require explicit treatment.

### 18. Ships — `ships_vessels`

**Solution:** start with observed age bands by ship type and size/service basis;
use individual build-year histories only when data access is established.

**Data to use:** S21 UNCTAD's published age tables give the initial seagoing
benchmark. Choose count shares for comparable ship units or capacity shares with
an explicit LCI conversion; DWT is not tonne-km. L6 and original ship studies
are candidate survival sources. Do not assume licensed vessel microdata access.

**Future/fallback:** evolve age bands using survival and a fleet-demand path
converted from transport work with documented use/load. Without that conversion,
use constant-stock renewal and sensitivity. **Gate:** the local IAM audit has
not established native ship vintages; inland vessels, ferries and engines need
separate sources or priors. Registration flag is not automatically service region.

### 19. Aircraft — `aircraft`

**Solution:** operating airframe manufacture cohorts by aircraft class, separating
engine replacement and overhaul; join service where available.

**Data to use:** S22 BTS B-43 carrier inventories as the US commercial pilot,
checking actual downloadable manufacture-year/status fields; FAA registry as
cross-check/extension. Use original manufacture, not carrier acquisition year.
L6 supplies candidate lifetime evidence; airport/airline operating statistics
are an acquisition need for class-specific flights/hours/load.

**Future/fallback:** class-specific stock-demand/survival model if transport work
can be converted; otherwise constant-stock renewal with utilisation sensitivity.
**Gate:** registered versus active aircraft, parked fleets, ownership transfers
and passengers versus freight. No worldwide observed fleet is claimed from US data.

### 20. Charging infrastructure — `charging_infrastructure_equipment`

**Solution:** installation cohorts by public/private, AC/DC and power class,
with separate equipment replacement and civil/grid connection works.

**Data to use:** S23 Bundesnetzagentur public charging commissioning records as
the German pilot; obtain throughput by class where possible. Private charging
needs separate deployment/install data; neither coverage nor access is assumed.
Use verified inventory/manufacturer life as a provisional survival input.

**Future/fallback:** use a charger-deployment scenario. EV stock can drive an
explicitly calibrated charger/EV or energy/charger model, with public/private
shares and utilisation; this is external to the inspected IAM stock variables.
**Gate:** count locations, charging points and installed units consistently.
Rapid growth makes a stationary whole-fleet profile an unsuitable default.

### 21. Small compressed-air equipment — `compressed_air_equipment_small`

**Solution:** compressor-class renewal/cohort model separated from vessel, dryer,
filters and maintenance. Prefer operating-hour survival where supported.

**Data to use:** L5/S29 compressor survival as a provisional industrial-class
candidate, S27 DOE for use/maintenance context, and actual small-compressor
manuals or operator replacements for class fit. The local 20-year value is not
automatically valid for small portable equipment or a calendar replacement rule.

**Future/fallback:** matched class survival with a stationary age prior for mature
stocks; operating-hour and additions-trend sensitivity. **Gate:** distinguish
small compressors from grid-scale compressed-air energy storage, and air flow/
pressure/service hours from manufactured equipment units.

### 22. Pumps — `pumps_small_medium`

**Solution:** separate pump classes and duty; component-specific installation/
replacement histories, independent of the enclosing plant age.

**Data to use:** S24 ASHRAE for matching building-service pumps; L5/S29 for
industrial pump/vacuum-pump survival; S25 for water-system engineering priors;
operator installation and duty records when obtainable.

**Future/fallback:** stationary-survival priors within a mature, matched class,
or a component-renewal model conditional on the host and its operating hours.
**Gate:** pump body, motor and seals may renew separately. The local 18.7-year
`pumps` entry is a Japanese class estimate, not a global small-pump default;
pumped volume/head and duty are needed before service weighting.

### 23. Electrical cabinets and controls — `electrical_cabinet_control_equipment`

**Solution:** split housing/cabinet, switchgear, drives/controllers and electronics.
Use initial manufacture for the housing and component replacement/upgrade events
for parts actually replaced.

**Data to use:** L5/S29 for the matched switchgear/control/switchboard class;
S24 for selected HVAC controls; actual inventory bills of materials and component
manuals/maintenance logs to distinguish renewal fractions. S25 is a water-sector
prior only, not a universal electrical-equipment source.

**Future/fallback:** component-specific stationary survival or renewal assumptions
with explicit uncertainty. **Gate:** the local 17.7-year class entry cannot date
all electronics and housings. A software update is not new hardware production;
without a defensible split, keep a coarse declared prior or unresolved status.

## Local survival inputs to use, and their limits

Paths are relative to `dev/trails/lt_data/`. All are already present locally;
they supply assumptions or source leads, not generally stock histories.

| ID | Local input | Selected use |
|---|---|---|
| L1 | `3_LT_Buildings_CIRCOMOD.xlsx` | Building-type/region survival candidates; verify mean, Weibull parameters and source |
| L2 | `3_LT_CIRCOMOD_Infrastructure.xlsx` | Road layers/traffic classes, track components, bridges/tunnels and pipes; use only after boundary/source review |
| L3 | `3_LT_Electricity_CIRCOMOD.xlsx`, `3_LT_Electricity_Generation_Technologies_Allington_2022.xlsx`, `3_LT_Electricity_System_Technologies_Victoria_et_al_2020.xlsx` | Technology/region survival priors; exclude the percentage-valued circular-economy row and separate energy-storage from small-equipment assets |
| L4 | `3_LT_Industrial_Assets_POSTED_Database_VERPOORT_2026.xlsx` | Matched industrial-process technical-life assumptions, not vintage observations |
| L5 | `3_LT_Machinery_CIRCOMOD.xlsx` | Machinery-class survival candidates from disposal-survey research; see exact records below |
| L6 | `3_LT_Vehicles_CIRCOMOD.xlsx`, `3_LT_Vehicles_Siavash_2019.xlsx` | Trace transport-class lifetime citations; mean-age observations stay validation constraints |

The L5 `Data` sheet has the following exact records. Values are transcribed
source evidence; neither global applicability nor the original parameter mapping
has been approved. The source is Nomura and Suga (2018), S29.

| Worksheet row / record ID | Class | Value in years | `stats_array_3` / `stats_array_4` | Proposed disposition |
|---|---|---:|---|---|
| 32 / 3322805 | pumps | 18.7 | 20.9 / 1.67 | Candidate industrial pump survival |
| 33 / 3322806 | compressor | 20.0 | 22.6 / 2.21 | Candidate industrial compressor survival; small-class fit unresolved |
| 35 / 3322808 | vacuum pumps and equipment | 16.9 | 18.9 / 1.68 | Separate vacuum-pump candidate |
| 81 / 3322854 | filters | 18.1 | 20.2 / 1.70 | Do not apply to disposable filter media; machinery boundary must be established |
| 161 / 3322934 | switchgears, controlling equipment and switchboards | 17.7 | 19.7 / 1.52 | Candidate class survival, not all cabinet components |

Each record says Japan, approximately 2018, and Weibull. The two numerical
arrays look compatible with scale/shape parameters, but approval requires checking
the original definition and mean relation. The author's publication record
describes estimation from disposed assets and separates retirement observations
from sales for continued use. Thus the study is worth recovering for survival;
a geometric price-depreciation rate is not a physical survival rate. The original
PDF endpoint was inaccessible during this follow-up; exact parameter verification
and microdata access remain open. [Author's publication record][nomura]

## What to implement first

1. **UK cars and REMIND cars/trucks:** test observed versus native scenario cohorts,
   transfers, service units and common amortisation.
2. **German PV and REMIND power:** test growth, capacity units, reporting periods,
   early retirement and component replacements.
3. **Canadian pipes/roads and US bridges:** test long tails, broad age bins and
   structural versus renewal dates.
4. **RECS HVAC and one UV/filter inventory:** test survey weighting, replacement
   hours and the distinction between an existing component and a new replacement.
5. Expand the same three models to the remaining groups; obtain operator data
   where contribution sensitivity justifies it. Record labelled priors or
   unresolved rows as explicit outcomes, not falsely observed coverage.

Each implemented group produces an unnormalised cohort/event table, service
conversion, source manifest, annual weights, amount reconciliation, missing-age
and coverage report, and a future/fallback definition. Validate on a real matched
exchange and its reference product before promoting its profile. This document
selects the approach; calibrated values and consumer changes remain subsequent
work under the [implementation plan](implementation-plan.md).

[nomura]: https://k-ris.keio.ac.jp/html/100013705_ronbn_en.html
