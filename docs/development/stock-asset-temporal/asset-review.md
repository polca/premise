# Asset-group evidence and migration review

Status: review specification. No new empirical default values are approved by
this document. Existing lifetime evidence must be checked before reuse.

## Baseline coverage

The 23 identifiers below are the groups in
`dev/trails/stock_asset_grouped_temporal_defaults.xlsx`, sheet
`Group_Assumptions`, at the audited premise revision. They are review groups,
not a guarantee that every current stock row is classified correctly.

The packaged CSV contains 987 stock rows, including 204 rows whose activity
name contains `passenger car` case-insensitively. Do not count these as 987
independent empirical distributions or assume every row matches an exchange in
every ecoinvent version. Build a complete row/group/exchange crosswalk in P0/P2.

## Review checklist for every group

- [ ] Resolve whether each exchange uses an existing asset, new component,
      replacement, or lifecycle service.
- [ ] Define the asset boundary and unit, including size/capacity conversions.
- [ ] Record geography, observation year, technology, and source weighting basis.
- [ ] Separate observed ages from design/technical/economic/territorial lifetimes.
- [ ] Verify source cells/series and distribution parameter conventions.
- [ ] Identify measured cohorts, additions, survival, transfers and utilisation.
- [ ] Assign an evidence tier and a documented fallback where data are absent.
- [ ] Check observed stock totals and age statistics with the reconstruction.
- [ ] Specify future evolution and the service-allocation convention.
- [ ] Reconcile embedded manufacture, maintenance and disposal amounts.
- [ ] Quantify numerical, parameter and structural sensitivity.
- [ ] Approve a row disposition and versioned profile, or retain unresolved status.

## Group-specific work

The evidence column describes the data to seek or validate. Except where linked
in [references](evidence-and-references.md), it does not assert that a source has
already been retrieved or is complete.

| Existing group ID | Preferred evidence and service basis | Boundary/fallback issue to resolve |
|---|---|---|
| `buildings` | Construction cohorts by building type; floor area, occupancy and use | Separate structural shell, refurbishment and building services; do not turn a pre-1969 share into a single modal age |
| `roads_pavements` | Road construction and resurfacing records; traffic or declared infrastructure service | Separate pavement renewal from long-lived foundations; identify how the inventory already allocates infrastructure |
| `rail_infrastructure` | Track/network cohorts and renewal records; train/transport service | Separate rails, sleepers, signalling and civil works; preserve component replacement cycles |
| `bridges_tunnels_civil_structures` | Commissioning and rehabilitation cohorts; traffic/capacity basis | Repairs do not necessarily reset structural age; mean lifetime is not maximum structure age |
| `waterways_canals_harbours` | Asset registers, construction and major renewal; throughput where applicable | Avoid combining locks, dredging, harbour equipment and civil structures into one lifetime |
| `pipelines_buried_pipe_networks` | Installation cohorts by material/application; throughput or capacity | Reconcile the current workbook's roughly 20-year mode with notes referring to much older stock; distinguish replacement from repair |
| `electricity_grids_substations` | Line/transformer/substation installation cohorts; delivered electricity or declared capacity service | Separate network components and voltage levels; retain explicit geographic fallback |
| `power_plants_general` | Unit commissioning, retirement, retrofit and output records | Split by generating technology; include hydro and thermal/nuclear life extensions; do not use one generic fleet-age curve |
| `wind_farms_turbines` | Gross installations, decommissioning, repowering and generation | Separate repowering from initial site construction; use output/capacity conversions and age-related performance |
| `pv_systems` | Annual installations and retirement/performance data; electricity output | Separate modules, inverters and structures; do not infer stock age from technical lifetime in an expanding fleet |
| `industrial_machinery_plant` | Sales/installation cohorts by equipment class, operating hours and retirements | Generic stationary-survival fallback only where history is absent; do not assign plant age to all components |
| `water_wastewater_treatment_equipment` | Installed equipment and replacement histories; treated volume/load | Keep civil basins separate from pumps, aerators and controls; avoid treating whole-plant age as equipment age |
| `hvac_systems` | Installation/replacement cohorts by technology; delivered heat/cooling and operating hours | Distinguish compressors, refrigerants, distribution and building shell; account for utilisation differences |
| `filters_replaceable` | Replacement intervals, operating hours and maintenance schedules | Model as components/events where appropriate; reconcile the grouped uniform prior with later workflow instructions preferring triangular curves |
| `uv_lamps_equipment` | Replacement intervals and service hours | Separate consumable lamps from treatment equipment; annual resolution may be coarse relative to lifetime |
| `vehicles_cars_vans_buses` | Registration and surviving cohorts, used-vehicle trade, retirements and kilometres | Split cars/vans/buses and powertrains; regional stock mean age is not global lifetime; retain original cohort across trade |
| `vehicles_trucks_heavy_duty` | Class-specific registrations, trade, survival, mileage and loads | Split vehicle sizes and technologies; use vehicle/tonne-kilometres consistently with the reference product |
| `ships_vessels` | Ship registers, build/scrap years, capacity and transport work | Count-weighted and deadweight-weighted fleet age differ; choose the basis matching inventory units and service |
| `aircraft` | Fleet registers, entry/retirement, utilisation and load | Distinguish aircraft, engine replacement and overhaul; avoid assigning fleet age to new manufacturing components |
| `charging_infrastructure_equipment` | Installation and replacement cohorts; charging service | Represent deployment growth and different charger utilisation; distinguish equipment from civil connection works |
| `compressed_air_equipment_small` | Equipment installation/replacement and hours/output | Use a labelled stationary prior if data are absent; separate machinery from the supplied compressed-air service |
| `pumps_small_medium` | Installation/renewal and pumped-volume or hour data | Match actual pump class and duty; keep replacement timing separate from the enclosing plant's age |
| `electrical_cabinet_control_equipment` | Installation/upgrade/replacement and service hours | Distinguish controllers, power electronics and housings; upgrades can replace only part of the inventory |

## Evidence sources already identified

The existing workbook and review CSV point to ACEA, CIRCOMOD/IEDC, electricity
technology lifetime compilations, and infrastructure-age sources. These are
starting points; their parameter semantics must be rechecked.

- ACEA's cited EU mean age is a stock statistic, not a complete vehicle age
  histogram or survival model.
- The CIRCOMOD vehicle workbook's global private-car row gives 14.74 years
  and labels its source distribution Weibull. Its parameter text needs review;
  the value alone does not justify the normal survival assumption used in an
  illustrative comparison or the current lognormal stock profile.
- The same workbook contains national ACEA age observations alongside other
  lifetime evidence. Do not treat all numeric rows as equivalent lifetime data.
- [Global Energy Monitor's integrated tracker][gem] provides capacity,
  commissioning and retirement fields for electricity assets. Assess geographic,
  technology, size and distributed-generation coverage before adopting it.
- [Held et al.][held] establishes why national vehicle outflows and used-asset
  trade require careful interpretation. Do not replace all vehicle lifetimes
  with a regional number from its abstract.

## Review priorities

First build three contrasting pilots: passenger cars, an expanding electricity
technology such as PV, and a long-lived infrastructure asset. Add a short-lived
replacement component and a nested new-asset construction case to test roles.

Then prioritise using actual matched-exchange coverage and contribution analysis
for the studies being supported. The 317 lognormal rows need immediate numerical
auditing, but the 670 triangular rows and nested-role errors may be equally
important scientifically. Do not infer an LCA impact ranking from row counts.

## Defaults when evidence is incomplete

Retain a supported lifetime as a lifetime parameter. Choose a stationary
survival-based profile only as a declared fallback; a fixed-lifetime uniform
age profile is the minimal baseline when only mean lifetime is available.
Alternative lifetime variability and inflow trends are sensitivity cases, not
arbitrarily precise new defaults.

If only an observed mean age is available, record it and compare candidate
profiles against it after binning. Do not force it to be the mode/median, or
rescale it to a new lifetime. Mark inconsistent combinations for review.
Where service or lifetime information is insufficient for individual-lifetime
allocation, retain an explicitly labelled timing-only approximation.

For every fallback, record what evidence would replace it and which results
are sensitive to that gap. A group is complete when every row has a disposition,
not when every missing value has been replaced by a numerical guess.

[gem]: https://globalenergymonitor.org/projects/global-integrated-power-tracker
[held]: https://doi.org/10.1186/s12544-020-00464-0
