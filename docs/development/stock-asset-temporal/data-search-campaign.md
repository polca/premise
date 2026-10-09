# Data search campaign for stock-asset temporal profiles

Status: research plan with an initial source audit, 9 October 2026.
Branch: `plan/stock-asset-temporal-distributions`.
This operationalises P2 of the [implementation plan](implementation-plan.md).
No new temporal defaults are approved here.

## Recommendation

Build a small set of reproducible **observed stock-cohort datasets** first,
then expand coverage by asset, region and technology. Retain the existing
lifetime collection as a source of survival assumptions and references after
checking what each entry measures. It cannot supply the missing stock histories
by itself.

The subsequent [audit of the actual IAM scenarios](iam-scenario-assessment.md)
also identifies a high-priority **native REMIND cohort route**. Its local MIFs
contain stock-building inputs; upstream EDGE-Transport already calculates
construction-year stock and service contributions. Run that acquisition and
reconciliation pilot alongside the observed-data pilots below. IMAGE needs a
richer export than the files currently available.

Start with UK road vehicles, German PV, and Canadian infrastructure construction
cohorts. Add Danish wind as a candidate for linking commissioning, retirement
and actual output, plus a replacement-component case. These pilots exercise
different histories and boundaries; they are not a ranking of LCA importance.
Use measured exchange coverage and contribution sensitivity to order subsequent
work. A global empirical profile for every small component is unlikely to be a
reasonable first-release target.

The [solutions by group](group-solutions.md) select a modelling route, primary
data, future evolution and fallback for each of the 23 groups, including exact
local machinery-survival leads. Use these recipes to start implementation.

The companion [source register](data-source-register.md) records 29 source
families, their access status, relevant fields and limitations. The
[local inventory](local-source-inventory.csv) records all 20 inspected workbooks
with row counts, hashes, scope and declared licence metadata. The
[acquisition manifest](acquisition-manifest.csv) distinguishes inspected files
from sources identified through documentation.

## What we already have

| Resource | Verified finding | Appropriate use |
|---|---|---|
| `dev/trails/lt_data/*.xlsx` | 20 workbooks; 1,794 source rows: 1,742 in years and 52 in percent. The latter comprise 51 asphalt recycling-yield rows and one electricity circular-economy assumption. Five asphalt rows have no numerical value | Lifetime/source discovery, after separating observations, estimates and assumptions; exclude percentage-valued assumptions and recovery yields from lifetime fitting |
| CIRCOMOD vehicles | 128 rows mixing lifetime evidence and fleet mean-age observations; the global 14.74-year entry has ambiguous distribution text | Trace original sources; keep mean stock age separate from survival parameters |
| CIRCOMOD buildings/infrastructure/machinery/electricity | Respectively 232/121/180/72 rows, covering broad assets and selected regions; the electricity file has 71 year-valued rows and one percentage-valued assumption | Component lifetimes and survival candidates after unit checks; original empirical studies may contain more useful data than these extracts |
| Electricity compilations, Allington/Victoria | 52/36 lifetime rows | Engineering priors; no commissioning histories in these extracts |
| POSTED extract | 34 rows of industrial technology lifetime inputs | Process-specific priors; not observations of installed machinery age |
| Norwegian appliance estimates/literature | 258/53 rows | A useful route back to stock-flow research. Uncertainty in an estimated mean is not the spread of individual lifetimes |
| Grouped defaults workbook | 23 review groups | Classification starting point; the plotted curves are assumptions, not observations |
| `stock_asset_review_bw25_with_iedc.csv` | 1,004 review rows; 578 labelled `NO_GOOD_IEDC_MATCH`; 680 without ecoinvent lifetime evidence; 313 direct ecoinvent matches and 11 neighbour inferences | Search leads and inventory-amortisation audit, not evidence of stock-cohort coverage |
| Packaged temporal table | 987 stock rows | Migration target. Reconcile against the 1,004-row review before reporting coverage percentages |
| premise lifetime YAML, inventory workbooks, transport load factors and PV files | Existing model assumptions and service/conversion inputs | Audit embedded lifetime-service denominators and units. Do not count them as independent empirical vintage evidence |
| Actual IAM scenario exports | All 8 IMAGE 3.4 and 12 REMIND 3.5.2 files audited; 21 other model exports screened | REMIND stocks/sales/capacity/additions/lifetimes support reconstruction; seek native cohorts. The other inspected reduced exports do not provide equivalent inputs |

The local files are IEDC-style extracts. Repeated technologies, regions, years,
and citations are not independent studies. Build a citation-level deduplication
map. In particular, do not average assorted lifetime, mean-age and economic-life
values merely because a text matcher selected them.

Twelve workbook metadata sheets say their licence is unknown or unspecified;
four declare CC BY 4.0, two CC BY ND 4.0, one MIT and one CC BY SA 4.0. These are
transcribed metadata, not a determination of downstream reuse rights. Keep
source-level terms and derived-output permissions separate before packaging
new data. Existing local possession does not establish permission to redistribute
source workbooks.

## Data required for each modelling layer

| Layer | Minimum evidence to seek | What remains an assumption |
|---|---|---|
| Current stock composition | Surviving stock by original installation/manufacture cohort, technology, service region and observation date; count/area/capacity units | Splitting broad age bands into annual bins; unknown cohorts; mapping manufacture to commissioning |
| Stock reconstruction and evolution | Gross new installations, retirements, initial stock, transfers and survival, preserving original cohort | Retirement law and future additions where not observed |
| Current service mix | Output or utilisation by cohort/class, compatible with the reference product | Equal use if only counts/capacity are available |
| Production attribution | Existing inventory amount and amortisation basis; common lifetime service for the baseline | Individual lifetime-service allocation unless sufficient joint history is available |
| Maintenance and end of life | Component replacement intervals/hours, repair versus replacement, retirement and disposal destinations | Conditional remaining life and lifecycle scheduling where not observed |

An observed surviving-cohort table can directly determine the stock profile at
its observation date: **do not multiply it by survival again**. Conversely, a
sales history requires survival and transfers to become a stock. A mean age is
a validation constraint, not a stock distribution; a mean lifetime supplies
neither historic installations nor a unique survival curve.

The baseline does not require heterogeneous lifetimes. With the same total
lifetime service and equal current utilisation, stock shares also give
manufacturing attribution shares. Gather utilisation and amortisation evidence
to assess that assumption, while keeping the optional heterogeneous extension
off the critical path. A cross-sectional registry cannot establish complete
realised lifetime service for assets still operating.

## Feasibility across all 23 groups

Ratings concern the prospect of obtaining an adequate **regional stock profile**,
not the ease of finding a lifetime number. Low difficulty is relative and does
not imply worldwide coverage. Source IDs resolve in the source register.

| Group ID | Difficulty and best starting evidence | Main gap and first-release disposition |
|---|---|---|
| `buildings` | Low–medium for residential age bands: S10, S11, S13 | Non-residential/industrial shells, floor-area weights and demolition histories are less consistent. Split shell from renovations and equipment; retain age-band uncertainty |
| `roads_pavements` | Medium–high: S14 construction periods; authority resurfacing registers to seek | Road opening is not pavement age. Separate structural base and wearing course; use a stated renewal-cycle prior for surfacing if renewal histories are unavailable |
| `rail_infrastructure` | High: S18; operator component registers to request if needed | Public route length and rolling-stock mean age do not describe rails/sleepers/ballast. Audit component dates and spot replacement; use component priors if access fails |
| `bridges_tunnels_civil_structures` | Medium: S15 for US bridges, S14 for Canadian structures | Different structural units and rehabilitation definitions; tunnels less covered. No rows assigned to this group in the old review CSV: verify mapping before collecting at scale |
| `waterways_canals_harbours` | High: S19 for locks, then port authority registers | Lock evidence does not cover harbour cranes, quay walls or dredging. Split these; regional civil-stock proxies only where boundaries match |
| `pipelines_buried_pipe_networks` | Medium regionally: S16 energy pipelines, S14 water/sewer networks | Material, diameter, lining and replacement history; poor global coverage. Keep energy and water applications separate |
| `electricity_grids_substations` | Medium–high: S17 regulatory age-profile submissions | Templates prove that data are collected, not that completed submissions are public. Obtain component-specific profiles; no single grid-age distribution |
| `power_plants_general` | Low–medium: S04, S05, S07 | Small/captive units, unknown start years, retrofits and output joins. Split technology, unit and plant; quantify missing capacity before normalising |
| `wind_farms_turbines` | Low–medium: S08, S06, S04 | Repowering and replacement components; capacity versus generation weights. Preserve original and repowering events separately |
| `pv_systems` | Low–medium in registry countries: S06, S07; S04/S09 for checks | Rooftop coverage, MWac versus MWdc, inverter replacement and historic retirements. Start nationally; do not apply utility-scale ages to all PV |
| `industrial_machinery_plant` | High overall; medium for large steel furnaces using S20 | POSTED and local machinery papers supply lifetime leads, not universal installed cohorts. Recover selected original datasets; use process-specific priors elsewhere |
| `water_wastewater_treatment_equipment` | High for equipment; medium for whole facilities: S14, S25 | Plant construction dates do not date aerators/pumps/controls. Seek operator replacement records; keep civil works and equipment separate |
| `hvac_systems` | Medium: S12 observed US age bands; S24 survival samples | Residential versus commercial systems, replacement versus building age, and climate/use differences. Survey weights and equipment class matter |
| `filters_replaceable` | Very high for population ages; medium for maintenance schedules | Seek model-specific service intervals and operator logs. Usually resolve the exchange as a replacement event before pursuing a fleet histogram |
| `uv_lamps_equipment` | Very high for stock ages; medium for maintenance evidence: S26 | Lamp hours/output and ballast/reactor lives differ. Use event timing and operating-hours uncertainty; annual resolution may suffice only as a documented approximation |
| `vehicles_cars_vans_buses` | Low–medium in Europe: S01/S02; S03/S28 for transfer/use gaps | Split classes/powertrains. Imported vehicles and unknown first-use years require separate treatment; global transfer and scrappage histories remain difficult |
| `vehicles_trucks_heavy_duty` | Medium: S02, then national heavy-vehicle tables | Class/weight/load/mileage and exports. Do not borrow passenger-car survival without a labelled sensitivity case |
| `ships_vessels` | Medium for global age bands: S21; high for vessel histories | Counts, DWT and tonne-km differ. Inland/small vessels and individual scrapping/utilisation may need different or commercial sources |
| `aircraft` | Medium in the US: S22; high for a complete global operating fleet | Registered versus operating aircraft, transfer dates, engines and utilisation. Start with carrier airframes; separate engines and overhauls |
| `charging_infrastructure_equipment` | Medium for German public charging: S23 | Home/workplace chargers, retired units, hardware replacements and throughput. Rapid growth rules out an unexamined stationary prior |
| `compressed_air_equipment_small` | High–very high: S27 operating-context leads, local machinery sources | Industrial system studies may poorly match small compressors. Seek class-specific hours and replacements; otherwise explicitly scoped lifetime/history scenarios |
| `pumps_small_medium` | High: S24 for some HVAC pumps, S25 for water-system priors | Duty, size, motor/pump replacement and sector mismatch. No generic national pump-age evidence established by this search |
| `electrical_cabinet_control_equipment` | Very high: S24 selected controls; S25 component priors | Cabinet, drives, controllers and electronics have different renewal histories. Model identifiable replacements; retain unresolved/proxy status for the remainder |

The hardest groups are mainly **equipment nested inside larger assets**. More
plant construction dates will not resolve their component vintages. The useful
next evidence is a maintenance register, replacement survey or defensible event
schedule, rather than another generic engineering lifetime table.

## Geographic and historical coverage

1. Establish reproducible pilots where primary data are accessible: UK/EU
   vehicles, Germany/Switzerland PV, Denmark wind, Canada/US infrastructure and
   US HVAC. These are methodological test cases, not global defaults.
2. Map country records to each supported IAM regional definition, versioning the
   crosswalk. Keep service location separate from manufacturing location and
   registration flag. Do not treat `RoW` as a country with an observed fleet.
3. Extend first to the countries contributing most service/capacity to the
   target studies. Include a non-European, non-North-American test. China,
   India, Japan, Brazil and South Africa are candidate searches, selected by
   actual model coverage; this audit has not verified equivalent open cohort
   files for each of them.
4. Use GEM for wider large-asset coverage and official regional totals to expose
   missing stock. Transfer a profile across countries only with an explicit
   justification and sensitivity test; do not average country histograms equally.
5. Define historic coverage from the oldest relevant surviving cohort and the
   modelled service years, not a universal 1990 start. Preserve oldest open bins,
   earlier construction dates and unknown shares. A recent snapshot omits assets
   retired before that snapshot and cannot alone reconstruct historical stocks.
6. For future years, age the initial cohorts and add scenario-consistent gross
   installations and retirements. Net capacity change or IAM energy production
   alone is insufficient. Reconcile stock balance, class conversions and
   utilisation; keep future scenarios distinct from observations.

## Search and acquisition procedure

For each group, the evidence curator follows this sequence:

1. Resolve the exchange role and asset boundary before searching. Record the
   service unit, observation years, region, technology and required weighting.
2. Search official statistical tables and asset registries for `stock by age`,
   `year built`, `commissioning`, `installation cohort`, `retired units`, or
   `asset age profile`, with local-language equivalents. Search replacement
   components by `maintenance interval`, `operating hours` and `replacement log`.
3. Retrieve the data dictionary before a large bulk download. Check whether a
   date is original manufacture, first use, national registration, retrofit,
   last update or planned commissioning. Reject the wrong date as a vintage.
4. Follow local workbook citations back to original papers and supplements.
   Record study design, samples, censoring, transfer treatment, parameter order
   and distinction between lifetime variability and estimation uncertainty.
5. Freeze the raw release with retrieval date, URL/version and SHA-256. Retain
   units, quality flags and missing categories; document access/redistribution
   terms and keep restricted raw files outside distributable packages.
6. Build one minimal extraction and reproduce a published total. Compare an
   independent total where available; report share with known cohort and share
   represented in the target geography/class. Only then scale extraction.
7. Record failed searches, inaccessible tables and excluded sources, including
   the reason. A blank result is not proof that no data exist.

Search logs should include `group_id`, query, provider, date, URL, inspected
file/table, evidence type, coverage, access status, missing fields, decision,
next action and curator. The source register is the initial screening log;
future retrievals must also record exact release identifiers and raw hashes.

## Acceptance rules for stock profiles

- Preserve source bins and totals in the curated observations. If annual weights
  are required, label the within-bin rule as an assumption, compare at least two
  plausible rules where timing is sensitive, and retain the open-tail policy.
- Do not normalise away unknown vintages, omitted small assets, or unreported
  countries. Report known/unknown coverage and propagate an explicit residual
  profile or retain unresolved status.
- Validate survey weights, rounding, suppression, imputation and changing
  geographical boundaries. Do not merge UK and GB series across a boundary
  change without reconciliation.
- Distinguish new installations from used imports and exits from destruction.
  Preserve manufacture cohort across ownership/flag/country changes. Record
  temporary nonoperation separately when it changes the service population.
- Separate original construction from partial renewal. Match the dated physical
  component to the ecoinvent supplier and the caller's service boundary.
- Use capacity weighting only as a declared approximation to service when output
  is missing. Area, traffic, load and degradation conversions need their own
  provenance. Energy consumption is not delivered heat without conversion.
- For survival fits, account for censoring and left truncation; a current-stock
  sample overrepresents survivors. Do not infer a retirement distribution by
  treating all currently operating assets as if they had completed their lives.
- Reproduce stock balance for dynamic reconstructions and retain data revisions
  separately from physical additions/retirements. Reject implausible cohort
  gains/losses before fitting a distribution to them.

No universal missing-data percentage is declared acceptable here. At the first
pilot review, specify domain-specific thresholds and result-sensitivity limits
before promoting profiles. A high known-vintage share can still conceal the
most relevant technology or service class.

## Work programme and deliverables

Effort below is a planning estimate for one analyst with model-review support,
not an estimate for complete worldwide coverage or runtime implementation.

| Work package | Initial effort | Concrete output and decision |
|---|---|---|
| C0: reconcile local evidence and exchange scope | 2–3 person-days | 987-row migration crosswalk; deduplicated lifetime/stock claims; role and unit audit; model-impact priorities |
| C1: retrieve and curate three stock pilots | 5–8 person-days | UK vehicles, German PV, Canadian civil infrastructure; raw manifests, dictionaries, cohort tables, missingness and total checks |
| C2: survival/service and component pilots | 3–5 person-days | Danish wind history/output assessment; RECS/ASHRAE contrast; one filter or lamp replacement case; amortisation decisions |
| C3: broaden regions and technologies | 5–10 person-days initially | At least one non-European/non-North-American validation, power coverage assessment, and explicit dispositions for all 23 groups |
| C4: hard-gap review and release evidence | 3–5 person-days | Bounded operator-data requests if authorised, documented priors, sensitivity priorities, distribution rights and audit report |

The initial programme is approximately **18–31 person-days**. Re-estimate after
C1; data cleaning, access and matching are the main uncertainties. Do not promise
23 globally observed profiles within that effort.

Add **4–7 person-days** for the REMIND cohort adapter/feasibility pilot described
in the IAM assessment: approximately **22–38 person-days** for the expanded
initial programme. This estimate excludes full runtime integration and waiting
for external model exports. Begin the exact-run metadata/cohort request and
preservation of unfiltered Stock/Cap fields during C0; let the external pilot
data validate the model's historical initialisation.

First ten working days: use days 1–2 for scope/crosswalks and source dictionaries;
days 3–5 for the vehicle and PV pilots; days 6–8 for infrastructure and total
checks; days 9–10 for residual/tail review and the decision on expansion. C2 can
start only as those tasks allow; ten days is an initial review point, not full
completion. The model reviewer checks asset units, service weighting and existing
amortisation before a curator's dataset becomes a profile default.

Suggested pilot completion criteria:

- Vehicles: original first-use cohorts by class/powertrain and year; unknown and
  imported categories retained; stock totals reproduced; transfer limitations
  documented; equal-use baseline and mileage sensitivity identified.
- PV: operational units by commissioning year and size; MW basis consistent;
  coverage against national totals; missing cohorts and inverter replacements
  separated; past decommissioning limitations stated.
- Infrastructure: source construction bands retained with the correct count,
  length or area unit; no mixing of pavement resurfacing and road opening;
  residual/oldest-bin treatments reviewed.
- Component: caller role and replacement interval established; no inherited age
  from the enclosing plant; existing maintenance amount reconciled.

## Stopping rules and fallbacks

Limit the initial search for a hard group to one analyst-day after checking
official registries, the local original references and two relevant operator or
manufacturer sources. Extend it when impact sensitivity or a concrete access
lead justifies the effort. Do not let small-component data hunting block the
observed-stock pilots.

Use these dispositions, consistent with the strategy's evidence hierarchy:

1. Observed cohorts, preserving bin/missing-data uncertainty.
2. Reconstructed cohorts from gross inflows, survival and transfers.
3. A documented stationary-survival approximation where stationarity is plausible.
4. A fixed-lifetime/equal-use stationary baseline only where the lifetime is
   defensible, with inflow-trend and lifetime sensitivity. This is an assumption,
   not a recovered empirical fleet.
5. An explicit maintenance/replacement event where that is the exchange role.
6. Unresolved, when neither the role nor the evidence supports a defensible profile.

For rapidly expanding assets, a stationary fallback needs particular scrutiny:
use an additions-history scenario if available and expose the uncertainty.
Do not infer heterogeneity solely to justify reweighting stock shares. Do not
invent an empirical distribution to make the coverage checklist look complete.

Potential contacts include the original CIRCOMOD/IEDC source authors, registry
statisticians, utilities and equipment owners. Ask for original-cohort counts,
observation dates, replacements versus repairs, exits versus transfers, output
and permitted reuse. No contacts, purchases or new subscriptions were made in
this audit; paid fleet/register access is not assumed to be available at PSI.

## Audit boundary

The source search and local-file inspection are complete for this planning
revision. They establish promising routes and material gaps, not validated
worldwide profiles. There is no full asset acquisition, survival fit, profile
export, database rebuild or LCA recalculation in this change. Future acceptance
requires the [validation and rollout](validation-and-rollout.md) checks.
