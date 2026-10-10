# Pilot boundaries and source refinements

Working evidence, 9 October 2026. These findings refine the candidate populations
and inventory boundaries. They do not yet constitute approved temporal profiles.

## Trucks: replace the all-HGV observation with a bounded subgroup

The [DfT detailed vehicle files](https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-files)
provide `df_VEH0520.csv` with first-use year, weight band, fuel, tax class and
wheel plan. The pinned download permits a substantially closer match to a
40-tonne articulated-lorry inventory than the original all-HGV observation.
The candidate selection is United Kingdom, goods tax class, articulated,
road-using, diesel, maximum gross weight above 32 and up to 40 tonnes. Constituent
nations and Great Britain overlap the UK and must not be added.

The 2022 selection contains **14,231 vehicles**: 13,860 with annual first-use
cohorts, 370 with unknown year (2.6000%), and one in the open pre-1980 bin. The
curation retains all three quantities. It produces eleven annual observations
for 2015–2025. Although the source page describes a UK series from 2014, the
downloaded 2014 UK cells are not applicable; they are not interpreted as zero.

The subgroup resolves much of the weight/wheel-plan mismatch. It does not
identify EURO class, manufacture year, loading or mileage. A EURO-6 inventory
cannot be called an observed technology match for all these cohorts. The next
choice is an explicit representative operational-technology proxy, a compatible
aggregate service inventory, or a further technology/cohort restriction with
reported exclusions. First-use year alone must not silently stand in for EURO
class. Counts also remain different from tonne-kilometre weights.

Reproduce with `dev/stock_vintage/curate_observations.py`. The previous broad HGV
series remains in the output for comparison; the new group is
`heavy_trucks_32_40t`. There are now 43 observations across all original series
and this refinement. The acquisition manifest pins the new file under S02;
DfT/DVLA attribution and the Open Government Licence still apply.

## Power: audit output joins before creating service weights

`dev/stock_vintage/audit_power_service.py` verifies the pinned EIA-860/EIA-923
files and writes the complete candidate records, exclusions and unresolved
choices. It deliberately does not generate an approved service distribution.
The [EIA-923 source page](https://www.eia.gov/electricity/data/eia923/)
describes plant/prime-mover data and the smaller subset with generator data.

### CCGT

Five apparent missing generator matches were differences in numeric padding:
EIA-860 IDs `0001`/`0002`/`0003` versus EIA-923 numeric `1`/`2`/`3`, at plants
55210 and 56298. The audit accepts an alias only when the scoped plant identity
is one-to-one and records both original IDs. It rejects ambiguous aliases and
retains zero outputs. All selected generator IDs now match.

The initial 50,099.8 MW AC contains 120 plant/unit-code groups. Restricting to
nonblank unit codes, complete CT+CA or single-shaft CS configurations, common
commissioning year and available positive block output gives these candidates:

| Quantity | Result |
|---|---:|
| Candidate complete-block capacity | 41,476.3 MW AC |
| Candidate annual net generation | 159,002,044.49 MWh |
| Excluded: unknown unit code | 2,214.0 MW AC |
| Excluded: mixed commissioning years | 6,072.2 MW AC |
| Excluded: incomplete configuration | 337.3 MW AC |

These capacities reconcile to the initial selection. Sum component generation
once within each block before associating a whole-plant construction cohort;
do not assign a zero-output steam component zero capital independently of its
paired turbine. The current [EIA-923 instructions](https://www.eia.gov/survey/form/eia_923/instructions.pdf)
distinguish CT, CA and CS and request generator output. A zero observed CA value
is therefore a reporting/operating boundary to inspect, not proof of separate
zero-burden steam capacity or an official whole-block reporting convention.
The current instructions are contextual evidence, not the archived 2022 form.

Before promotion, compare candidates with all same-unit members in excluded,
nonoperating and retired generator records. A complete configuration in the
selected subset is not by itself proof that no members were filtered out.
Mixed-year block and component-capacity sensitivities remain necessary.

### PV

The initial 27,649.8 MW AC includes multiple cell technologies and mounting
systems. Matching the solar supplement for crystalline silicon and fixed tilt,
while excluding reported tracking and other cell materials, retains **1,844.5
MW AC across 557 generators at 412 plants**. This is a more suitable starting
population for the selected fixed multi-Si inventory than all WECC PV.

EIA's crystalline flag does not distinguish mono-Si from multi-Si, and fixed
tilt does not establish ground versus rooftop mounting. These remain technology
proxies, not observed matches. The audit retains DC capacity separately from AC
nameplate. A 570 kWp inventory is a DC reference; an AC stock trajectory needs
a documented conversion or a relative-growth convention that states its limits.

PV output is available at plant level in the inspected EIA-923 data. Plant
records distinguish sites where all currently operating PV matches the selected
technology. Before dividing output among cohorts, account for mixed installation
years, within-year entry, and retired/nonoperating units that may contribute to
the same annual plant output. Normalising the available rows alone would hide
these gaps. Compare output weights with explicit capacity/exposure fallbacks.

## Capital inventories: lifecycle and component boundaries

The following results come from read-only inspection of the local ecoinvent
3.12 cut-off inventory. Comments and full inventories remain local. Identity
codes identify the reviewed source activities; they are not runtime mappings.

| Pilot | Reviewed construction activity | Finding and action |
|---|---|---|
| PV | `9200781106b014524c285d277f0f2009` | Construction contains panels, mounting, electric installation and **3.126 units of inverter equivalent**. The inverter exchange describes a 15-year life with one replacement over the plant's 30-year design life, plus size/material scaling. Separate initial and replacement timing; do not interpret 3.126 as three observed replacement events. Disposal is embedded further down in component datasets. |
| CCGT | `409bc961971c08001fda366891c86c18` | The whole-plant inventory represents a gas/steam turbine combination, materials and construction energy. Its design reference is 180,000 operating hours (approximately 36 years at 5,000 hours/year). Reviewed direct technosphere inputs contain no explicit disposal port. Do not invent a disposal amount; distinguish a temporal scenario lifetime from the existing amortisation. |
| Water network | `31b5b1b80a306de7cb628978f285fa9b` | The kilometre-based network includes coated pipes and an allocation of tanks, construction and embedded material disposal; operation/maintenance belongs to the water-service market. Its design-life reference is 70 years. Lift reviewed disposal ports to service-year context, and disclose use of pipe construction cohorts for the composite network including tanks. |

The PV lifetime bundle requires a joint review of replacements and their own
disposal. Moving its parent construction year alone would move both embedded
replacement manufacture and disposal incorrectly. The general lifecycle rewrite
can separate selected paths, but a justified event schedule and conditional
survival still need to be supplied. It does not infer replacement intervals.

For water, some waste-material exchanges have multiple geographic suppliers;
retain every signed coefficient and exact supplier identity once. New material
inputs are construction-time inputs, while machinery used in construction can
have its own stock age. The existing annual water-service capital coefficient
must remain unchanged under common amortisation.

## Vehicle survival: a useful source with a different endpoint

[Nguyen-Tien et al. (2025)](https://www.nature.com/articles/s41560-024-01698-1)
estimate vehicle longevity from British MOT records. Their endpoint combines
scrappage and export. The often-quoted 18.4-year BEV result summarises predicted
median lifetimes; it is not automatically a Weibull mean. A fitted regional
exit curve could support regional stock modelling, but cannot directly date
physical disposal after export. Keep this distinction in the source register
and in any future reconstruction. No parameter from this paper has yet been
adopted as a physical-disposal default.

The local CIRCOMOD vehicle workbook also mixes lifetime assumptions with ACEA
mean fleet ages; its UK 2021 car and commercial-vehicle figures are mean ages,
not survival parameters. The source-review work must not recreate the original
error by renaming those values as lifetimes.


## 10 October: complete-plant PV boundary

The power audit now checks every PV member of each candidate plant against the
Operable, Retired and Canceled, and Proposed generator sheets. The conservative
subset requires only selected fixed-tilt crystalline PV, a single commissioning
year preceding 2022, positive recorded DC capacity, and nonnegative annual
plant PV generation no greater than 8,760 hours times AC capacity. Zero-output
plants remain in stock; they have zero service weight in the reference year.
Mixed-cohort plants are excluded instead of allocating their output by an
unobserved within-plant utilisation rule. Hidden/proposed/retired members are also
excluded; this deliberately narrow choice is not a claim that a proposed
addition generated electricity during 2022.

The accepted subset has 363 plants and 444 generators: 1,294.8 MW AC,
1,629.2 MW DC, and 2,049,897.43 MWh. Commissioning years span 2005–2021, and
14 accepted plants report zero generation. Mean ages are 5.6049 years using AC
capacity, 5.5971 using DC capacity, and 5.5844 using reported generation.
The 49 excluded candidate plants account for 549.7 MW AC. Exclusion reasons
in the machine report can overlap and must not be added as mutually exclusive
capacity totals. This is a subset of the earlier 1,844.5 MW candidate population,
not all WECC solar PV.

`complete_pv_membership` checks exact generator membership before accepting
plant output; its tests cover zero output, AC/DC separation, mixed dates,
reference-year starts, hidden retired members, mixed technology, missing DC and
impossible annual output. The existing CCGT audit remains unchanged in result.
These checks provide a generation-weighted starting population. Crystalline
silicon still does not identify mono/multi-Si, and fixed tilt does not prove
open-ground mounting. The selected inventory remains a declared technology
proxy. PV future cohorts and the replacement/disposal runtime checks remain open.

## 10 October: transport component quantities confirmed

Read-only inspection of the actual electric-car service comments confirms a
262 kg battery, 150,000 km vehicle life, and 100,000 km battery life. The source
battery coefficient of 0.00262 kg/km represents 1.5 packs over the assumed
vehicle life, including replacement. It is not one 393 kg initial battery. The
matching used-battery coefficient is negative with the same magnitude. Separate
initial/replacement manufacture and their disposal dates while retaining this
original total under common amortisation.

Car-body manufacture states that glider and drivetrain end-of-life burdens are
embedded in the component inventories, in addition to manual dismantling at
assembly. The earlier proof lifting dismantling alone establishes its scoped
static equivalence, not a complete car lifecycle boundary. Those component paths
must be reviewed and timed explicitly. Maintenance is a life-aggregated package
consumed per kilometre; it needs a service-year role rather than an inherited
asset-age profile.

For the selected 40-tonne lorry, the source capital and maintenance amounts are
based on 540,000 vehicle-kilometres and its average load factor. The negative
used-lorry input is vehicle end of life. The negative wastewater-from-production
input is manufacturing waste and must remain at manufacture. Genuine factory
capital remains an independently aged stock.

The actual NPi MIF contains Stock, Sales and ES leaf variables for four-wheel
BEVs and liquid-fuel 40-tonne trucks. The current premise REMIND topology places
GB in EUR. A UK pilot still uses a regional proxy; these exports do not prove
native UK cohorts. Sales has unit `million veh`, so do not transfer the
power-capacity centred-rate convention without verifying its reporting meaning.
Passenger ES is passenger-kilometres while the source car service is kilometres;
absolute conversion needs an occupancy assumption.
