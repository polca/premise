# Passenger-BEV and articulated-truck vintage reconstruction

Updated 10 October 2026. These are bounded, opt-in UK timing pilots with EUR
scenario-growth proxies. The annual generators are implemented; actual vehicle
inventory export and consumer validation remain outstanding. Neither pilot is
promoted. See [implementation status](implementation-status.md).

## Populations, dates and service

The reference date is 31 December 2022. DfT S02 source bytes are pinned in the
[acquisition manifest](acquisition-manifest.csv). The curation scripts read
numeric ODS cells, including values displayed as low counts, and distinguish
unknown first-use dates from zero counts. First use proxies parent manufacture;
it does not establish battery installation, conversion, import or refurbishment.

| Pilot | Primary observed population | Missingness and scope |
|---|---|---|
| Passenger BEV | 628,318 UK BEVs with known first-use years 2010–2022 | 99.8941% of 628,984 registered BEVs. The 309 pre-2010 records and 357 unknown dates are explicit exclusions. Old first use can precede conversion; it cannot date today's battery. Sensitivities add unknown dates proportionally or at the earliest/latest included year. Those broaden the bounded population and retain the exclusions explicitly. |
| Heavy trucks | 14,231 UK road-using articulated diesel goods vehicles, with 32 < maximum gross weight ≤ 40 tonnes | All selected counts are retained: 370 unknown dates are distributed proportionally in the primary case; one pre-1980 vehicle is distributed uniformly over 1950–1979. Unknown-date endpoints and a 1900 lower-bound sensitivity remain separate. Counts reconcile selected source rows, not an independent published subgroup total. |

Primary service shares are vehicle-count shares. Observed age-specific mileage,
occupancy and freight loading are unavailable for these exact subgroups. The
primary result therefore represents service from an end-year stock snapshot,
not reconstructed exposure throughout that calendar year. This matters for the
rapidly expanding BEV fleet: 262,412 of the selected cars first entered use in
2022. A half-year weight for current-year entrants changes the reference mean
age from 1.24249 to 1.57043 years; an assumed exponential utilisation decline
with a ten-year scale gives 1.00195 years. Neither is measured mileage evidence.
For trucks, the corresponding means are 4.96014, 5.19436 and 3.71691 years.

## Future stocks and exact source semantics

The reviewed local REMIND 3.5.2 SSP2-NPi2025 and SSP2-PkBudg650 files provide
these exact EUR leaves, with prefixes `Stock`, `Sales` and `ES`:

- `Transport|Pass|Road|LDV|Four Wheelers|BEV`;
- `Transport|Freight|Road|Heavy|Truck(40t)|Liquids`.

Stock and Sales are reported in `million veh`; ES uses `billion pkm/yr` or
`billion tkm/yr`. The generator checks file hashes, model, scenario, region,
units, unique leaves, every required anchor and finite nonnegative values.
Missing rows/cells fail instead of becoming zero. It also checks the repository
mapping of GB to EUR and records that mapping's hash. EUR liquid-fuel 40-tonne
truck trends are a proxy for the selected UK diesel subgroup.

At each annual year, interpolate the stock level and scale it to the observed
2022 population. Conditional survival evolves the existing cohorts. In the
primary case, entrants close the stock balance; any target below survivors
requires an explicitly labelled, proportional territorial exit. The alternative
sales-constrained case instead uses interpolated reported entrants and retains
the resulting stock discrepancy. Both stock and sales residuals are reported.
The macroseries cannot generally impose both constraints under this survival
prior. Primary local runs need no forced territorial exits through 2030, but the
implementation and analytical tests cover contraction requiring them.

The pinned [reporttransport sales routine](https://github.com/pik-piam/reporttransport/blob/eb4046a859b25363eb8460ced06561010cb4653d/R/reportTransportVarSet.R)
selects the construction-year cohort whose date equals the reporting year before
reporting interpolation. Sales therefore represents calendar-year entrants in
that inspected code. We interpolate these sampled annual counts; we do **not**
apply the power-sector centred five-year additions expansion. The inspected
[reporting entry point](https://github.com/pik-piam/reporttransport/blob/eb4046a859b25363eb8460ced06561010cb4653d/R/reportEdgeTransport.R)
can harmonise service demand to REMIND separately from stock. ES/Stock is thus
not proof of a native cohort-specific utilisation history. The exact reporter
revision used for the local scenarios has not been established.

ES relative change rescales a service **index**, normalised to one per vehicle
at the reference date. A common scaling factor reconciles the index total and
cancels from normalised cohort weights. No pkm-to-vehicle-km conversion is
invented, and no measured UK freight-service total is claimed.

## Survival and disposal are separate assumptions

The pinned [EDGE default table](https://github.com/pik-piam/edgeTransport/blob/1124c9967e9644b41977c4750640629d51b5494b/inst/extdata/genParAnnuityCalc.csv)
has `serviceLife` 20 for LDV four-wheelers and 15 for heavy trucks. The
[annual kernel](https://github.com/pik-piam/edgeTransport/blob/1124c9967e9644b41977c4750640629d51b5494b/R/toolCalculateVehicleDepreciationFactors.R)
is:

```text
S(0) = 1
S(a) = 1 - ((a - 0.5) / L)^4, for integer ages 1 <= a <= L
S(a) = 0, for a > L
```

`L` is the maximum included age, **not the mean life**, and is distinct from the
power-capacity `1.25 L` convention. The independent Python implementation uses
conditional ratios S(a+d)/S(a). Observed survivors are not multiplied by S(a)
again. Already observed vehicles beyond support receive an explicitly assumed
exponential residual life of three years; one- and seven-year cases bound it.
The primary L values are varied by ±5 years. These are model reconstruction
priors, not empirical physical-life fits.

The native [fleet routine](https://github.com/pik-piam/edgeTransport/blob/1124c9967e9644b41977c4750640629d51b5494b/R/toolCalculateFleetComposition.R)
works with service cohorts and mileage/loading assumptions, includes its own
initialisation and early-retirement logic, and can change service per vehicle
over time. Native construction-year outputs and exact local-run parameters
were not found in the audited scenario exports. This reconstruction therefore
does not claim exact reproduction of EDGE vehicle numbers or vintages.

Territorial disappearance is not automatically physical scrapping. For every
requested service year, the serving cohorts seed a **separate conditional
physical-retirement projection**, with no future regional stock targets. It
borrows the same annual kernel as a declared physical-life proxy. Disposal is
at that assumed retirement in the primary case, with a five-year delay case.
The code protects this distinction even when future territorial exits occur.
Exports, storage and dismantling delays are not observed by the stock table.

## Inventory boundaries and retained amortisation

These exact ecoinvent 3.12 cut-off callers are the planned integration targets:

| Caller / port | Original quantity | Required timing role |
|---|---|---|
| GLO electric passenger-car transport, per vehicle-km | 0.00612146666666667 kg car without battery/km | Parent first-use composition; source mass 918.22 kg and 150,000 km common amortisation remain |
| LiMn2O4 battery input and used-battery output | +0.00262 and −0.00262 kg/km | Active battery manufacture and conditional replacement/parent end, respectively |
| BEV maintenance without battery | 1/150,000 maintenance units/km | Expected maintenance in the requested service year, with its maintenance waste; no extra lifetime spread |
| RER >32-tonne diesel EURO6 freight transport | 9.65e-8 units of 40-tonne lorry/tkm | Parent composition; the EURO6 operating inventory is an explicit technology proxy for capital timing, not an observed EURO6 stock |
| 40-tonne lorry maintenance | 9.65e-8 maintenance units/tkm | Requested service-year maintenance; original lifetime package retained |

The BEV inventory uses a 262 kg battery, 150,000 km car life and 100,000 km
battery life. Its coefficient already contains **1.5 equivalent packs**, not a
single 393 kg pack. Component timing must not multiply that coefficient by a
second replacement factor.

Without observed battery histories or annual mileage, the primary timing proxy
uses a separately assumed nominal 20-year horizon and the inventory mileage
ratio: replacement interval = 20 × 100,000/150,000 = 13⅓ years. This is neither
an observed calendar life nor the mean of the EDGE survival curve. Ten- and
twenty-year intervals are explicit sensitivities. Manufacture is the last
scheduled installation at or before service; component exit is the earlier of
next replacement and conditional parent retirement. Fractional milestones round
up to the annual end-year. Joint events preserve the pairing. The original
coefficient is attributed across those dates. With a fractional equivalent-pack
coefficient, this convention is **not a forecast of physical replacement counts**
and does not establish consistency of a material-demand model.

Source boundary review identified embedded used-glider and used-powertrain
outputs as well as manual dismantling. All must be separated from manufacture;
lifting only manual dismantling would leave disposal at the construction date.
Glider production scraps remain manufacturing waste. For trucks, the used-lorry
port is end of life, while manufacturing wastewater stays at manufacture.
Factory capital and road infrastructure keep their independent background roles.
The source vehicle and battery technologies are older than the observed fleet;
these pilots validate timing and conservation with constant technology at the
anchors, not contemporary average BEV technology or a complete prospective LCA.

## Reproduction and evidence

From the premise implementation worktree, use its supported Python environment:

```sh
python dev/stock_vintage/acquire_sources.py \
  --input-dir /private/tmp/stock-data-campaign --source-ids S02 S30 S31
python dev/stock_vintage/generate_vehicle_cohorts.py \
  --input-dir /private/tmp/stock-data-campaign \
  --iam-file '/local/scenarios/remind 3.5.2/REMIND_generic_SSP2-NPi2025.mif' \
  --scenario SSP2-NPi2025 --group passenger_bev \
  --output /private/tmp/stock-vintage-pilots/bev-npi2025-cohorts.json
python -m pytest tests/test_stock_cohorts.py tests/test_stock_vehicles.py \
  tests/test_stock_pv.py tests/test_stock_ccgt.py -q
```

Repeat for `heavy_trucks_32_40t` and `SSP2-PkBudg650`. The four local outputs
contain 468 case/year records: 14 BEV cases and 12 truck cases per scenario,
nine years each. Maximum absolute stock-balance residuals are `4.66e-10` vehicles
for cars and `1.82e-12` for trucks. The 61 focused tests pass, covering source
identity, missingness, annual sales semantics, stock/sales discrepancies,
territorial versus physical exit, service weighting, active-component chronology
and preserved common coefficients.

DfT observations use their published open-data terms. S30 code is GPL-3 and
S31 code LGPL-3 in their pinned DESCRIPTION files; the repository commits source
addresses/hashes, mathematical definitions and an independent implementation,
not copies of those R files. Restricted ecoinvent and IAM raw/derived data and
generated packages stay local. These checks establish the reconstruction layer;
the real producer/consumer release requirement remains open.
