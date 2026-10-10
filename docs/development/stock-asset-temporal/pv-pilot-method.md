# PV parent stock, active inverters and lifecycle boundaries

Experimental bounded pilot, 10 October 2026. Cohorts, component separation and
the real export are implemented. Matrix equivalence and signed event-calendar
checks and full signed temporal LCI pass, including the two ambiguity endpoints.
Private evidence archiving and independent verification are complete.

## Observed population and units

Use the pinned 2022 EIA-860 and EIA-923 archives in the acquisition manifest.
`audit_power_service.py` checks membership against all three generator sheets.
The selected population consists of US-WECC plants whose PV generators all have
fixed crystalline modules, a shared commissioning year before 2022, positive AC
and DC ratings, and nonnegative annual PV generation within the AC nameplate
bound. Mixed technology/cohort plants, unexpected operating/retired/proposed
members and ambiguous joins are excluded. This conservative boundary does not
assert that a proposed member produced electricity in 2022.

The accepted population has **363 plants, 444 generators, 1,294.8 MW AC,
1,629.2 MW DC and 2,049,897.43 MWh** in 2022. Fourteen zero-output plants remain
in the stock. Their service weight is zero, rather than their installed capacity
being discarded. Cohorts span 2005–2021. The prior fixed-crystalline candidate
population contained 1,844.5 MW AC; exclusions account for 549.7 MW AC in 49
plants. Exclusion-reason capacities overlap and must not be added together.

AC, DC and observed generation are reconciled independently. Service weights
use generation by parent commissioning cohort. Reference mean ages are 5.5844
years for service, 5.6049 for AC stock and 5.5971 for DC stock. EIA's crystalline
classification does not establish multi-Si, and fixed mounting does not establish
open-ground installation. The ecoinvent 570-kWp multi-Si ground-mounted system
is an explicit technology proxy. No numerical AC/DC conversion is inferred from
the IAM's absolute capacity scale.

## Annual scenario reconstruction

`generate_pv_cohorts.py` verifies exact local REMIND file hashes and reads these
USA leaves for SSP2-NPi2025 and SSP2-PkBudg650:

| Role | Variable | Unit |
|---|---|---|
| Capacity | `Cap\|Electricity\|Solar\|+\|PV` | GW |
| Additions | `New Cap\|Electricity\|Solar\|+\|PV` | GW/yr |
| Electricity | `SE\|Electricity\|Solar\|+\|PV` | EJ/yr |
| Reported losses | `SE\|Electricity\|Curtailment\|Solar\|+\|PV` | EJ/yr |
| Lifetime | `Tech\|Electricity\|Solar\|PV\|Lifetime` | years |

Use relative capacity and electricity changes from the interpolated 2022
reference. This is a USA all-PV trend proxy for the bounded WECC population,
not an absolute geography/technology crosswalk. The inspected pinned
[secondary-energy reporter](https://github.com/pik-piam/remind2/blob/e414aecde1eb0ce334c2424367617d711374ea09/R/reportSE.R)
forms the PV electricity leaf from production minus storage losses. The separate
loss leaf must therefore not be subtracted again. The inspected file SHA-256 is
`d0780b935cff4fe91cab6773a1907efc5806df1f8e74d4cc2f37f0c4303727b7`.
This pinned reporter has not been proven to be the exact local run revision.
The extracted loss series is retained for audit, not silently reinterpreted as
another reduction in generation.

The primary parent lifetime is a **declared fixed 30-year design life**, matching
the source inventory's nominal plant life. The MIF's 30-year technology lifetime
does not itself prove hard retirement at age 30; REMIND's continuous quartic
analogue is a separate sensitivity. Initial observed survivors are not survival-
weighted again. Future closing-stock targets determine annual additions after
natural retirement. The reported-additions case instead expands five-year
centred rates; both cases disclose residuals against the other constraint.

Observed cohort utilisation persists; new cohorts receive the reference fleet's
mean utilisation. A common annual rescaling reconciles the secondary-electricity
trend without altering normalized service weights. This is an end-year portfolio
approximation, not a within-year commissioning or dispatch model. No stock-year
requires interpreting a forced service exit as disposal in the two bounded runs;
the generator refuses that inference if a future input would require it.

Both scenarios generate eight cases for every year in 2022–2030: primary,
equal-capacity weighting, parent lives 25/40 years, quartic parent depreciation,
inverter intervals 10/20 years, and reported-additions reconstruction. The shared
30-year/15-year primary remains explicit. All are timing sensitivities with the
original inventory coefficient preserved, not forecasts of changed material
requirements under alternative lifetimes.

## Component quantities and timing assumptions

The source plant contains one inverter replacement over thirty years. Its 3.126
500-kW inverter equivalents include material sizing as well as two generations;
they are not 3.126 replacement events. The active-component algorithm dates each
service year's unchanged coefficient to the then-active 15-year generation.
Parent and inverter retirement records retain their conditional chronology.
The [generic derivation](lifecycle-and-cohort-method.md#date-the-component-currently-providing-service)
and thirty-service analytical test demonstrate that another factor of one half
must not be applied.

The plant's panel parameter explicitly contains **2% lifetime replacement plus
1% handling loss**. Treating the entire 3% as a younger operating stock would be
incorrect. The exporter distinguishes installation, occasional maintenance
and disposal. Neither replacement dates nor failure-age distributions for these
small panel replacements are observed. The declared fallback attributes the
already-amortised maintenance quantity at the requested service year. It is an
expected maintenance burden, not an observed active-panel cohort distribution.

For the complete panel-manufacture coefficient, timing shares are `101/103` at
parent installation and `2/103` at service. For product disposal, shares are
`100/103` at conditional parent retirement, `2/103` at maintenance and `1/103`
at installation handling. The replacement panels' terminal disposal replaces
the failed initial panels' terminal share. Thus one nominal lifetime recovers
101 initial/handling and two maintenance manufactures, with 100 terminal, two
maintenance and one handling disposals. No coefficient or lifetime denominator
is applied again. This convention is distinct from the regular active-inverter
model, for which component generations are reconstructed explicitly.

Direct source review also establishes:

- Inverter production contains product disposal and packaging disposal. Moving
  every negative exchange to inverter retirement would misdate packaging.
- Panel production distinguishes municipal factory waste, process oil and
  wastewater from polymer streams described as including end-of-life disposal.
  Some polymer totals include manufacturing losses. Their unknown split is an
  explicit `--polymer-eol-share` timing fraction. The primary value 1 is the
  end-of-life endpoint; value 0 is the factory-waste endpoint. Neither is a
  measured partition, and endpoint sensitivity remains part of release evidence.
- Mounting-system plastic input comments identify packaging, while some matching
  waste comments call it end of life. Preserve the quantity and document this
  source inconsistency: installation is primary and
  `--mounting-packaging-at-retirement` supplies the opposite timing endpoint.
- Mounting and electrical installation include end-of-life ports; the mounting
  system also embeds land occupation. The exporter dates occupation at service,
  retains both transformation flows at installation and invents no land recovery.
- Component factories are independently used capital. They must not receive the
  blanket zero shift assigned to newly installed PV components.

Raw ecoinvent comments and inventory coefficients remain in restricted local
audit files. Exact reviewed supplier codes, input hashes and timing roles belong
in the exporter audit; they must be validated against a new database revision.
`export_pv_pilot.py` reads an exact reviewed register of 82 negative ports.
Thirty-five factory/packaging ports remain with manufacture; 47 ports are lifted
for disposal or ambiguous timing. Two component-manufacture ports and one direct
occupation port are also lifted. The complete 26,533-activity source becomes
26,596 activities through 63 scoped copies/adapters. Inverter/panel manufacture
adapters supply the scoped remainder with embedded disposal removed. All 58
internal links receive zero shifts, while independent factory capital retains
its background timing. Anchor-invariance validation includes lifted direct
biosphere quantities as well as technosphere quantities.

The actual exporter produced constant-background 2022/2025/2030 packages. The
corrected consumer passed the full matrix rewrite with maximum componentwise
relative residuals `2.61e-13` for A and `1.83e-15` for B. All 510 signed annual
port checks and 58 zero-shift links pass, including repeated/reversed service
years. The interpolation cache was created successfully. Full temporal LCI,
confirmed warm-cache load, the actual occupation date, no-interpolation and
legacy and waste-timing endpoint comparisons are complete; results are reported
below. Together they satisfy the third bounded release requirement.

## Reproduce the cohort evidence

```sh
PYTHONPATH="$PWD" python dev/stock_vintage/generate_pv_cohorts.py \
  --input-dir /path/to/downloads \
  --iam-file '/path/to/remind 3.5.2/REMIND_generic_SSP2-NPi2025.mif' \
  --scenario SSP2-NPi2025 --output /private/local/pv-npi2025-cohorts.json
python -m pytest tests/test_stock_cohorts.py tests/test_stock_pv.py \
  tests/test_stock_ccgt.py tests/test_stock_power_service.py
```

Repeat generation for SSP2-PkBudg650. Two local runs each produced 72 annual case records plus paired
inverter and parent retirement profiles. Annual stock-balance residuals are no
larger than `4.55e-13 MW AC`. Restricted IAM files and derived series remain local;
the public repository contains methods, scripts and synthetic tests.

The broader cohort/lifecycle/export suite passes 82 tests, including nested
component separation, panel quantity partition, land occupation and anchor
invariance. TRAILS has 89 passing focused profile/verifier tests, including
positive/negative direct occupation dates with caller losses. In TRAILS,
`dev/stock_vintage/report_pv_sensitivity.py`
accepts the two local cohort JSON files and an `--output-dir`. It writes a
144-row CSV, input-hash manifest and PNG/SVG/PDF figure distinguishing observed
parent stock from modelled inverter dates. These derived artifacts remain local.

```sh
# premise environment; keep outputs outside repositories
PYTHONPATH="$PWD" python dev/stock_vintage/export_pv_pilot.py \
  --inventory /private/local/ecoinvent-3.12-cutoff-local.json.gz \
  --cohorts /private/local/pv-npi2025-cohorts.json \
  --output-dir /private/local/pv-real-primary

# TRAILS environment
python dev/stock_vintage/check_pv_pilot.py /private/local/pv-real-primary \
  --cache --output /private/local/pv-real-primary/checks-warm-2023.json
```

## Completed full-LCI checks, 10 October 2026

The primary corrected package now passes full LCI for demand +1 and −2 in both
2023 (annual interpolation, confirmed warm-cache reload) and 2030 (no
interpolation). All 9,847 biosphere rows agree with the independent year-wise
reference; maximum absolute error is `1.78e-14`, and the largest componentwise
operator residual is `2.57e-16`. These figures use the same UMFPACK backend and
complement the complete rewrite identity, rather than asserting independent
numerical conditioning across different solvers.

The direct land-occupation adapter returns `0.026653982919539 m²·year/kWh`
exactly at the requested service year, with sign reversal and scaling for
negative demand. Capital-attributed fossil CO2 summed over all revised roles
is `0.008559121266763 kg/kWh`, unchanged within numerical tolerance. Its role
split and calendar distributions are retained in the local JSON evidence.

For the 2030 service, corrected parent construction has mean age 5.69750 years.
Legacy raw construction has mean age 13.50179; the old no-interpolation calendar
clamp reduces its reported mean to 7.31900. The legacy negative-reference sign
error is a separate operator-conservation failure and is not attributed to
stock weighting. The checked local reports are `checks-warm-2023.json`,
`checks-2030.json` and `checks-legacy-2030.json` in `pv-real-primary`.
Both timing endpoints now have actual exports and full 2030 LCI checks for
positive and negative demand. All 9,847 flows agree with the independent
reference (maximum absolute error `2.67e-14`); the largest operator relative
residual is `2.57e-16`. Every role's total is unchanged, and unrelated roles keep
their calendars. The mixed-polymer fossil-CO2 burden is `6.03418e-6 kg/kWh`;
its mean emission year moves from 2053.54 to 2024.41 when the ambiguous flow is
assigned to manufacture. Mounting packaging retains `1.48751e-8 kg/kWh` while
its mean moves from 2024.30 to 2054.30 when assigned to retirement. These means
include upstream processes and do not claim measured disposal dates.

TRAILS `report_pv_endpoints.py` checks those invariants and writes a 590-row
calendar table, hash-pinned summary and PNG/SVG/PDF figure. The figure was
inspected. Inputs are the three `checks-2030.json` reports under
`pv-real-primary`, `pv-polymer-manufacture` and `pv-mounting-eol`.
This resolves the three requirements for the bounded constant-technology PV
pilot, including its explicit timing ambiguity. It does not validate native IAM
vintages, historical technologies or a public stock dataset. Final campaign
review and private evidence archiving are complete.
