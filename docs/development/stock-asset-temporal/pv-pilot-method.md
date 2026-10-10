# PV parent stock, active inverters and lifecycle boundaries

Experimental bounded pilot, 10 October 2026. Cohort generation and analytical
checks are implemented. The component-level real export and TRAILS comparison
are still pending; this document does not approve promotion.

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

## Components and unresolved export work

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
incorrect. The exporter must distinguish installation, occasional maintenance
and disposal. Neither replacement dates nor failure-age distributions for these
small panel replacements are observed; service-year maintenance is a candidate
explicit fallback, with alternative timing to be tested.

Direct source review also establishes:

- Inverter production contains product disposal and packaging disposal. Moving
  every negative exchange to inverter retirement would misdate packaging.
- Panel production distinguishes municipal factory waste, process oil and
  wastewater from polymer streams described as including end-of-life disposal.
  Some polymer totals include manufacturing losses. Their split must be declared
  or bounded; a negative sign alone is insufficient classification evidence.
- Mounting-system plastic input comments identify packaging, while some matching
  waste comments call it end of life. Preserve the quantity and document this
  source inconsistency with a timing sensitivity.
- Mounting and electrical installation include end-of-life ports; the mounting
  system also embeds land occupation. These need an explicit lifecycle review.
- Component factories are independently used capital. They must not receive the
  blanket zero shift assigned to newly installed PV components.

Raw ecoinvent comments and inventory coefficients remain in restricted local
audit files. Exact reviewed supplier codes, input hashes and timing roles belong
in the exporter audit; they must be validated against a new database revision.
The real static rewrite, signed temporal pulses, full LCI conservation, annual
cache/interpolation and legacy comparison are outstanding release work.

## Reproduce the cohort evidence

```sh
PYTHONPATH="$PWD" python dev/stock_vintage/generate_pv_cohorts.py \
  --input-dir /path/to/downloads \
  --iam-file '/path/to/remind 3.5.2/REMIND_generic_SSP2-NPi2025.mif' \
  --scenario SSP2-NPi2025 --output /private/local/pv-npi2025-cohorts.json
python -m pytest tests/test_stock_cohorts.py tests/test_stock_pv.py \
  tests/test_stock_ccgt.py tests/test_stock_power_service.py
```

Repeat generation for SSP2-PkBudg650. The four focused test modules have 46
passing tests. Two local runs each produced 72 annual case records plus paired
inverter and parent retirement profiles. Annual stock-balance residuals are no
larger than `4.55e-13 MW AC`. Restricted IAM files and derived series remain local;
the public repository contains methods, scripts and synthetic tests.

The broader cohort/lifecycle/export suite passes 80 tests, including nested
component separation. In TRAILS, `dev/stock_vintage/report_pv_sensitivity.py`
accepts the two local cohort JSON files and an `--output-dir`. It writes a
144-row CSV, input-hash manifest and PNG/SVG/PDF figure distinguishing observed
parent stock from modelled inverter dates. These derived artifacts remain local.
