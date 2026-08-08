# Consequential marginal-mix validation

Generated 2026-08-08T15:21:41+02:00 from Git state `f76905b0 + uncommitted changes`.

## Scope

Fixed case: **IMAGE / SSP1-M, WEU, 2050, electricity**. The complete technology-by-technology results are in [consequential_marginal_mix_validation.csv](consequential_marginal_mix_validation.csv).

Reference arguments: `range time=2`, `duration=0`, `foresight=False`, `lead time=False` (market average), `capital replacement rate=True`, and `measurement=0`. Each following case changes one argument family.

An additional `2090` row keeps the same scenario and region but checks the declining-market supplier-selection branch. It is not part of the one-factor comparison.

The production result comes from `premise.marginal_mixes.consequential_method`. The oracle is separate code in `examples/validate_consequential_marginal_mixes.py`: it reads the YAML parameters directly, uses SciPy's Akima interpolator, constructs the documented observation windows, and evaluates the six measurement equations without importing Premise's marginal-mix helpers.

## Results

| Case | Change or branch check | Effective interval | Market | Largest suppliers | Max absolute delta |
|---|---|---|---|---|---:|
| reference | short, myopic, average lead time | avg 2050–2054 | expanding/replacing | Wind Offshore 0.583; Wind Onshore 0.192; Solar PV Centralized 0.112; Solar PV Residential 0.055; Storage, Battery 0.041; Gas CC 0.015 | 0.00e+00 |
| individual_lead_time | lead time = individual | avg 2050–2054; starts 2050–2055; ends 2054–2059 | expanding/replacing | Wind Offshore 0.559; Wind Onshore 0.203; Solar PV Centralized 0.119; Solar PV Residential 0.058; Storage, Battery 0.042; Gas CC 0.018 | 0.00e+00 |
| perfect_foresight | foresight = perfect | avg 2048–2052 | expanding/replacing | Wind Offshore 0.613; Wind Onshore 0.190; Solar PV Centralized 0.108; Solar PV Residential 0.046; Storage, Battery 0.041; Geothermal 0.002 | 0.00e+00 |
| long_duration | duration = 20 years | avg 2052–2072 | expanding/replacing | Wind Offshore 0.306; Wind Onshore 0.254; Solar PV Centralized 0.197; Gas CC 0.112; Solar PV Residential 0.065; Storage, Battery 0.051 | 0.00e+00 |
| no_replacement | capital replacement rate = off | avg 2050–2054 | expanding/replacing | Wind Offshore 0.966; Storage, Battery 0.024; Solar PV Residential 0.010 | 0.00e+00 |
| measurement_1 | measurement = linear regression | avg 2050–2054 | expanding/replacing | Wind Offshore 0.583; Wind Onshore 0.192; Solar PV Centralized 0.112; Solar PV Residential 0.055; Storage, Battery 0.041; Gas CC 0.015 | 0.00e+00 |
| measurement_2 | measurement = area above baseline | avg 2050–2054 | expanding/replacing | Wind Offshore 0.431; Wind Onshore 0.256; Solar PV Centralized 0.157; Solar PV Residential 0.068; Storage, Battery 0.045; Gas CC 0.023 | 0.00e+00 |
| measurement_3 | measurement = weighted slope | avg 2050–2054 | expanding/replacing | Wind Offshore 0.567; Wind Onshore 0.199; Solar PV Centralized 0.116; Solar PV Residential 0.057; Storage, Battery 0.042; Gas CC 0.017 | 0.00e+00 |
| measurement_4 | measurement = split annual | avg 2050–2054 | expanding/replacing | Wind Offshore 0.581; Wind Onshore 0.193; Solar PV Centralized 0.113; Solar PV Residential 0.055; Storage, Battery 0.041; Gas CC 0.015 | 0.00e+00 |
| measurement_5 | measurement = legacy volume | avg 2050–2054 | expanding/replacing | Wind Offshore 0.338; Wind Onshore 0.283; Solar PV Centralized 0.211; Solar PV Residential 0.089; Gas CC 0.038; Storage, Battery 0.037 | 0.00e+00 |
| declining_branch | declining-market check at year 2090 | avg 2090–2094 | declining | Gas CC 0.589; Wind Onshore 0.298; Hydro 0.056; Coal PC 0.048; Solar PV Residential 0.008 | 0.00e+00 |

`Max absolute delta` is the largest supplier-share difference between the production calculation and the independent oracle.

## Checks

| Check | Result | Evidence |
|---|---|---|
| Production vs independent oracle | PASS | 11/11 cases within 1e-12; maximum 0.00e+00 |
| IAM-to-electricity wiring | PASS | Raw mapped volumes normalise to the `IAMDataCollection.electricity_mix` result; max absolute delta = 0.00e+00 |
| Mixes sum to one | PASS | Range 1–1 |
| No negative or non-finite shares | PASS | Minimum share -0; all values finite = True |
| Constrained suppliers excluded | PASS | Largest total constrained share 0.00e+00 |
| Invalid argument combinations rejected | PASS | 3/3 expected errors raised |
| Real-data market-direction branches | PASS | Expanding/replacing and declining branches both covered = True |
| Constrained-supplier parameter schema | WARN | YAML runtime type is `str`; unintended substring matches: `diesel`, `liquefied petroleum gas` |

Guardrails exercised:

- PASS — range and duration cannot both be non-zero
- PASS — a 2-year duration must use range time
- PASS — split-annual measurement rejects individual lead times

## Interpretation

This run confirms that the implemented numerical code follows the stated algorithm for this real IAM market across every consequential argument family, all measurement methods, and both market-direction branches. The lead-time switch changes the supplier-specific windows (rather than disabling lead time), while perfect foresight centres the window on the demand year.

One separate robustness issue was found: `constrained_suppliers.yaml` is not a YAML list. It currently parses as one folded string, so production uses substring membership. This does not change the electricity results above, but it also classifies `diesel` and `liquefied petroleum gas` as constrained even though neither is an exact entry. The file or loader should be corrected before treating fuel-sector marginal mixes as validated.

It is a strong implementation and regression check, but not a proof that the behavioural assumptions are empirically correct for every market. Confidence should also come from the synthetic unit tests, additional scenario/region fixtures, and review of the lead-time, lifetime, and constrained-supplier parameter files.

## Reproduce

```console
PREMISE_KEY=... python examples/validate_consequential_marginal_mixes.py
```
