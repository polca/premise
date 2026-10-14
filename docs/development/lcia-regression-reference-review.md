# LCIA reference review, 28 September 2026

The weekly certification failures required a review of the LCIA references after
the September inventory and fuel-carbon changes. The existing 3% relative and
1e-9 absolute tolerances are retained. Only scores outside those tolerances are
refreshed; functional units, activities, LCIA methods, and scenario definitions
are unchanged.

Builds use source revision `f16d90bc`, including the electricity rounding and
EN15804 equipment-product matching fixes. Each build uses the existing source
database, encrypted IAM data, `PYTHONHASHSEED=0`, the compact inventory backend,
and all default sectors. Separate `audit-lcia-20260928-testN` databases preserve
the existing source and test databases. Every functional unit is one unit of the
activity declared in `tests/data/lcia_regression_scores.yaml`.

The scenario labels are `test1` = REMIND SSP3-rollBack 2050, `test2` = IMAGE
SSP2-VLHO 2050, and `test3` = TIAM-UCL SSP2-RCP19 2050. The 3.11 cutoff test
contains only `test1` and `test2`. Each case uses its configured IPCC 2013 GWP100
method, with version-specific method-name resolution. EN15804 uses its own
source database with Premise's `cutoff` transformation setting, matching the
integration test.

## Evidence for the changes

The [20 September](https://github.com/polca/premise/actions/runs/35500605696) and
[27 September](https://github.com/polca/premise/actions/runs/36309642720) weekly
runs share 25 failing scores across seven cases. Their largest relative
difference is `2.15e-8`. Fresh local builds reproduce all 25 with a maximum
relative difference of `3.02e-8` from the 27 September run.

Most older-version references originate from August. The intervening changes
include [IEA PVPS 2026 inventories across supported ecoinvent versions](https://github.com/polca/premise/commit/cb61fedf90971dab4bd03ef84f147a76eb8c0833)
and the [fuel-carbon capture-balance correction](https://github.com/polca/premise/commit/678da0b991d220b602fd067060504d5c77d44407).
The [22 September reference refresh](https://github.com/polca/premise/commit/82b7475f6347b4554cf074b0622b84adc99aefc8)
covered only 3.12 cutoff and consequential.

A contribution comparison against the existing 28 August 3.10 consequential
test databases supports the direction of the changes:

- IMAGE Swiss low-voltage electricity decreases from `0.0214603260` to
  `0.0193464018` kg CO2-eq/kWh. Reduced coal-heat, aluminium, silicon and transport
  contributions accompany the updated PV supply chain.
- TIAM-UCL low-alloyed steel decreases from `1.0888955895` to `0.9950880417`
  kg CO2-eq/kg. Pig-iron CO2-capture activities account for about `0.08808` of the
  `0.09381` kg CO2-eq/kg decrease. In the ODA capture activity, the earlier
  fossil-CO2 credit was zero; the rebuilt activity preserves `-1 kg`. Its gas
  demand (`0.0694871768 m3`) and CO2-storage demand (`1 kg`) are unchanged.
- REMIND natural-gas heat increases from `0.0133379723` to `0.0148835663`
  kg CO2-eq/MJ. For the Italian natural-gas CHP supplier, gas demand is unchanged,
  total emitted fossil plus non-fossil CO2 is conserved, and its fossil/non-fossil
  split changes with the corrected fuel accounting.

These older local outputs are comparison inventories, not an exact reconstruction
of the historical fixture-generating environment. The contribution comparison
supports the identified mechanisms; the fresh builds and repeated CI values
provide the numerical reference evidence.

## Reproduction

Provide the encrypted IAM key through `PREMISE_KEY` or `IAM_FILES_KEY`, then run
in the Brightway environment containing the source project:

```sh
PYTHONHASHSEED=0 python dev/validate_lcia_regression.py \
  --case ecoinvent-3.10-consequential \
  --database-prefix lcia-review-new \
  --output results/lcia-review/ecoinvent-3.10-consequential.json \
  --check
```

Use a fresh prefix for rebuilding. To check existing audit outputs, retain their
prefix and add `--score-existing --check`. The utility records scores, methods,
environment metadata and build validation summaries; it does not modify the
reference file. Its `--check` option calls the integration tests' LCIA assertion.

## Reviewed results

9 source/system-model cases, 26 scenario builds, and 130 LCIA scores were checked.
All scenario builds passed validation with zero errors. 33 reference values were
refreshed. The integration tests' LCIA assertions pass for all nine cases against
the refreshed references. Existing warnings remain reported by the validators.

The focused LCIA regression, photovoltaic, fuel-carbon balance, and consequential
blacklist tests also pass (103 tests).

| Case | Scores checked | References refreshed |
| --- | ---: | ---: |
| ecoinvent-3.10-consequential | 15 | 5 |
| ecoinvent-3.10-cutoff | 15 | 3 |
| ecoinvent-3.11-consequential | 15 | 5 |
| ecoinvent-3.11-cutoff | 10 | 1 |
| ecoinvent-3.12-EN15804 | 15 | 4 |
| ecoinvent-3.8-consequential | 15 | 4 |
| ecoinvent-3.8-cutoff | 15 | 3 |
| ecoinvent-3.9.1-consequential | 15 | 5 |
| ecoinvent-3.9.1-cutoff | 15 | 3 |

The 3.8 consequential and 3.12 EN15804 runs previously stopped before their LCIA
assertions. Their additional reference changes were identified after fixing those
earlier errors. The already-refreshed 3.12 cutoff and consequential references are
unchanged.

| Case / scenario / activity | Previous | Reviewed | Change |
| --- | ---: | ---: | ---: |
| ecoinvent-3.10-consequential / test1 / electricity_low_voltage_ch | 0.08012565911 | 0.0756153617 | -5.63% |
| ecoinvent-3.10-consequential / test1 / heat_natural_gas_europe | 0.0135094796 | 0.0148835663 | +10.17% |
| ecoinvent-3.10-consequential / test2 / electricity_low_voltage_ch | 0.0214890164 | 0.01934640181 | -9.97% |
| ecoinvent-3.10-consequential / test3 / electricity_low_voltage_ch | 0.01939237892 | 0.016578572 | -14.51% |
| ecoinvent-3.10-consequential / test3 / steel_low_alloyed_glo | 1.105967943 | 0.9950880417 | -10.03% |
| ecoinvent-3.10-cutoff / test2 / electricity_low_voltage_ch | 0.021126054 | 0.01937803816 | -8.27% |
| ecoinvent-3.10-cutoff / test3 / electricity_low_voltage_ch | 0.01313821067 | 0.01216565123 | -7.40% |
| ecoinvent-3.10-cutoff / test3 / steel_low_alloyed_glo | 0.8381406806 | 0.7997440382 | -4.58% |
| ecoinvent-3.11-consequential / test1 / electricity_low_voltage_ch | 0.08045863601 | 0.07599882613 | -5.54% |
| ecoinvent-3.11-consequential / test1 / heat_natural_gas_europe | 0.02430814218 | 0.02561122233 | +5.36% |
| ecoinvent-3.11-consequential / test2 / electricity_low_voltage_ch | 0.02147163104 | 0.0193976933 | -9.66% |
| ecoinvent-3.11-consequential / test3 / electricity_low_voltage_ch | 0.0192419759 | 0.01657105355 | -13.88% |
| ecoinvent-3.11-consequential / test3 / steel_low_alloyed_glo | 1.024398465 | 0.9181525025 | -10.37% |
| ecoinvent-3.11-cutoff / test2 / electricity_low_voltage_ch | 0.02094665991 | 0.01936589335 | -7.55% |
| ecoinvent-3.12-EN15804 / test2 / electricity_low_voltage_ch | 0.02221726734 | 0.01856076696 | -16.46% |
| ecoinvent-3.12-EN15804 / test3 / diesel_europe | 0.1453559841 | 0.1396275915 | -3.94% |
| ecoinvent-3.12-EN15804 / test3 / electricity_low_voltage_ch | 0.01388188527 | 0.01163226511 | -16.21% |
| ecoinvent-3.12-EN15804 / test3 / steel_low_alloyed_glo | 0.5238464077 | 0.4804793914 | -8.28% |
| ecoinvent-3.8-consequential / test1 / electricity_low_voltage_ch | 0.06789346158 | 0.064157768 | -5.50% |
| ecoinvent-3.8-consequential / test2 / electricity_low_voltage_ch | 0.01944769738 | 0.01743299624 | -10.36% |
| ecoinvent-3.8-consequential / test3 / electricity_low_voltage_ch | 0.01647275747 | 0.01422691458 | -13.63% |
| ecoinvent-3.8-consequential / test3 / steel_low_alloyed_glo | 0.9132813033 | 0.8061346242 | -11.73% |
| ecoinvent-3.8-cutoff / test2 / electricity_low_voltage_ch | 0.01820973283 | 0.01660457106 | -8.81% |
| ecoinvent-3.8-cutoff / test3 / electricity_low_voltage_ch | 0.01137074814 | 0.01043831418 | -8.20% |
| ecoinvent-3.8-cutoff / test3 / steel_low_alloyed_glo | 0.8119124246 | 0.774426581 | -4.62% |
| ecoinvent-3.9.1-consequential / test1 / electricity_low_voltage_ch | 0.07889547157 | 0.07476496349 | -5.24% |
| ecoinvent-3.9.1-consequential / test1 / heat_natural_gas_europe | 0.01343980522 | 0.01496456363 | +11.35% |
| ecoinvent-3.9.1-consequential / test2 / electricity_low_voltage_ch | 0.02116953403 | 0.01909164507 | -9.82% |
| ecoinvent-3.9.1-consequential / test3 / electricity_low_voltage_ch | 0.01949817498 | 0.01676252148 | -14.03% |
| ecoinvent-3.9.1-consequential / test3 / steel_low_alloyed_glo | 1.134883839 | 1.031469666 | -9.11% |
| ecoinvent-3.9.1-cutoff / test2 / electricity_low_voltage_ch | 0.0205869131 | 0.01895557864 | -7.92% |
| ecoinvent-3.9.1-cutoff / test3 / electricity_low_voltage_ch | 0.01313291012 | 0.01217674843 | -7.28% |
| ecoinvent-3.9.1-cutoff / test3 / steel_low_alloyed_glo | 0.8488257253 | 0.8118015598 | -4.36% |

## Metals correctness review, 11 October 2026

The metals corrections in `3002c98f` change direct material conversions, producer
shares and freight inputs. Paired full builds compare an untouched `fe02694ac5`
checkout with the corrected implementation in isolated Brightway projects.
The same encrypted IAM data, sources, scenario definitions, methods and functional
units are used. All 25 baseline climate scores satisfy the existing references.
Three corrected scores exceed the unchanged 3% tolerance and are refreshed below;
the other 22 references remain unchanged.

| Case / scenario / activity | Previous reference | Fresh baseline | Corrected reference |
| --- | ---: | ---: | ---: |
| 3.12 cutoff / test1 / steel_low_alloyed_glo | 1.8816609917041642 | 1.8802364533034677 | 2.0022511065208146 |
| 3.12 cutoff / test2 / electricity_low_voltage_ch | 0.01897114198748734 | 0.0189694614911037 | 0.01963575644309365 |
| 3.12 consequential / test3 / steel_low_alloyed_glo | 0.9151738406362014 | 0.915152545340846 | 0.9732937653615192 |

The steel changes are consistent with restored freight inputs. For example, the
cutoff iron-ore-concentrate market previously retained sea transport only; it now
also includes 1.50052 tonne-km of road, 0.52140 of rail and 0.05915 of air freight
per kilogram. Supplier-share changes and corrected direct material inputs also
affect supply chains. These total-score comparisons do not assign a separate
causal percentage to each correction. In-ground metal-flow corrections do not
directly contribute to IPCC GWP.

The cutoff climate calculation preceded the final resource-only mine/reagent
guard and metadata-serialization correction; neither changes its technosphere
or IPCC scores. The final RELICS comparisons include both corrections. Detailed
commands, coverage gaps and methodological limitations are in
[the metals audit](../../results/metals-correctness/README.rst). Its
`regression_comparison.csv` records all 25 paired scores and
`reference_check.json` compares them with the updated reference file without
repeating the LCA solves. The raw regression JSON files retain their comparisons
against the references present at calculation time.

Older versions and EN15804 passed the metals compatibility builds documented in
that audit. Their full-scenario LCIA references were not recertified or changed
in this review.
