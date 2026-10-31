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

## Fuel-energy and cross-version review, 14 October 2026

All 11 source/system-model cases were rebuilt from encrypted IAM inputs at
`83b9c0ae`. The review covered 31 scenario databases and 155 climate scores.
Every scenario build passed validation after removing recursive source-market
suppliers from generated fuel markets. That correction was required because an
ecoinvent LPG market could otherwise be relinked to the new World LPG market,
while the World market already purchased regional outputs.

The unchanged 3% relative tolerance identified 65 references for refresh. The
other 90 values remain unchanged.

| Case | Scores checked | References refreshed |
| --- | ---: | ---: |
| ecoinvent-3.8-cutoff | 15 | 8 |
| ecoinvent-3.8-consequential | 15 | 5 |
| ecoinvent-3.9.1-cutoff | 15 | 8 |
| ecoinvent-3.9.1-consequential | 15 | 6 |
| ecoinvent-3.10-cutoff | 15 | 8 |
| ecoinvent-3.10-consequential | 15 | 5 |
| ecoinvent-3.11-cutoff | 10 | 5 |
| ecoinvent-3.11-consequential | 15 | 5 |
| ecoinvent-3.12-cutoff | 10 | 4 |
| ecoinvent-3.12-consequential | 15 | 4 |
| ecoinvent-3.12-EN15804 | 15 | 7 |

The changes follow the expected affected pathways: 15 electricity scores moved
by +3.50% to +8.84%, 15 diesel scores by +5.59% to +26.14%, seven cement scores
by +3.20% to +3.89%, and 28 steel scores by +3.02% to +9.26%. No heat reference
changed. The steel pattern is consistent with the 11 October metals audit; the
diesel and downstream electricity/cement changes are consistent with the later
energy-preserving fuel-market normalization in `531fa008`.

The largest change was ecoinvent 3.8 cutoff / TIAM / diesel, from
0.14569424734924133 to 0.1837842111464194 (+26.14%). Its regional market is
97.85% used-cooking-oil biodiesel by energy. The corrected 38 MJ/kg basis buys
1.1072645311 kg rather than the legacy 0.9785128415 kg mass-normalized amount.
A supplier-score counterfactual attributes 0.0196497417 kg CO2-eq of the
0.0380899638 kg CO2-eq increase directly to this quantity correction; the same
energy correction also propagates through upstream fuel markets. This causal
check supports refreshing the value despite its larger percentage change.

## Approved reference review, 31 October 2026

The ecoinvent 3.10 consequential / IMAGE SSP2-VLHO 2050 Swiss low-voltage
electricity reference is refreshed with user approval. The functional unit is
one kWh of the CH low-voltage electricity market, using the configured IPCC
2013 GWP100 method. The 3% relative and 1e-9 absolute tolerances are unchanged.

| Previous reference | Historical rebuild | Current controlled rebuild | New reference |
| --- | --- | --- | --- |
| 0.020819954475643783 | 0.0201099626452748 | 0.02012391871166371 | 0.02012391871166371 |

Scores are kg CO2-eq/kWh. The historical rebuild uses the reference commit
`aad47eae`, matching encrypted IAM inputs, the compact backend and
`PYTHONHASHSEED=0`. Its original three-scenario integration test reproduces the
historical rebuild value, including with default report generation. The current
controlled rebuild uses the current worktree with the same source project and
method. The current full integration run gave 0.020117887930879652, within 0.03%
of the controlled rebuild. Both UMFPACK and SuperLU reproduce the controlled
scores closely, excluding solver instability as the explanation.

The historical commit also misses the stored reference. The origin of that
discrepancy remains unconfirmed. This approved refresh records the validated
current output; it changes no inventory coefficients, scenario definitions,
methods or tolerances. Other outstanding reference differences remain under
review.

The original three-scenario 3.10 consequential integration test passes after
the refresh, including all 15 LCIA comparisons and its additional LCA check.

The analogous ecoinvent 3.11 consequential / IMAGE SSP2-VLHO 2050 Swiss
low-voltage electricity reference is also refreshed with user approval, from
0.02036883743148212 to 0.019630616626011066 kg CO2-eq/kWh. A fixed-seed rebuild
of `aad47eae` gives 0.019637698525297734, within 0.04% of the current result.
The current integration build passes inventory and export validation; its other
14 LCIA comparisons meet the existing tolerances. This update changes only the
one expected score, retaining the method, functional unit and 3% tolerance.
The full three-scenario 3.11 consequential integration rerun passes after this
update, including all 15 LCIA comparisons and its additional LCA check.

The ecoinvent 3.12 consequential / IMAGE SSP2-VLHO 2050 Swiss low-voltage
electricity reference is refreshed with user approval, from
0.02025572805969673 to 0.019530277530099203 kg CO2-eq/kWh. The fixed-seed
historical rebuild gives 0.019533702823056782, within 0.02% of the current
result. Only this expected score changes; the inventory coefficients, method,
functional unit and 3% relative tolerance are retained.
The full three-scenario 3.12 consequential integration rerun passes, including
all 15 LCIA comparisons and its additional LCA check.

The ecoinvent 3.8 cut-off / IMAGE SSP2-VLHO 2050 Swiss low-voltage
electricity reference is refreshed with user approval, from
0.01749772929936413 to 0.016851242477964448 kg CO2-eq/kWh. The historical
rebuild gives 0.01708337394051675; the current result is 1.36% below that
rebuild. The method, inventory coefficients and 3% tolerance are retained.
The separate TIAM diesel reference in the same integration test remains under
review before the full-file rerun.

The ecoinvent 3.9.1 consequential / IMAGE SSP2-VLHO 2050 Swiss low-voltage
electricity reference is refreshed with user approval, from
0.02077945731165172 to 0.020152543098023504 kg CO2-eq/kWh. The historical
rebuild gives 0.020082313738978355, within 0.35% of the current result. Only
the expected score and review notes change; the method, inventory coefficients
and 3% tolerance are retained.
The full three-scenario 3.9.1 consequential integration rerun passes, including
all 15 LCIA comparisons and its additional LCA check.

The ecoinvent 3.8 consequential / IMAGE SSP2-VLHO 2050 Swiss low-voltage
electricity reference is refreshed with user approval, from
0.018711945923389622 to 0.0180931018765829 kg CO2-eq/kWh. The historical
rebuild gives 0.018124668814854784, within 0.18% of the current result. The
method, inventory coefficients and 3% tolerance are unchanged.
The full three-scenario 3.8 consequential integration rerun passes, including
all 15 LCIA comparisons and its additional LCA check.

## Isolated 3.8 cut-off diesel diagnosis, 31 October 2026

The TIAM SSP2-RCP19 2050 diesel score was reproduced in a fresh environment
with separate historical/current source snapshots and private Brightway roots.
The historical build at `aad47eae` gives 0.18378421188517066, matching the stored
0.1837842111464194 to 7.39e-10. The current build gives
0.17621421161062167 kg CO2-eq/kg, a 4.11896% decrease. Native records, dependency
versions, IAM input bytes, functional unit and LCIA method are held constant.

Sequential controls attribute 69.32% of the decrease to removal of the
unsupported potassium sulfate purchase (`bd7ad59f`), 7.43% to other updates,
8.06% to truck battery sizing with cargo fixed, and 15.19% to increased cargo
under fixed payload utilization. The historical cache removed the existing
sizing metadata; `b844a9d0` preserves it so the intended scenario sizing runs.
Historical transport supply chains plus sulfate restoration recover 99.45%
of the full difference; the remaining 0.55% has not been individually traced.

JRC Table 231 supports sulfate as a non-credited output rather than a purchase.
The transport contribution follows the documented model, with the scientific
qualification that increased cargo is an additional fixed-utilization
assumption, not independently established by TIAM. Holding cargo fixed still
gives 0.1773638981461366, 3.4934% below the historical score. Three relevant
vehicle suites pass (263 tests). This audit changes no diesel reference or
tolerance. Aggregate evidence, limitations and reproduction scripts are in
[`results/lcia-diesel38-isolated-20261031`](../../results/lcia-diesel38-isolated-20261031/README.rst).

Following review of this isolated diagnosis, the user approved refreshing the
3.8 cut-off / TIAM diesel reference from 0.1837842111464194 to
0.17621421161062167 kg CO2-eq/kg. Only that expected value changes; the method,
functional unit, inventory coefficients and 3% relative tolerance are retained.
The complete three-scenario 3.8 cut-off integration rerun passes, including all
15 LCIA comparisons and its additional LCA check.

## Approved 3.9.1 cut-off diesel reference review, 31 October 2026

The approved refresh is TIAM SSP2-RCP19 2050 diesel, Europe without
Switzerland, from 0.1960268324260079 to 0.18849716086515955 kg CO2-eq/kg
(-3.84114%). The historical `aad47eae` rebuild gives 0.1960268331714873,
matching the stored reference to 7.45e-10. Following user approval, only this
expected value and review notes change; the method, functional unit, inventory
coefficients and 3% tolerance are retained.

Read-only matrix controls on the historical and current databases identify
the same pathways reviewed for 3.8 cut-off. Removing only the sulfate purchase
from the historical system gives 0.18992619320354287, accounting for 81.02%
of the decrease. Restoring it in the current system gives 0.1945100105483542
(79.86% on that basis). Restoring historical road-transport supply chains
and the sulfate purchase together gives 0.19602862586693745, within 0.00092%
of the historical rebuild. This identifies essentially the entire change,
with the same documented vehicle/battery and payload assumptions as the
approved 3.8 review. Whole-chain effects interact and are not additive.

The controls restore 34 regional sulfate purchases and redirect 8,072 current
consumer edges to 784 exact matching historical road-transport providers.
They alter matrices in memory only. Evidence is retained privately in
`/private/tmp/premise-diesel39-sulfate-check.json` and
`/private/tmp/premise-diesel39-combined-counterfactual.json`.
The complete three-scenario 3.9.1 cut-off integration rerun passes, including
all 15 LCIA comparisons and its additional LCA check.

## Approved 3.10 cut-off diesel reference review, 31 October 2026

The approved TIAM SSP2-RCP19 2050 diesel reference, Europe without Switzerland,
is 0.2106008475987415 rather than 0.21779146737887428 kg CO2-eq/kg (-3.30161%).
The historical `aad47eae` rebuild gives 0.21779146847056235, reproducing the
stored reference within floating-point precision. Following user approval,
only this expected value and review notes change; the inventory coefficients,
functional unit, method and 3% tolerance are retained.

Removing only the sulfate purchase from the historical system gives
0.21218043923833188, explaining about 78% of the decrease on that basis.
Restoring sulfate plus historical road-transport supply chains in the current
system gives 0.21802022236908392, 0.105% above the historical score. The combined
control over-recovers the decrease by 3.18%; other updates partly offset these
two pathways, and that small remainder has not been individually attributed.
This supports the same reviewed sulfate and vehicle-accounting explanation,
including the documented payload assumption, without claiming an exact
additive decomposition. The control restores 34 sulfate purchases and redirects
8,736 current consumer edges to 784 exact matching historical road providers.
It changes no stored inventory data. Private evidence is in
`/private/tmp/premise-diesel310-sulfate-check.json` and
`/private/tmp/premise-diesel310-combined-counterfactual.json`.
The complete three-scenario 3.10 cut-off integration rerun passes, including
all 15 LCIA comparisons and its additional LCA check.

## Approved 3.12 EN15804 diesel reference review, 31 October 2026

The final approved refresh is TIAM SSP2-RCP19 2050 diesel, Europe without
Switzerland, from 0.14801012562412413 to 0.14111062017035847 kg CO2-eq/kg
(-4.66151%). The historical `aad47eae` rebuild gives 0.14801012434102145,
reproducing the stored reference within floating-point precision. Following user
approval, only this expected value and review notes change; inventory
coefficients, method, functional unit and 3% tolerance are retained.

Removing sulfate alone from the historical system gives 0.14265146453304794,
explaining about 77.67% of the decrease. Restoring sulfate and historical
road-transport supply chains in the current system gives 0.14803488262553646,
within 0.017% of the historical result. The small overshoot is not individually
attributed; whole-chain effects interact. The same reviewed sulfate correction
and documented vehicle/battery/payload assumptions apply. The control restores
34 sulfate purchases and redirects 9,697 current consumer edges to 784 exact
matching historical road providers, entirely in memory. Evidence is retained
privately in `/private/tmp/premise-dieselEN15804-sulfate-check.json` and
`/private/tmp/premise-dieselEN15804-combined-counterfactual.json`.
The complete three-scenario EN15804 integration rerun passes, including all
15 LCIA comparisons and its additional LCA check.

## Final verification, 31 October 2026

Exactly ten expected scores were refreshed with user approval: six electricity
and four diesel references. Activity identities, units, LCIA methods, inventory
coefficients and the 3% relative / 1e-9 absolute tolerances are unchanged.
All nine previously failing integration tests pass their complete reruns.
The LCIA regression helper suite also passes (six tests).

Merging the original full suite's three JUnit batches with those nine affected
test reruns accounts for all 3,508 original tests: **3,507 passed, zero failures,
zero errors and one intentional skip** (`Passenger-car calibration only`).
This is a combined result ledger, not a new full-suite invocation after the
reference-only edits. Private evidence is retained in
`/private/tmp/premise-final-suite-20261031.json`, which lists the original
batches, successful rerun XML files and the skip reason.
