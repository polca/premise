# Vanadium source inventory correction for 2.5.4

This correction addresses [issue #286](https://github.com/polca/premise/issues/286).
The released South African mining inventory combines an avoided-ilmenite
technosphere input with direct extraction of iron, titanium and vanadium.
The metals transformation removes the direct co-mined titanium but retains
the avoided supplier, producing negative titanium extraction upstream of
vanadium products and batteries.

## Source changes

Five South African datasets change. The other eight imported datasets,
including the Chinese route, are exactly equal before and after the edit.

| Stage | Correction |
| --- | --- |
| Mining | Remove the ilmenite substitution input. Allocate shared burdens by the relative value of 1.53 kg magnetite and 0.46 kg ilmenite: 58.6855815661% to magnetite. Assign Fe and V resources to magnetite and Ti to ilmenite. |
| Pre-reduction | Normalize the original 1.46 kg reference output to 1 kg. |
| Cast iron | Normalize the original 1.32 kg reference output to 1 kg. |
| Slag | Normalize the accounting output of 0.1226 kg to 1 kg; this already encodes the original 50% economic allocation, which is not applied twice. |
| Refining | Remove the sodium-sulfate credit, retain the 0.5 kg gross chemical input, and allocate 98.9280245023% of burdens to V2O5 using the value of the two saleable outputs. Preserve recycling and waste-treatment directions. |

Lognormal distributions in changed datasets are centered on the corrected
amount, with their relative scale retained. The historical allocation sheet
is preserved but explicitly superseded. The new allocation sheet and exchange
comments identify the assumptions and sources. Unchanged spreadsheet formulas
retain cached values and correct cell references after row removal.

The full exchange-level amount comparison is in
[vanadium-inventory-exchanges.json](vanadium-inventory-exchanges.json).
The source-price rationale and limitations are in
[the stationary-battery methodology](../docs/methodology/battery-stationary.rst).

## Validation

Local validation on 2026-10-07 used:

- Brightway project and source database: `ecoinvent-3.12-cutoff`; biosphere: `biosphere`.
- Background system model: cut-off; ecoinvent version: 3.12.
- Scenario: REMIND `SSP1-PkBudg1000`, 2050; all Premise transformations.
- LCIA method: `('IPCC 2021', 'climate change: total (excl. biogenic CO2)', 'global warming potential (GWP100)')`.
- Baseline workbook: unchanged `v.2.5.3`, SHA256
  `6ce897e076fd5bcd35c8aaf5e9097addb61f9bb6cf03177dab74c7b793453903`.
- Both runs used the same transformation code and background; only the
  vanadium workbook differed. Source inventories contained 29,804 activities;
  each transformed scenario contained 42,758 activities.

Sparse inventory calculations preserve production quantities, waste signs
and supplier identities. All suppliers resolved uniquely. Matrix solve
residuals were below `1e-7`. No Brightway database was overwritten; calculations
used materialized no-write builds. No licensed background inventories are
included in these report files.

### Results after all scenario transformations

Each demand is one unit of the named reference product. Mining and V2O5
results are per kg; the battery result is per complete 8.3 MWh system.

| Dataset / location | Titanium, before (kg) | Titanium, after (kg) | Climate, before (kg CO2-eq) | Climate, after (kg CO2-eq) |
| --- | ---: | ---: | ---: | ---: |
| Vanadium-bearing magnetite / ZA | -0.0975693 | 0.00000145 | 0.0274738 | 0.0257816 |
| Vanadium pentoxide / ZA | -1.643643 | 0.00021240 | 5.330737 | 5.363811 |
| Vanadium pentoxide / CN | 0.00046958 | 0.00047410 | 22.277354 | 22.277355 |
| Vanadium-redox flow battery system assembly, 8.3 megawatt hour / RER | -127783.9819 | 198.5140 | 665834.4541 | 668409.4150 |

The battery climate result changes by approximately +0.387%. The changed
resource accounting is not a clamp on negative results: the negative
technosphere credit is removed at its source. Unchanged Chinese activities
can still receive small indirect changes through the shared scenario background.

Before scenario transformations, ZA V2O5 titanium extraction changes from
0.7356712 to 0.00041887 kg/kg. Its climate result changes from 21.3922875 to
21.4119880 kg CO2-eq/kg. Full source/scenario results, including Fe and V, are
in [vanadium-inventory-validation.json](vanadium-inventory-validation.json).

Validation also passed:

- 12 source-inventory regression tests, including migration to ecoinvent
  3.9, 3.10, 3.11 and 3.12, uncertainty, retained formula caches, allocation,
  cache invalidation, and the titanium-credit failure mechanism.
- The broader non-slow suite: 1,010 passed, two skipped, 17 slow tests deselected.
- Sphinx with warnings treated as errors; 104 pages, 592 historical anchors
  and 59 Python examples checked with zero documentation-check errors.

## Scope and remaining limitation

The mining price uses ordinary magnetite as a proxy; it contains no measured
vanadium premium. Refinery prices are historical US proxies on a common
2010 currency basis. They are fixed foreground assumptions, not forecasts.

The original ore-to-slag vanadium quantities are inconsistent under the
inherited V2O5-content interpretation. This change preserves those source
yields. For example, source ZA V2O5 still carries approximately 0.207 kg
upstream vanadium per kg product, below the product's elemental V content.
Therefore these results establish the allocation/credit correction, **not a
closed physical vanadium mass balance**. Revising metallurgical yields requires
additional source clarification. Consequential scenario results were not
validated in this comparison.

## Reproduction

Extract the released workbook and run the guarded correction script:

```sh
git show v.2.5.3:premise/data/additional_inventories/lci-batteries-vanadium.xlsx > /tmp/vanadium-2.5.3.xlsx
python scripts/correct_vanadium_inventory.py /tmp/vanadium-2.5.3.xlsx
```

The script checks the original file hash and refuses a corrected or otherwise
modified input, preventing double allocation. It writes the corrected workbook
and the exchange comparison. With the licensed project above and `PREMISE_KEY`
set for encrypted IAM data, run:

```sh
python dev/validate_vanadium_inventory.py --baseline --baseline-workbook /tmp/vanadium-2.5.3.xlsx
python dev/validate_vanadium_inventory.py
```

Aggregate reports are written under `results/`; local inventory snapshots are
written under the ignored `dev/` directory. Rebuild existing exported scenario
databases to apply the correction. Package installation alone does not rewrite them.
