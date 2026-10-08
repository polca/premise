# CPC / ISIC versus SimaPro waste classification

Checked 2026-09-24 against the same prepared ecoinvent 3.12 cut-off scenario
used for the CSV comparison: REMIND SSP1-PkBudg1000, 2050, 42,770 datasets.
No exporter or classification rule was changed by this investigation.

## Coverage

42,461 datasets have usable ISIC rev.4 ecoinvent codes; 42,744 have usable CPC
codes. The legacy SimaPro lookup classifies 5,214 datasets as waste treatment.
Only six of these lack ISIC codes; none lack CPC codes. Some classifications
are present as text without a parseable numeric code, explaining the distinction
between classification entries and usable codes.

## Candidate rules

Counts below measure agreement with legacy behavior, not correctness. The legacy
lookup is a comparison baseline, not independently validated ground truth.

| Candidate | Also legacy waste | Legacy waste not selected | Selected outside legacy waste |
| --- | ---: | ---: | ---: |
| ISIC 382 (treatment/disposal) | 4,241 | 973 | 1,026 |
| ISIC 3700 or 382 (also sewage) | 4,567 | 647 | 1,053 |
| All ISIC 38 (also collection/recovery) | 4,663 | 551 | 1,258 |
| CPC 943 (waste-treatment services) | 9 | 5,205 | 9 |
| CPC 39 (waste/scrap products) | 4,743 | 471 | 986 |
| ISIC 3700/382/383 AND CPC 39 | 4,560 | 654 | 249 |
| Brightpath name heuristic | 4,516 | 698 | 1,888 |

## Why classifications differ

ISIC describes the economic activity; CPC describes the product. Neither encodes
SimaPro's exchange section directly. Ecoinvent uses ISIC Rev.4 with extensions;
classification scheme/version must therefore be retained in a resolver.

- `treatment of municipal solid waste, municipal incineration`, reference product
  `electricity, for reuse in municipal waste incineration only`: ISIC 3821,
  CPC 17100, legacy non-waste. ISIC can describe waste treatment while the
  dataset's reference product represents electricity.
- `market for waste packaging paper`: ISIC 3821, CPC 39240, legacy non-waste,
  positive unit reference flow. Combining waste-sector and waste-product codes
  alone still does not distinguish this market's role.
- `treatment of used laptop computer, manual dismantling`: ISIC 3830, CPC 39990,
  legacy waste. A narrow ISIC 382 rule misses material-recovery activities.
- The discussed sewage-sludge treatment producing organic phosphorus fertiliser:
  ISIC 3821, CPC 34659, legacy waste. A fertiliser CPC does not by itself negate
  waste-treatment classification.
- `treatment of spent nuclear fuel, conditioning`: ISIC 3822, CPC 33720,
  legacy waste. Waste reference products are not confined to CPC division 39.

These examples establish ambiguity and mismatches; they do not prove all legacy
choices correct or all code-based alternatives incorrect.

## Recommendation

Use embedded ISIC/CPC as primary evidence for inference, with explicit activity
role information and reviewed exceptions where needed. ISIC is the stronger
activity-level signal; CPC supplies product-level context. Missing or conflicting
evidence should remain an explicit unresolved result. An existing native SimaPro
category should be preserved on round trip. For included suppliers, resolve once
on the supplier dataset and reuse that classification for every link. External
links lacking supplier metadata need a catalog or an explicit classification.

This can reduce dependence on a large process-name lookup, but the current
comparison does not establish an automatic replacement with higher accuracy.
Keep classification separate from sign conversion and folder construction.

## Sources and artifacts

- [UN ISIC 3821](https://unstats.un.org/unsd/classifications/Econ/Structure/Detail/en/27/3821)
- [UN ISIC 3830](https://unstats.un.org/unsd/classifications/Econ/Detail/EN/27/3830)
- [UN CPC 943](https://unstats.un.org/unsd/classifications/Econ/Structure/Detail/EN/1073/943)
- [UN CPC 393 and division 39](https://unstats.un.org/unsd/classifications/Econ/Detail/EN/1073/393)
- [ecoinvent classification methodology](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/report_of_changes_ecoinvent_2.2_to_3.0_20130904.pdf)

Local per-dataset results and cross-tabs are in
`export/simapro-comparison/classification-comparison.csv` and
`export/simapro-comparison/classification-comparison.json`.
The reproduction script is in that directory's `reproduce/` folder. Licensed
inventory data remain in ignored local export files.
