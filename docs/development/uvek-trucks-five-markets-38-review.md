# Five required truck supply markets

This decision covers five inputs encountered by the truck workflow in ecoinvent
3.8 cut-off. It adds mapping data and scoped approvals, with no production Python
or notebook changes. It does not qualify the complete truck update.

| Input | Decision |
| --- | --- |
| Waste electrical and electronic equipment | Keep the two original collection transport legs and use the complete Swiss shredding process. Convert the negative waste reference to a positive treatment demand exactly once. The processor is nonempty. |
| Potassium hydroxide | Keep both producer shares and all four delivery legs. The source already expresses one kg of pure KOH, so no additional concentration multiplier is applied. |
| Benzaldehyde | Use the complete native benzaldehyde process and retain the three source delivery legs, including their source 3.8 uncertainty data. |
| Fabricated integrated-circuit wafer | Reuse the complete reviewed logic-wafer helper per square metre. Keep rail and road delivery. Two uncertainty scales differ only by one adjacent floating-point value; this rounding is recorded in the review. |
| Swiss high-pressure gas | Use 39 MJ of complete native delivered gas per source cubic metre, as explicitly documented by source 3.8. The earlier source 3.9 conversion remains 36 MJ. No assumed equivalence between standard cubic metres, extra network losses or combustion emissions is introduced. |

Four new helpers are needed; the wafer helper is reused. Original caller signs,
formulas and supported uncertainty remain consistent with the conversion factor.
Existing approved source profiles and their recipe contents are unchanged.
Warnings retain differences in technology, geography, allocation, vintage,
internal uncertainty and supply boundaries. These are native screening models;
source/native impact-score equivalence has not been established.

The normal migration generator reproduces all three checked-in resources exactly.
The two biosphere resource files remain unchanged. Focused tests cover signed
normal/lognormal quantities, both UVEK releases, treatment signs, the gas energy
basis, preservation of older permissions, cache replay and permission withdrawal.

The actual workflow retry reduced its first source-3.8 truck cohort from nine
unapproved rules to four: waste wood, waste polyethylene, amorphous PET with
recycled supply, and recycled HDPE. They remain unresolved. In particular, an
empty recycling handover or virgin HDPE is not accepted as paid recycling.

Machine-readable results are in `uvek-trucks-five-markets-38-verification.json`.
Full licensed source inventories remain local. The shareable decision evidence
and native recipe inputs are in Brightpath's
`data/export/uvek_trucks_five_markets_38_review.json`.
