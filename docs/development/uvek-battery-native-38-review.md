# Required battery suppliers from ecoinvent 3.8

Fifteen of the 41 unresolved source-3.8 interfaces in the actual battery import
now have reviewed mappings. Fourteen reuse complete named native supplies or
services. One corrects a primary-aluminium relabelling boundary. All source and
native reference signs and units were checked against the installed inventories;
native counterparts were inspected in both UVEK 2025 and 2026.

| Interface | Decision and main limitation |
| --- | --- |
| Aircraft freight | Native complete European freight service per tonne-km; its Lufthansa fleet and flight-length mix replace the global source mix. |
| NMP solvent | Named native N-methyl-2-pyrrolidone per nominal kg. Native documentation says methylamine while an input says dimethylamine; retain and disclose the published model, without inventing a reaction correction. |
| Lithium hexafluorophosphate | Named native Chinese LiPF6 conductive salt per nominal kg; purity, route, residues and delivery differences remain warnings. |
| Nylon 6 | Native nylon 6, with documented industry-data omissions; neither nylon 66 nor a forming service. |
| Primary aluminium ingot | Complete native primary metal, including liquid-metal manufacture and casting. Source alloy-grade wording and silicon input are inconsistent; performance equivalence remains unproven. |
| WEEE shredder-residue treatment | Complete native disposal for residue from manual dismantling. Source reference -1 kg becomes native +1 kg with factor -1 once; geography, collection and residue/recovery differences remain explicit. |
| Light commercial freight | Complete native Swiss fleet service per tonne-km. The 3.5-tonne label is gross weight, not a payload multiplier. |
| Aluminium sheet rolling | Processing per kg, excluding bulk metal. Retain caller metal separately and use native loss/energy/infrastructure once. |
| Aluminium casting facility | One plant unit; both document 130,000 tonnes/year and 50 years. No additional lifetime or capacity normalization. |
| Metalworking factory | Defined infrastructure module, separate from machine processing and operation; source regional construction shares become native infrastructure. |
| Carbon black | Solid carbon black per nominal kg, with uncertain native emissions and unverified battery-grade performance; no graphite or coke substitution. |
| Deionised water | Native deionised water per kg; raw-water quality, regeneration, electricity and purity differ. No additional water-balance correction. |
| Plastics processing factory | Defined infrastructure module, separate from polymer/processing energy; retain caller plant fraction without throughput or lifetime multipliers. |
| Ethylene glycol | Native monoethylene glycol per nominal kg, replacing the allocated source manufacture; no additional EO production or coproduct credit. |
| Primary-ingot relabelling adapter | Source only relabels primary ingot into the wrought-alloy market. Replace its regional primary-ingot shares with native primary metal, rather than a whole wrought-alloy/recycled-metal mix. |

Every decision preserves caller signs, formulas and supported uncertainty, with
only the documented waste-reference sign conversion. Complete native inventories
replace source internal shares, transport, allocation and emissions; those
internal source details are not claimed preserved. Geographic, technology,
vintage, uncertainty and unquantified impact differences remain warnings. Native
nylon and ethylene-glycol inventories contain embedded upstream energy that
ordinary electricity/heat supplier relinking cannot decarbonize.

The standing autonomous goal and modelling auto-approval authorize these scoped
decisions. Exact permissions remain limited to ecoinvent 3.8 cut-off and the
UVEK 2025/native 2026 route. No inverse or cross-system-model permission is added.
There is no new helper, foreground omission, zero-burden authority, native-data
edit or production/generator Python modification in this group. The earlier
numeric-comment fix is separate.

Brightpath's three review files record source/native witnesses and limitations:
`uvek_battery_native_direct_38_review.json`,
`uvek_battery_reused_native_38_review.json` and
`uvek_primary_aluminium_adapter_38_review.json`. Premise's approval ledger binds
their exact scopes. Fresh and cached conversion checks preserve quantities and
warnings; withdrawing either native review or the adapter correction refuses
conversion. All three generated migration resources reproduce byte for byte.
Fourteen distinct native suppliers pass 28 signed finite LCIs without database
writes. The adapter uses the same verified primary-aluminium supplier.

The actual preparation retry reports **26 remaining source-3.8 interfaces**, down
from 41. This diagnostic uses the recorded archive and bypasses only the public
sector-admission gate; it does not disable migration validation or export a
database. Battery is still unqualified. Nonzero source amounts do not prove
every chemistry has a nonzero IAM market share: the shared battery update also
adjusts energy density across imported capacity datasets. Pruning them here
would alter its existing behavior. See `uvek-battery-native-38-verification.json`.
