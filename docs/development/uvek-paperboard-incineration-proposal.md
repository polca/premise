# Paperboard incineration within two mixed-disposal markets

Status: pending explicit scientific approval. The two exact profiles remain
refused by the public pilot. This proposal does not enable a mapping.

The scope is `bp-3e43100f56430f0c`, the RoW waste-paperboard market in
ecoinvent 3.9 and 3.12 cut-off, for the electricity pilot's UVEK 2026
cut-off technosphere and ecoinvent 3.10 biosphere. Its existing cork-board
disposal match does not represent this market.

The proposed helpers preserve the original disposal mix in both versions:

| Fate | Share of waste mass | Proposed treatment |
| --- | ---: | --- |
| Unsanitary landfill | 53.6467% | Complete inspected source inventories, separately by infiltration class |
| Open dump | 25.6479% | Complete inspected source inventories, separately by infiltration class |
| Open burning | 8.1891% | Complete inspected source inventory |
| Municipal incineration | 12.5163% | Complete native UVEK packaging-paper incinerator, subject to approval |

The displayed shares are rounded; the candidate retains the original
coefficients without renormalization. All original road and rail collection
quantities remain separate. The five exact collection suppliers match
previously reviewed source metadata and inventory hashes. Their native
geographic and fleet approximations remain warnings.

The new decision is whether to use the complete Swiss
`Disposal, packaging paper, 13.7% water, to municipal incineration` process
for only the controlled-incineration branch, at **+1 kg per kg treated**.
The negative ecoinvent waste reference requires a conversion factor of
**−1** for caller exchanges. The proposal introduces no moisture,
heating-value, allocation or efficiency multiplier.

The paper grades differ. Both describe biogenic paper incineration using
the same documented Swiss 2010 grate and flue-gas-cleaning technology mix,
but 38 of 44 inspected composition/heating-value coordinates differ,
including constituents defined in only one inventory. The source lower
heating value is about 12.7% above the native value; its chlorine
concentration is about 3.8 times the native concentration. Carbon, actual
water content, trace metals, slag, stabilized ash and cement requirements
also differ. Product-name moisture percentages do not establish the
reference basis and must not be used as a conversion factor.

Approval would accept those waste-grade differences as an explicit
screening approximation, with native emissions, energy, ash treatment,
recovery, allocation, geography, vintage and upstream biosphere exclusions
retained as limitations. Score effects are unquantified. The source
incinerator emissions would not be added to the complete native process,
and no extra electricity, heat or metal-recovery credit would be invented.
The source 3.12 nonphysical disposed-waste indicator would be retained
once, weighted by the controlled share, outside the native process that
lacks it.

The remaining 87.4837% uses complete source-emission helpers, preserving
every signed elementary-flow amount and marginal uncertainty. The
unsanitary-landfill helpers retain the already reviewed complete diesel
combustion and skid-steer excavation services. Canonical biosphere links
were checked using publisher UUID, unit and full receiving compartment;
label or CAS metadata changes require warnings, not chemical factors.
There are no source parameters or formulas in these treatment helpers.

Two private complete-market candidates each contain 12 foreground records.
Both link to the exact installed background and give finite, nonempty LCIs.
All foreground supplies are nonnegative; extended-system residuals are
below 8 × 10⁻¹⁷. Background supply comparisons differ by less than
1.5 × 10⁻¹⁰ in relative L2 norm, within the declared numerical tolerance.
Source database scientific-content hashes match the unchanged pilot
baseline. These calculations do not establish impact-score equivalence,
a public migration, scenario certification or an export.

Only an explicit decision on these two profiles would enable them. Other
paper grades, source releases, fly-ash-extraction technologies, reverse
routes and cross-system-model conversions are outside this proposal. The
existing user experience and warning/provenance paths would remain.

Private evidence in the pilot work area:
`paperboard_proxy_candidate_review.json`,
`native_paperboard_proxy_candidate_lci_report.json`,
`paperboard_incineration_component_full_evidence.json`,
`controlled_waste_endpoint_normalized_evidence.json` and the controlled,
uncontrolled and unsanitary biosphere-definition preflight reports.
