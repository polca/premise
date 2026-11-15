# NMC532 background battery screening proxy (15 November 2026)

The selected shipping inventories require ecoinvent 3.12 cut-off’s global NMC532 battery market. Neither inspected native BAFU release has a complete NMC532 pack. Native NMC622 is a cell, so substituting it per kilogram would lose the pack boundary. Under the user’s standing approval for coming modelling decisions, use the complete native NMC111 pack as an explicitly approximate background hardware proxy. It contains cells, module packaging, battery management, casing, cooling materials, assembly and infrastructure.

This changes cathode chemistry: nominal nickel:manganese:cobalt moves from 5:3:2 to 1:1:1. Source and native packs are not materially or environmentally equivalent. Metal demand, manufacturing energy, cell fraction, production geography, allocation, vintage and internal uncertainty differ; score effects are unquantified. Do not infer equivalent energy capacity or lifetime, blend cells to approximate metal fractions, or add pack components twice. Conversion warnings name these limitations. No Premise alias or IAM battery classifier is changed to merge technologies.

Retain each source producer quantity separately in its original order, including its original precision, and all four final freight entries with supported statistics. Native component/assembly delivery remains internal; final market delivery is retained once through complete native freight services. Preserve signed caller mass, formulas, uncertainty and separate emissions. Omit nothing.

Approval covers only the independently inspected source 3.12 cut-off profile, through the existing native 2025-to-2026 route. No runtime Python module changes are required; Brightpath’s resource replay hook is maintainer tooling.

## Evidence and limits

Thirteen Brightpath tests pass, covering signs, statistics, complete-pack boundaries, warnings and rejected unapproved profiles. Ten Premise connector cases for this and the preceding LFP correction pass, including cold/warm caches and withdrawn approvals. All 30,474 other rules and all previous recipes are unchanged. Two signed in-memory LCIs are finite and reverse and agree with independently flattened native demand after numerical residual correction on unchanged matrices. No Brightway database, activity or processed file was written. This verifies the implementation of the proxy, not its impact equivalence.

The archive-only shipping diagnostic now advances to NMC622. Seven correspondences remain unapproved in this source cohort: NMC622, NMC811, NCA, used-battery treatment, container-ship production, maintenance and port facilities. This count covers this cohort, not all later preparation or transformation checks. Live CLIC metadata still returns HTTP 502. Shipping is not qualified, and the overall count remains nine of nineteen. Bounded numerical evidence is in `uvek-nmc532-312-verification.json`.
