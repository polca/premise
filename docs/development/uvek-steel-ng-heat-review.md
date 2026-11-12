# Steel prerequisite: natural-gas production heat mix

The ecoinvent 3.9 cut-off global natural-gas district/industrial heat market
expands through six market nodes to 104 positive production-supplier
contributions. Every reference is +1 MJ. The market layers have deterministic
shares, no formulas, no direct biosphere exchanges, no other inputs and no
cycles. One derived native mix therefore replaces these wrappers without
losing uncertain market shares or introducing additional market datasets.

The recipe retains all 104 supplier contributions and the source's 80.0431% CHP
versus 19.9569% heat-only split. It uses 36 complete native production-side
suppliers. Conventional and combined-cycle large CHP map to country-specific
native gas CHP heat where available, otherwise ENTSO-E CHP heat. The ENTSO-E
fallback represents 25.8313% of total heat. This does not preserve each original
plant's cycle, efficiency, allocation, cooling system or country technology.

Lean-burn gas engines use Swiss exergy-allocated 300 kWth/1 MWth heat suppliers.
Source capacities are electrical; target capacities are thermal. They are
screening capacity proxies, not equal-size plant claims. Industrial, low-NOx,
condensing and modulating heat-only classes use corresponding native production
processes with explicit Swiss/RER geography and capacity proxies.

All target references are +1 MJ allocated heat, not fuel energy. Factor +1
preserves signed caller heat demand, uncertainty and formulas. The native
allocated heat inventories replace the source allocated plant inventories;
no physical-fuel normalization, efficiency rescaling or avoided-electricity
credit is added. Native fuel supply, combustion, direct emissions and
infrastructure remain inside each complete process once. Separate caller
emissions and freight remain unchanged. Geography, allocation, efficiency,
plant technology, vintage, internal uncertainty and native biosphere exclusions
can differ; score effects remain unquantified.

The old delivered-district-heat suggestion is not used. No district network,
transport service or loss multiplier is added. All 36 native suppliers were
checked for direct district-delivery inputs: none were found. This check is
bounded to their direct inputs, not a claim of identical upstream inventories.
See `uvek-steel-ng-heat-boundary-verification.json`.

Frozen Brightpath review `export/uvek_steel_39_ng_heat_review.json`, SHA256
`acb922b2f63b28666de6d270e29eb973e6bab71814a4ce019d02d404180045a1`.
Brightpath commit `1fecf3733c63c9bbf821d037d19d01dc4ac676bb` is pushed and
installed from GitHub. Only the exact independently inspected 3.9 cut-off source
profile is approved. Other releases, reverse routes and cross-system-model
conversions remain unapproved. No runtime Python modules changed; maintainer
replay, generator hook and tests make the resource reproducible and reject drift.
Licensed source vectors stay private; the public resource contains the derived
native mix, identity/reference facts and hashes.

Nine Brightpath checks pass (18.62 seconds), covering signed demand,
normal/lognormal uncertainty, formulas, immutable foreground, exact release
scope, all 104 helper quantities/provenance, CHP/heat-only separation and atomic
parent/recipe drift refusal. Premise strict fresh/cache/approval-withdrawal
passes (17.47 seconds). Black and Ruff pass. The 104-input weighted mix, correctly
summed across 36 distinct native suppliers, passes two finite signed composite
LCIs with exact sign reversal and unchanged source database. See
`uvek-steel-ng-heat-composite-lci-verification.json`.

Actual encrypted REMIND SSP2-NPi 2050 diagnostic steel preparation now has
**16 pending correspondences, down from 17**. Only the closed steel support
guard was bypassed; strict migration validation remained enabled. The remaining
two other-fuel heat mixes, thirteen waste/wastewater cases and ferrosilicon are
unapproved. No successful steel transformation/export is claimed. Private audit:
`4b3dc5fe5ceba74acd131aa197a3c4e8717f47a64a7448b4b99cbb08c05f5117.audit.json.gz`.

The regular environment's installed GitHub commit identity and all 794 approval
scopes validate. Eight obsolete wheel caches from this task were removed,
freeing 332,459,307 bytes. The current wheel, installed environments, source
inventories, exported databases and unrelated caches were retained. The private
cleanup receipt is `steel-ng-heat-pip-cache-eviction.json` in the task workspace.
