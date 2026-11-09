# Three material-grade screening proxies

Status: pending explicit scientific approval. These correspondences remain
refused by the public pilot. Finite candidate LCIs do not authorize them.

The scope is three exact ecoinvent 3.12 cut-off market profiles used by the
electricity pilot, converted to UVEK 2026 cut-off with ecoinvent 3.10 biosphere.
The proposal retains each complete existing native material inventory at
**+1 kg per signed kg of source product**. It preserves caller ownership,
uncertainty, formulas and separately listed exchanges. It introduces no
purity ratio, molecular-weight factor, guessed blend or additional processing.

| Material and exact rule | Source requirement | Proposed native screening inventory | Unverified distinction |
| --- | --- | --- | --- |
| Polyether polyol, `bp-5968433ef239f628` | Short-chain liquid polyether polyol, molecular weight below 1,000 g/mol, used for rigid polyurethane | Complete European `Polyols, at plant` | Native aggregated 1995 polyether-polyol data have no established chain-length or hydroxyl-functionality distribution. Rigid-foam performance is not established. |
| Kraft paper, `bp-7c580ffe9daa9693` | European 2021 average of bleached and unbleached sack/packaging kraft paper | Complete European `Kraft paper, bleached, at plant` | The source bleaching shares are unknown. Native data describe a non-integrated mill using early-1990s Swiss technology, without reproducing the source integration, fibre, coating or strength mix. |
| Class-I nickel, `bp-1104956dff1c701e` | Nickel purity of at least 99.70% | Complete global `Nickel, 99.5%, at plant` | The native label does not meet the source purity specification. Its 1994 class-I production mix has copper co-production, allocation and incomplete-emission limitations. |

These are product-mass proxies for screening inventories, not demonstrated
substitutable grades. The native nickel documentation also cautions against
its use where nickel contributes substantially to impacts without a more
detailed analysis. That caveat must remain visible; the candidate LCI does
not establish the nickel contribution or satisfy that further analysis.

The actual pilot uses the polyol in the iridium-production proxy, the paper
in transformer/rectifier production, and the nickel in a silicon-carbide
half-bridge module. Approval would preserve the native inventories once,
including their existing infrastructure, energy, waste and allocation
boundaries. No additional refining, bleaching, polymerization or recovered
co-product credit would be invented. Native inventory gaps, source-market
delivery, geography, technology and vintage differences would remain explicit
warnings. Effects on scores and fitness for a particular application are
unquantified.

All source and native references were checked as positive 1 kg. Native
2025/2026 technosphere coordinates agree within floating-point roundoff;
this comparison does not establish equality of complete inventories or LCIA
results. The three native candidate LCIs are nonempty and finite, and the
source scientific-content hashes match the unchanged pilot baseline.

Only explicit approval of the enumerated decisions would add exact-profile
admissions. Unrelated grades, other source versions, inverse routes and
cross-system-model conversions would remain outside this proposal. The user
experience would retain the existing warning and provenance path.

Private evidence in the pilot work area:
`material_grade_proxy_candidate_review.json`,
`native_material_grade_proxy_candidate_lci_report.json`,
`material_grade_candidate_consumers.json`,
`pilot_remaining_nitrate_polyol_paper_steel_evidence.json`,
`pilot_nitrate_polyol_paper_steel_producer_evidence.json` and
`pilot_remaining_cement_concrete_nickel_wood_evidence.json`.
