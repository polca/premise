# Steel, copper and glass routes using fly-ash extraction

Status: implemented under the user instruction to auto-approve coming
modelling decisions. All five exact profiles are registered and verified;
the accepted FAE technology differences remain explicit warnings.

The proposal uses existing UVEK ordinary, material-specific incinerators as
screening proxies for the enumerated Swiss incineration-with-fly-ash-extraction
(FAE) portions. All declared wet-composition/heating-value coordinates match
the selected native 2026 inventories: 42 for steel and copper, 44 for glass.
Unknown entries remain unknown. Composition agreement does not establish
equivalence of ash treatment, recovery, emissions or allocation.

| Exact source case | Rule | FAE portion per kg of source treatment | Other source stages retained |
| --- | --- | ---: | --- |
| 3.6 CH reinforcement-steel collection for final disposal | `bp-180ae2ddfbb9b03a` | 22.8557% | Collection operations, scrap-market collection and 77.1443% inert landfill |
| 3.9 CH reinforcement-steel market | `bp-6ac3c055dea65676` | 2.3021% | 62% recycling stage, 28% sorting stage, and the balance of the 10% final-disposal branch |
| 3.9 CH scrap-steel market | `bp-93fac8cf40762835` | 23.0210% | 76.9790% inert landfill and original collection |
| 3.9 CH scrap-copper market | `bp-7df5d08d5bab9d33` | 100% | Original collection |
| 3.9 CH waste-glass market | `bp-954bd1c433edd37a` | 98.9999% | 0.9474% open dump, 0.0527% open burning and original collection |

The table is rounded. The candidates retain the actual source coefficients,
individual freight inputs and marginal uncertainties without renormalization.
The different 3.6 and 3.9 steel treatment shares are independently inspected;
neither version inherits the other's mix.

The new decision is to use the complete native
`Disposal, steel, 0% water, to municipal incineration`,
`Disposal, copper, 0% water, to municipal incineration` and
`Disposal, glass, 0% water, to municipal incineration` processes only for those
FAE portions, at **+1 kg per kg treated**. Source negative waste references
require a caller conversion factor of **−1** to positive treatment helpers.
No moisture or heating-value multiplier is proposed. In particular, the glass
process's name is not its composition definition; its inspected water content
matches the source despite the name's “0% water” label.

The source FAE documentation reports a 46.22% FLUWA share in the technology
mix. The ordinary native processes do not reproduce that ash treatment.
Ash quantities and handling, short- and long-term releases, auxiliary energy,
internal energy reuse, recovery, allocation and vintage differ. The source
steel and copper inventories explicitly retain recovered-metal handovers;
they contain no separate recovered-zinc exchange. Add no invented zinc,
electricity, heat or avoided-primary-metal credit. Effects on impact scores
remain unquantified. This proposal does not construct a new FLUWA inventory.

The candidates preserve the original recovered-metal exchanges of
−0.47289 kg of iron per incinerated kg of steel and −0.31 kg of copper per
incinerated kg of copper. Their exact positive source references contain only
self-linked production, with no processing or biosphere exchanges, and match
the existing inspected cut-off-stock evidence. Retain those material handovers
through production-only stock helpers. Their negative supply is intentional;
it does not become a credit for virgin metal production or an empty treatment
process. The selected ordinary native incinerators have no negative
technosphere inputs duplicating those recovered-material handovers.

Other stages use the actual source operating structure. Steel recycling and
sorting retain their original diesel demand; sorting also retains electricity,
hydraulic excavation and facility demand. Source and native sorting facilities
both document 200,000 tonnes/year and 50 years; retain the original plant count
without another throughput or lifetime factor. These source-stage helpers
exclude the extra haulage and waste heat found in the complete native sorting
service. Original collection freight remains separate. Inert-landfill branches
retain their original complete generic operation and facility quantities,
including the source's absence of direct waste-specific leachate exchanges.
That observed data boundary does not establish zero physical leaching.

The glass open-dump and open-burning helpers preserve all 34 original
elementary-flow exchanges, signed quantities and uncertainty. Canonical
ecoinvent 3.10 links use matching publisher UUIDs, units and full receiving
compartments; label or CAS metadata changes remain warnings. No physical
release or uncontrolled-disposal branch is replaced by incineration.

Two 3.6 scrap-market freight formulas reference `WM_RP`. The source production
exchange explicitly defines that symbol as its fixed wet mass of 1 kg. The
candidate binds that declared property to the corresponding numeric literal
in the helper formulas, preserves their evaluated quantities, and records the
original formulas and binding in the review. No unbound parameter is exported.
Caller formulas and uncertainty remain separate and unchanged except for the
required source-to-target reference-sign scaling.

The five private complete-route candidates contain 5, 8, 4, 3 and 4 foreground
records respectively and give finite, nonempty LCIs on the exact installed
UVEK 2026 / ecoinvent 3.10 biosphere context. After one residual-correction
step, independent native and extended-system solutions agree within
1.61 × 10⁻¹⁵ in relative supply norm, with equation residual below
5.64 × 10⁻¹⁶. Only the explicitly verified stock nodes have negative foreground
supply. Source scientific-content hashes remain unchanged. These checks do
not establish impact-score equivalence, public migration, scenario
certification or export.

The implementation covers only these five exact cut-off profiles and the stated
substitutions, with the full limitations retained in warnings and provenance.
Other materials, FAE source profiles, recovered products, inverse routes and
cross-system-model conversions remain outside this proposal. The existing user
experience remains unchanged.

Private evidence in the pilot work area:
`pilot_fae_group_full_evidence.private.json`,
`steel_collection_36_full_evidence.private.json`,
`fae_auxiliary_primitives_full_evidence.private.json`,
`pilot_fae_candidates_review.json` and
`native_pilot_fae_candidates_lci_report.json`.

## Implemented checkpoint

Packaged review: Brightpath `export/uvek_pilot_fae_routes_review.json`, SHA-256
`98fee8e678b40b6ea87f81c4276bc7b46d9f3367fa71e9ec5c2074299d4ea55a`.
Fresh readback binds 73 complete source/native inventories. Seventeen new
recipes retain the five routes; two existing stock recipes remain unchanged.
Every stock dependency carries its exact positive cut-off source profile and
profile-review hash. A bare empty dependency is rejected; no empty treatment
is introduced. The same 3.9 scrap-steel market recipe is shared between direct
demand and the reinforcement-steel route.

All 390 previous fingerprints, 3,368 recipes and 724 shared reviews remain
unchanged. Five exact root scopes and one shared review are added (395 total
scopes). No standalone stock or other-version scope is added. The complete
ordinary incineration model substitutes for FAE technology; it does not
reconstruct extraction or invent recovered-zinc/energy credits.

Independent semantic inspection verifies every candidate coefficient,
uncertainty and native target, all 67 non-FAE source terms, the 34 glass
emissions and both declared `WM_RP` bindings. Handovers retain −0.47289 kg of
iron in each version-specific incinerator and −0.31 kg of copper. Only verified
production-only stocks have negative foreground supply in physical treatment
checks. Caller and source inventories remain unchanged.

56 unique focused Brightpath cases pass: 26 passed initially and 30 signed
cases passed after correcting a test-fixture field name; the production
resource was unchanged by that correction. All 41 Premise cases pass, including
cold/warm caches and portable warnings. Five actual migrated graphs yield
finite native LCI, with native scientific hashes unchanged. Full generator
replay is byte-identical. Full pilot scenario/certification/export remains
unverified.

Private receipts: `fae_routes_fresh_primary.private.json`,
`fae_routes_staged_review.json`, `fae_routes_scope_registration.json`,
`native_fae_routes_lci_report.json`, `fae_routes_generator_replay.json`.

All five installed-wheel conversions pass. The inspected wheel, installed
stage, verified build copies and nine superseded cache files were removed;
the five current audited cache entries and all lockfiles remain.
