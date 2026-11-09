# Concrete: preserve the delivery and strength-market layers

Scope: the nine ecoinvent 3.9 cut-off normal-strength concrete markets required
by the UVEK 2026 / ecoinvent 3.10 biosphere electricity pilot: BR, PE, AT, IN,
CO, RoW, RNA, CH and ZA. All nine exact profiles are now implemented and verified.
Future modelling decisions are covered by the user's automatic-approval
instruction, while exact data and unit/boundary checks still apply.

Each required market buys one cubic metre from a renaming activity,
`concrete, all types to generic market for concrete, normal strength`. Those
activities buy grade-specific concrete markets. Thus the nine initially
supply-only wrappers do not establish an at-plant boundary. The upstream grade
markets contain distribution that must not be silently discarded.

Fresh read-only inspection covers 112 complete source inventories:

- 41 market/renaming nodes: nine roots, nine renaming activities and 23
  grade-specific markets;
- 56 complete concrete-production supplier inventories;
- 15 distinct freight-provider inventories, supplying 56 separate original
  market freight entries.

There are 88 concrete supply entries, of which 32 connect market/renaming
nodes and 56 buy concrete production. The 41-node market graph is acyclic.
All inspected references are positive one in the declared unit. The market
layers have no direct elementary exchanges, parameter definitions or exchange
formulas. Their cubic-metre supply shares sum to one within 2.23e-16; that is
an observation, not authorization to normalize them.

For example, Brazil's renaming activity retains 83% 25-MPa, 16% 30-MPa and
1% 35-MPa markets. The Swiss mix retains its separate 25-, 30- and 37-MPa
markets. Other source labels include 20 MPa, 25–30 MPa and 30–32 MPa. A
reconstruction must keep those original grade-market shares and uncertainty
separate, even if an accepted native production proxy is shared downstream.

The required delivery services are rail, light-commercial road, unspecified
lorry services and two >32-t EURO3 lorry services. They are already expressed
in tonne-kilometres. Their original quantities and uncertainty must remain at
that basis, without concentration, density, payload or distance multipliers.
Complete native same-mode services require exact identity/inventory witnesses;
a pure traction inventory or a vehicle-operation-only process is insufficient.
The >32-t class needs an explicit fleet-size/upper-class approximation if using
the native 32–40-t fleet. No separate transport infrastructure should be added
to a complete freight service.

The installed native database contains ordinary `Concrete, normal, at plant`
and more specific building/pile/civil-engineering variants. Resource-correction
and common-base-only variants are different model boundaries and must not be
selected as complete production merely because their names match. Any whole
production screening choice must explicitly retain binder, strength/class,
water/admixture, recycling/allocation, density, geography, vintage and internal
uncertainty limitations. No structural-performance equivalence or strength,
clinker-content or density conversion factor is established by this inspection.
The source producer inventories remain available for reviewing those choices.

The implementation retains the source market/renaming forest and all original
freight, with complete native production screening at its leaves. It does not
replace the nine roots with bare at-plant suppliers or reconstruct unrelated
concrete applications.

Private evidence: `nitrate_concrete_grade_markets_primary.private.json`,
`nitrate_concrete_grade_market_findings.json`,
`concrete_delivery_closure_primary.private.json`,
`concrete_delivery_closure_findings.json` and
`concrete_delivery_topology_findings.json`.
Reproduction scripts: `inspect_nitrate_concrete_grade_markets.py`,
`inspect_concrete_delivery_closure.py`; the topology receipt records the
independent topological traversal and unmodified supply sums. Full prospective scenario, certification and export remain separate unverified
gates; the component checks are reported below.

## Implemented screening model and verification

The production proxy is the complete Swiss `Concrete, normal, at plant`, at
one native cubic metre per original source cubic metre. It documents B 35/25
with CEM I 42.5, 300 kg cement, 190 kg water, 1,890 kg gravel and density
2,380 kg/m³. It includes manufacturing, internal transport, infrastructure,
wastewater and production waste; administration is excluded as documented.
Source binder/strength recipes and producer uncertainty are not reconstructed.
A shared native supplier introduces shared upstream uncertainty across source
branches; the source producers' distinct uncertainty models are not recreated.
These are explicit screening limitations, not structural-performance or
strength-standard equivalence. Source-grade labels on generated helpers retain
traceability and do not certify that the proxy meets that grade.

All original source market coefficients, supported uncertainty, node input
order and 56 freight entries remain unchanged. No shares are aggregated or
normalized. Freight uses complete RER rail/heavy-road, CH light-road and CH
32–40-t fleet inventories, with the stated country, technology, emission-standard
and upper-size-class approximations. No volume, density, strength, binder,
payload or distance multiplier, additional cement process, or recovery credit
is introduced. Source inventories and caller-owned quantities remain unchanged.
Native exclusions and all score effects remain unquantified warnings.

Packaged evidence: Brightpath
`export/uvek_pilot_nitrate_concrete_delivery_review.json`, SHA-256
`dd58c37af1c28a5b10c90d86191d9e7353402193e81a806b120be8b43dfba70a`.
Fresh readback binds all 112 source inventories and ten native release-specific
endpoint inventories. Nine root recipes and 32 auxiliary recipes retain the
41-node forest. All 381 prior fingerprints, 3,327 recipes and 723 shared reviews
remain unchanged. Nine scopes, 41 recipes and one review are added (390 total
scopes). Standalone grades, other releases and reverse/cross-model routes gain
no approval.

87 focused Brightpath and 73 Premise tests pass, covering signed uncertainty,
caller non-mutation, source-version refusal, generated-helper counts, portable
warnings and cold/warm cache behavior. Semantic comparison verifies every one
of the 32 internal links, 56 production entries and 56 freight entries against
primary readback. All nine actual migrated graphs yield finite native LCI,
with native technosphere/biosphere scientific signatures unchanged. Full
generator replay is byte-identical. These checks do not establish comparative
LCIA or complete the electricity scenario/certification/export.

All nine installed-wheel conversions pass. Wheel, installed stage, verified
build copies and nine superseded harmonization files were removed, preserving
the five current audited cache entries and all lockfiles.
