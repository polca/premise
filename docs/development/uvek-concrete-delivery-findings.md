# Concrete: preserve the delivery and strength-market layers

Scope: the nine ecoinvent 3.9 cut-off normal-strength concrete markets required
by the UVEK 2026 / ecoinvent 3.10 biosphere electricity pilot: BR, PE, AT, IN,
CO, RoW, RNA, CH and ZA. This inspection enables no mapping or new scope.
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

A future implementation should therefore retain the source market/renaming
forest and all original freight, and qualify complete native production
suppliers at its leaves. It should not replace the nine roots with bare
at-plant suppliers or reconstruct unrelated concrete applications.

Private evidence: `nitrate_concrete_grade_markets_primary.private.json`,
`nitrate_concrete_grade_market_findings.json`,
`concrete_delivery_closure_primary.private.json`,
`concrete_delivery_closure_findings.json` and
`concrete_delivery_topology_findings.json`.
Reproduction scripts: `inspect_nitrate_concrete_grade_markets.py`,
`inspect_concrete_delivery_closure.py`; the topology receipt records the
independent topological traversal and unmodified supply sums. No prospective
scenario, certification, export or native LCI is claimed for this unfinished
concrete category.
