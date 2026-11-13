# High-slag cement and ground slag: inspected 3.8 cut-off mapping

The previous cement match used a native 50%-slag product for a source containing
85% slag. The ground-slag match also added iron-production burdens that the
inspected source cut-off handover does not carry. Both are corrected for the
exact ecoinvent 3.8 cut-off source profile, using existing Brightpath recipes and
Premise YAML selectors. No runtime Python module changes are needed.

The cement helper uses the native CEM III/B finishing inventory, with the source
binder amounts: 0.85 kg delivered ground slag, 0.1425 kg clinker and 0.0075 kg
delivered mineral gypsum per kg cement. Their central sum is one kg. Source
binder uncertainty, pedigree and formulas are retained; this does not guarantee
mass closure for independently sampled coefficients. A fixed one-kg cement
parameter supplies the original formula basis.

The complete native no-burdens ground-slag process retains granulation, drying,
grinding and its own upstream supply. It replaces the source factory model,
including raw-slag transport and losses; these are not added a second time.
The separate source final delivery of 0.2 tkm per kg ground slag is retained.
No-burdens refers to upstream molten-slag allocation, not burden-free processing.

The mineral-gypsum helper uses complete native Swiss quarry gypsum and retains
all eleven original final-delivery freight entries, including uncertainty and
pedigree. This replaces the original stock mix, including its 0.0615% zinc
co-product, with a quarry proxy. No zinc credit, calcination or moisture
adjustment is added. Although the source cement comment mentions FGD gypsum,
its actual purchase is mineral gypsum; the mapping follows the exchange.

The cement prototype's two incoming freight rows are removed because the new
material helpers provide delivery. Its finishing fuel, electricity quantity,
waste heat, grinding aid, steel, plant and uncertainty remain native. Site
power uses the native US medium-voltage grid. Native fuel-burning MJ and source
useful-heat MJ have different bases; they are not equated or added together.
Native finishing technology, strength, energy, clinker geography, allocation,
vintage and freight fleets remain screening approximations with unquantified
score effects. Limitations are recorded in the approval audits and helper
comments. Full licensed source vectors remain private.

The exact materialized cement helper is added to `cement_markets.yaml`. The
ordinary shared cement filter selects it and its regional clones, excludes the
two delivery helpers and wrong products, and replaces its clinker supplier with
the ordinary clinker market without changing its 0.1425 kg dose or uncertainty.
No alternative cement algorithm is introduced.

Verification: 39 Brightpath migration/replay regressions, seven Premise
fresh/cache and adapter tests, and nine shared cement tests pass. Six signed
LCIs of the actual harmonized and adapted helpers are finite and reverse sign
consistently. The original 11,947-record Brightway source is unchanged; the
helper-only test database was deleted. Wheel review, migration resource and
manifest bytes match the checkout. Documentation reference generation and its
check pass. All 1,241 approval scopes validate.

The real mining preparation diagnostic now reports three unapproved rules in
its first source group, down from four: municipal-waste routing, polyacrylamide
and polyester fibre. Waste includes open dumping/burning and unsanitary disposal;
replacing the whole mix with incineration would discard important treatment
routes. These cases remain unfinished. Later source groups may expose further
work. Mining transformation, full validation and export have not yet passed;
its public gate remains closed. No complete licensed ecoinvent scenario build
was run for this mapping batch.

Packaged review: `brightpath/data/export/uvek_mining_38_high_slag_review.json`.
Decisions follow the user's standing autonomous approval, with limitations
retained for review.

The installed Git package repeats the eleven cache/adapter/cement checks and
the same three real mining refusals. A broader storage/schema/manifest run has
98 passing tests and one existing failure: eight archived review cards are
unreferenced. The exact same eight are present in the previous Brightpath
commit; this batch adds none. Their authoring-resource cleanup remains pending.
