# Mining material supplier batch

This batch qualifies twelve exact ecoinvent 3.8 cut-off correspondences for
UVEK 2026 with ecoinvent's 3.10 biosphere. No runtime Python module changes are
needed. The native database remains read-only.

Two chemically wrong matches are corrected. Refinery sulfur now uses recovered
elemental sulfur from the native European refinery, rather than petroleum coke.
Ordinary Portland cement now uses native Portland cement, rather than slag
cement, with the original three final-delivery freight rows kept separately.
Light-commercial freight uses the existing 3.5 t fleet service. The cement
proxy is explicitly imperfect: native CEM I 42.5 states 90% clinker, 5% gypsum
and 5% milling substances; the source CEM I description specifies 95–100%
clinker. Strength, composition, allocation, geography, vintage and score
differences remain unquantified. No clinker-fitting or transport multiplier is
introduced.

Three new release-specific recipes preserve the outer Swiss/non-Swiss water
shares, fossil ethanol production and delivery, and Portland-cement delivery.
Water uses each complete native delivered-water model once; source feedback
loops, treatment mixes, losses and returns are replaced rather than partially
added to native water. Ethanol retains its active-substance reference: one kg
of dry ethanol uses one kg of the previously inspected native chemical
reference, without a 1/0.997 multiplier or a biomass substitution. All original
freight quantities, distribution fields and pedigree data are preserved within
the ethanol and cement delivery recipes.

The other qualified counterparts are low-pressure vehicle-grade gas,
refinery-sludge incineration, delivered diesel, lubricating oil, Nylon 6,
packed milled quicklime, liquid ammonia and solid anhydrous ammonium carbonate.
Reference products, units and signs were inspected. Sludge has a source
reference of −1 kg and a native disposal reference of +1 kg, so its conversion
factor remains −1. Whole native inventories replace historical production,
delivery and allocation assumptions, with explicit proxy warnings. There is no
purity, density, capacity or recovery-credit adjustment.

Water, lubricant and ethanol rules already had approvals for other releases.
Seven prior scopes are refreshed only after checking that their complete
effective profile projections and earlier helper recipes are identical. The
old fingerprints and the refresh evidence remain in the approval ledger.
All 1,239 existing and new approval scopes validate. Fresh and cached
harmonization checks cover 3.6, 3.7, 3.8, 3.9 and 3.12; the matched audit warnings
retain each profile's own review. Historical authoring tests use historical
parent snapshots, without changing runtime drift guards. The deferred-material
fixture also restores a later 3.7 lubricant extension and omits unreferenced
archived review cards only in its historical copy; active resources retain them.

Verification: 30 migration tests, ten focused historical replay tests, five
fresh/cache Premise tests and 24 signed finite native LCI checks pass. The built
wheel contains the same review, resource and manifest bytes. These checks do
not establish source/native score equivalence, exact joint uncertainty
propagation or a complete licensed ecoinvent scenario build.

Real mining preparation reduces this first 3.8 group's refused rules from
sixteen to four. Municipal-waste routing, polyacrylamide, polyester fibre and
high-slag cement remain unapproved; their current matches would discard
important treatment, manufacturing or composition details. Later source groups
may expose additional work. Mining remains publicly gated until its complete
preparation, normal shared transformation, validation and export pass.

The packaged evidence is
`brightpath/data/export/uvek_mining_38_material_suppliers_review.json`.
Complete licensed source vectors remain private. Decisions follow the user's
standing autonomous approval and retain limitations for later review.
