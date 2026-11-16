# Hydrogen trains: three source-3.12 market reconstructions

The required titanium-dioxide, silica-sand and hazardous-waste markets have the
same main quantities as their source-3.9 counterparts, but source 3.12 changes
the freight uncertainty parameters. Reusing the complete older market recipe
would silently retain the wrong marginal uncertainties. Three small new market
helpers retain the actual newer entries and reuse complete existing native
production or treatment services. No sector algorithm changes are required.

For titanium dioxide, preserve the separate chloride and sulfate route shares
and all three final-delivery entries. Both source and native production routes
have positive one-kg references. No elemental-titanium, purity, pigment-grade,
yield or allocation multiplier is introduced. Native route technologies and
internal burdens remain screening proxies.

For silica sand, preserve two ordinary producer entries, the allocated chemical
co-product entry and all five freight entries. The ordinary sources use native
German silica-sand production with geographic and process warnings. The chemical
co-product is not replaced by ordinary quarry sand: all fourteen nonzero source
3.12 input groups, including direct emissions, equal 0.0312 times the matching
allocated cryolite-product inventory. Use that independently verified burden
ratio with the complete native cryolite screening proxy. This allocates burdens;
it does not claim physical conversion of cryolite into sand or add a recovery
credit. Chemical route, allocation and upstream differences remain explicit.

For hazardous waste, the market and both source treatment variants have negative
one-kg references. Native disposal has a positive one-kg reference, so caller
factor **−1** is required. Preserve the two half-kg treatment portions and all ten
collection-freight entries. The source variants both specify 25% water and
17 MJ/kg net heating value and have no separately modelled energy credits. Use
the complete native 25%-water incinerator once per portion. Do not invent a heat
credit or apply a moisture/energy correction. Distinct source-portion comments
retain their identities without two redundant one-input treatment wrappers.
Energy accounting, residues, allocation, emissions and geography can differ.

Eleven native supply definitions were independently inspected in both UVEK
releases, with positive one references in matching units. The eighteen explicit
freight entries retain source-3.12 quantities, distributions and pedigree. This
does not preserve every supplier correlation or establish LCIA equivalence.
Native upstream biosphere exclusions and score effects remain unquantified.

Brightpath packages bounded primary witnesses, transformed native recipes and
the review in `export/uvek_hydrogen_train_three_markets_312_review.json`. Full
licensed source inventories stay private. Exact source-profile target overrides
preserve the previous physical recipes and sign decisions for older reviewed
profiles. Shared review text is updated to describe the separate version scopes;
Premise rebinds prior fingerprints only after proving those older physical
rules unchanged. A guarded maintainer replay hook preserves regeneration.
These decisions use the standing autonomous approval and add no consequential,
reverse or unrelated-version permission. The full train sector remains pending.

The actual six-root, 34-foreground train import now refuses only ferrosilicon
and mixed-plastic treatment in its first source cohort. Later source cohorts
have not yet been completed. No new sector is qualified: the operational count
remains twelve of nineteen. Selected tests pass 142 Brightpath cases and 45
Premise cases, including positive/negative callers, cold/warm cache reuse, exact
helper contents and prior approval guards. Clean generation reproduces three
resources byte for byte. About 198 MB of obsolete private generated files and
older wheels were removed; the current package and combined checkpoint remain.
No regular Brightway database was written. Numerical evidence is in
`uvek-hydrogen-train-three-markets-312-verification.json`.

The next ferrosilicon check is bounded to the two actual source-3.12 production
nodes. Their direct inert-waste markets are RoW and RER, with four and two input
entries respectively, and no clinker-treatment entries. The old source-3.9
co-processing omission approvals therefore must not simply be copied onto
these newer markets. Inspect the actual required inputs instead. Full source
manufacturing vectors and market records remain private.
