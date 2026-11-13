# Polyacrylamide: public reactants with retained production services

The old mapping used an average of unrelated organic chemicals for
polyacrylamide. The reviewed replacement uses a public water-treatment model's
rounded reactant basis: 0.75 kg Sohio acrylonitrile and 0.25 kg water per kg
active polymer. The source is Gross et al. (2015), DEMEAU deliverable D51.1,
section 4.4.2.4, Table 4-7, printed page 71:
[Unique selling propositions](https://kompetenzwasser.de/media/pages/forschung/publikationen/911/fddcbc82b0-1702634140/Gross-2015-911.pdf).
Only these numerical facts, attribution and a document hash are packaged; the
report text and full licensed source factory inventories are not redistributed.

The public reactant model alone leaves out processing. The final helper retains
the inspected source's 4e-10 chemical-plant units, 0.333 kWh electricity and two
separate heat purchases totalling 2 MJ, with their supported uncertainty and
pedigree. It uses native organic-chemical plant and global medium-voltage grid
services. Each heat purchase uses its already reviewed exact 3.8 MJ-based
chemical-steam heat helper, retaining make-up water and physical water releases.
Those two helper definitions and their dependencies are frozen unchanged.
There is no kg-steam conversion, efficiency fitting or energy omission.

Reaction water becomes part of the acrylamide repeat unit; it is not 25% free
moisture or a dilute product requiring a dry-mass multiplier. Utility make-up
water has a different purpose. The rounded public recipe and native RER monomer
and tap water replace the source monomer mix, deionised-water country portfolio
and reactant uncertainty. This changes water quality, supplier geography,
allocation and the exact reactant ratio. The retained plant dose is not a
claim that native/source plant capacity, lifetime or technology are identical.

The market helper retains all four final-delivery freight rows separately, with
original quantities, uncertainty and pedigree. Native rail/barge/lorry services
use RER; transoceanic freight uses OCE. Fleet, vessel/container, loading,
geography and vintage differences remain explicit proxies, without distance
or payload corrections. Signed caller quantities, formulas and uncertainty
are unchanged at factor +1.

The source factory model is already sparse: catalysts/enzymes, process wastes
and direct emissions are excluded. These remain unrepresented, rather than
being declared physically zero or negligible. Native inventory/biosphere
exclusions, historical technology and uncertainty differences also remain.
This is a screening recipe with unquantified source/native score differences,
not a complete industrial process or a demonstrated equivalent inventory.

An EF v3.1 climate calculation in the native Brightway project gives 2.326 kg
CO2-Eq per kg for the two reactants alone and 2.773 kg CO2-Eq for the final
production helper (+19.2%), before final-market delivery. This comparison shows
why retaining processing matters. It compares two native-model demands; it is
not a source/native comparison, uncertainty estimate or impact bound.

Verification: eight new migration/replay tests and ten retained fibre/archive
regressions pass. Four Premise fresh/cache/adapter regressions pass. Eight
positive/negative LCI checks of the two new helpers and two reused heat helpers
are finite and reverse sign consistently. The original 11,947-record source is
unchanged, and the temporary helper-only Brightway test database was deleted.
Wheel review/resource/manifest bytes match the checkout. All 1,243 approval
scopes validate; no earlier scope is refreshed or broadened. No runtime Python
module changes are needed.

The exact ecoinvent 3.8 cut-off profile is qualified. Other releases,
consequential routes and inverse mappings are not inferred. The real mining
preparation diagnostic now reports one unapproved rule in its first source
group, down from two: municipal-solid-waste routing. Later source groups may
expose more. Mining's complete preparation, shared update, validation and export
remain unfinished; its public gate remains closed. No complete licensed
ecoinvent scenario build was run for this mapping batch.

Packaged evidence:
`brightpath/data/export/uvek_mining_38_pam_public_review.json`.
Decisions follow the user's standing autonomous approval, with assumptions
and limitations retained for review.

The installed Git package repeats the four cache/adapter checks and the same
single mining refusal. Documentation reference checks pass. Twelve obsolete
Brightpath Git-install wheel caches were removed (480 MiB); the current
qualified wheel and installed packages are retained. The temporary local
wheel-build directory was also removed after its byte checks.
