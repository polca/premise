# Polyester fibre screening proxy and review-card cleanup

The previous mapping replaced polyester fibre with glass-fibre-reinforced
thermoset polyester. That is the wrong material and manufacturing route.
The inspected source instead makes PET fibre by melt spinning.

For the exact ecoinvent 3.8 cut-off source, use one helper combining one kg of
complete native amorphous PET granulate with one kg of the native PET fleece
conversion service. The latter explicitly excludes raw material, so this adds
PET exactly once. Its energy estimate comes from 1997 literature on polyester
yarn production. This is a screening proxy for a conversion stage, not an
identical fully oriented yarn or staple-fibre factory.

One kg feedstock per kg conversion is an explicit unity-net-yield assumption.
The helper replaces the source's India/RoW production portfolio with native RER
supply. It also replaces the source's gross polymer demand, spinning scraps and
internal recovery, water supply/returns, lubrication, waste treatments, direct
process emissions and internal uncertainty with the sparse native conversion
model. These differences and omissions have unquantified score effects.
The native conversion energy is retained without fitting it to the much lower
source energy. No extra source energy, recycling multiplier, dyeing, weaving,
glass fibre, thermoset resin or invented infrastructure is added. Caller signs,
formulas and supported uncertainty remain unchanged. All these limitations
appear in matched approval warnings and helper comments. No other source
release or system model inherits the approval.

The installed Git package repeats the fresh/cache and adapter checks and the
same two real mining refusals. Wheel review/resource/manifest bytes match.

No runtime Python module changes are needed. Brightpath's existing recipe
machinery and Premise's existing CLIC preparation/linking workflow are used.
The original source database remains read only. Full licensed source inventories
remain private; the packaged review contains reference facts, hashes and the
native-derived recipe.

The native helper passes positive and negative finite LCI checks after actual
harmonization and adapter linking. Its temporary Brightway test database was
deleted. Premise's fresh/cache warning checks and shared cement regressions pass.
The real mining preparation diagnostic now has two unapproved mappings in its
first source group, down from three: municipal waste and polyacrylamide.
Later source groups may expose more. Complete mining preparation,
transformation, validation and export have not passed; the public gate remains
closed. These checks do not establish source/native inventory or score equivalence.

The separately committed review-card cleanup archives eight superseded,
unreferenced cards outside the executable resource. All rules, helpers and
referenced reviews remain identical; all 1,241 pre-existing approval scopes
validate unchanged. The archive replay refuses changed cards, newly referenced
cards or unknown unreferenced cards before modifying anything. No validation
is relaxed. Two stale test expectations are updated: the reviewed direct-target
count is 4,020, and nickel 3.11 has a later independent approval, so its
negative profile test now uses the still-unapproved 3.10 release. The resource,
schema, manifest, archive and negative-profile checks pass (111 tests).

Packaged evidence:
`brightpath/data/export/uvek_mining_38_polyester_fibre_review.json` and
`brightpath/data/export/uvek_archived_unreferenced_reviews.json`.
Decisions follow the user's standing autonomous approval and remain available
for review.

## Remaining chemistry investigation

Native Sohio acrylonitrile is available, but native polyacrylamide is not.
Neither average organic chemicals nor polyacrylonitrile is an equivalent
polyacrylamide product. An open-access study by Farajzadeh et al. (2021),
[doi:10.1038/s41598-020-80369-z](https://doi.org/10.1038/s41598-020-80369-z),
provides material and energy tables for acrylamide and an oil-recovery polymer.
Its formulation, product basis, drying and auxiliary disposal boundaries need
checking before use for the source's water-treatment homopolymer. The paper's
exergy totals must not be entered as purchased heat or fuel quantities. This
is an investigation lead, not an approved mapping or a zero-burden omission.
