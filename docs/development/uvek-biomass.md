# Native biomass rollout

The native dry-mass wood-chip and pellet-feedstock adapter is privately validated.
The public UVEK biomass gate remains closed until combined
updates, certification, export and packaged-resource checks are complete.
Electricity support remains available through the existing public API.

Three IAM entries now have `uvek_aliases` alongside unchanged
`ecoinvent_aliases`. Two procedural wood-chip entries have corresponding native
aliases. They select actual +1 kg references explicitly measured as dry mass.
Native forest-road chips proxy purpose-grown woody/energy-crop supply; clean
sawmill chips proxy forest residues. These are screening choices under the
standing autonomous approval. Cultivation, land use, species, harvest/collection,
chipping, coproduct allocation, delivery and vintage differ. Complete native
supplier inventories, carbon accounting and infrastructure remain. No new uptake,
recovery credit, drying inventory or guessed density/moisture factor is added.

The native RER production mix supplies positive historical weights within each
class. IAM shares determine the amounts between classes. Available native
suppliers within an IAM region retain their conditional historical weights;
regions lacking such a portfolio use the complete RER class portfolio as a
geographic proxy. Producer locations are retained, rather than relocating their
forestry inventories. Historical weighting is deterministic and does not forecast
supplier uncertainty. Native source-mix rounding is not changed: normalization
applies only to the new within-class allocation weights.

The new market supplies one dry kilogram. Matching fuel callers retain signed
kg, formulas, supported uncertainty and other quantity metadata exactly. Material
and byproduct users remain outside the stage. Explicit foreground links are not
replaced by name matches. Existing combustion emissions/efficiencies remain;
this is not a moisture-dependent combustion correction. IAM's common heating-value
convention does not establish a new mass/calorific conversion here.

The separately named forestry-residue process has a +1 MJ reference and a gross
calorific biomass resource flow. It is not relabelled kg. Combustion/heat services
also have MJ references and are excluded from raw-fuel aliases. Glued/composite
industrial residual wood is not substituted for clean forest or mill residues.
Pellet manufacture now retains its native process coefficients while replacing
only five verified dry-kg raw-feedstock links (chips, shavings and sawdust) with
the IAM wood mix. Binder, process energy, infrastructure, freight, packaging,
wastes, output and other direct flows remain. Particle size, milling/drying,
origin and allocation differ, with unquantified score effects; no extra processing
factor is invented. Precursors are scoped to pellet manufacture, not material
uses or allocated byproduct outputs. A full-factory snapshot checks that no
other quantity changes. Public rollout is still outstanding.

Planning resolves the entire supplier/consumer vector before changing inventory.
Validation checks actual linked IAM shares, dry-mass totals, producer inventories
and unchanged caller quantities. Missing/invalid IAM data cannot be reported as a
passing empty update. Unrelated legacy geographies such as CPA in material
production are not mapped or assigned invented geographic aliases.

Focused regressions: 77 tests pass across the new stage, native/default activity
mapping and CLIC root requirements. The real encrypted REMIND SSP2-NPi 2050
private stage creates 13 regional mixes and retargets 93 fuel exchanges. Its full
12,526-activity sparse matrix has complete links and finite LCI for the EUR
biomass market and the Swiss ORC electricity process; mixed row backward errors
are below 1.5e-15. Source technosphere/biosphere scientific signatures are
unchanged. These are private stage checks, not public certification/export.
Evidence hashes and remaining rollout work are in the sector tracker. Private
receipts include `native_biomass_real_stage_report.json`,
`native_biomass_stage_lci_report.json` and `native_biomass_stage_tests.log`.

Pellet-stage evidence: 80 focused tests pass. The real REMIND2050 stage creates
13 mixes and updates 98 fuel/feedstock links, including five inside the pellet
factory. Three private LCIs (pellet, EUR biomass and Swiss ORC electricity) are
finite with complete links on the 12,526-activity graph and unchanged source
signatures. Legacy ordered forestry selection explicitly ignores native-only
YAML entries, preventing an empty-filter match of unrelated ECI activities.
