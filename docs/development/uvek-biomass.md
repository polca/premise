# Native biomass rollout

UVEK 2026 cut-off with the ecoinvent 3.10 biosphere supports native biomass
through the normal `NewDatabase` workflow. `update()` runs biomass before
electricity; `update("biomass")` selects it explicitly. Existing report/export
paths and ecoinvent behavior remain unchanged. Other 17 sector updates are still
gated while their implementations are developed under the expanded goal.

This is a screening implementation with explicit supply and process proxies,
not LCIA equivalence. Export metadata retains native-sector limitations even
without a CLIC manifest. Missing or all-zero IAM biomass data is recorded as
not applicable, without claiming a successful transformation or checking out
inventory; invalid finite/nonnegative inputs are refused before mutation.

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
other quantity changes. Each substituted pellet input retains its literal
original exchange and reference basis in the exported factory audit.

Planning resolves the entire supplier/consumer vector before changing inventory.
Validation checks actual linked IAM shares, dry-mass totals, producer inventories
and unchanged caller quantities. Missing/invalid IAM data cannot be reported as a
passing empty update. Unrelated legacy geographies such as CPA in material
production are not mapped or assigned invented geographic aliases.

The real encrypted REMIND SSP2-NPi 2050 default biomass/electricity run passes
semantic certification and Brightway export with 12,529 activities, 13 regional
biomass mixes and all 55 pinned CLIC records from the original nine roots.
Readback verifies actual IAM supplier fractions, five literal pellet source
audits, unchanged non-feedstock pellet coefficients, complete retained links,
warning metadata and three finite LCIs (pellet, EUR biomass and Swiss ORC).
Original source technosphere/biosphere scientific signatures remain unchanged.
Cache-disabled and warm-cache runs have identical biomass supplier vectors.

Relevant public integration regressions pass (146 focused tests, 54 shared
checks; groups overlap with smaller audit runs). Isolated installed wheels
pass 58 native regressions, all 41 reviewed electricity caller restorations,
exact unsupported-case policy and cross-model refusal checks, plus a real
default public update/certification/export and three-demand readback.
Receipt hashes and scientific boundaries are recorded in
[`uvek-biomass-verification.json`](uvek-biomass-verification.json) and the
[sector tracker](uvek-sector-progress.json). Private coefficients and licensed
inventories are not committed. These checks establish the biomass milestone;
the full expanded goal remains active for the other 17 sectors.
