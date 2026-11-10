# Finish UVEK integration through the shared Premise workflow

Goal activated on 2026-11-10. Baseline: `152fc82e` on `premise-CLIC`.
Use UVEK/BAFU 2026 with ecoinvent 3.10 biosphere, retain the public API, and
finish heat before operationalizing the other registered updates. Compact
storage and CLIC inventories remain in place. The active implementation uses
Premise's shared transformations, not the retired native-sector engines.

## Rules for implementation

Prefer Premise YAML aliases and Brightpath migration resources. Preserve product
boundaries, signed quantities, uncertainty and independently reviewed treatment
conventions. Each Python adaptation needs a reproduced failure and an explanation
of why mappings cannot fix it. Do not disable validation or call a sector finished
because its callback returned without touching eligible datasets.

Use actual encrypted REMIND SSP2-NPi 2050 builds initially. Extend to other
representative scenarios when needed to exercise applicable technologies.
Distinguish genuine IAM absence from missing native suppliers, missing CLIC
preparation, and filters that silently select nothing. Include auxiliary data
mappings outside the central sector registry in the audit.

## Starting evidence

The public update registry contains 19 built-in updates and the external-scenario
adapter. The UVEK scope guard currently admits only biomass, electricity and heat.
Several other constructors still explicitly request ecoinvent aliases or use the
source release as a biosphere release. Removing the scope guard alone is unsafe.

The actual filter audit reads the native source and the previously qualified
three-sector export, using `InventorySet` and the same role definitions used by
Premise. It does not count an empty declaration as coverage. Roles imported only
when another sector is prepared are not yet expected in the three-sector export.

| Mapping | REMIND roles | Roles matching the existing three-sector export |
| --- | ---: | ---: |
| Electricity | 21 | 20 |
| Fuels | 27 | 6 |
| Heat | 29 | 26 |
| Biomass | 2 | 2 |
| Carbon removal | 8 | 0 |
| Steel | 6 | 0 |
| Cement | 2 | 0 |
| Final energy | 217 | 0 |
| Cars | 30 | 0 |
| Two-wheelers | 6 | 0 |
| Buses | 4 | 0 |
| Trucks | 20 | 0 |
| Trains | 3 | 0 |
| Ships | 6 | 0 |

These are filter results, not a count of missing physical inventories. Some roles
are inactive, generated internally, or supplied by sector-specific CLIC roots.
For heat, the two district-supply roles use generated secondary markets, while
solar thermal is zero in this particular scenario. Renewable infrastructure,
batteries, metals, mining, emissions and external scenarios require additional
checks outside this mapping table.

## Heat: consumer filters

Reproduced failure: the earlier export contains regional heat markets but none of
959 reviewed native useful-heat inputs in material/service consumers enter those
markets: 870 industrial and 89 buildings inputs. The shared heat relinker has
hardcoded ecoinvent market names, so native suppliers cannot match. This matters
for real consumers such as battery-cell and viscose production.

Reuse the already approved source classifications in
`premise/data/heat/uvek2026_consumer_review.json`:

- Declare 58 distinct industrial and 26 buildings supplier name/product pairs in
  `iam_variables_mapping/heat_markets.yaml`. The reviewed source locations collapse
  to exact name/product pairs so the usual regional clones can also match.
- Extend the existing shared relinker to read those aliases. Keep its regional
  target selection, layered markets and normal graph relinking. Empty ecoinvent
  configuration retains the original default aliases.
- Match the name/product pair and useful-heat MJ together. Protect native energy
  inventories and CLIC/Brightpath foregrounds through explicit mapping fields.
  Steam, network-only services, ambient components, delivered district services
  and the zero-allocated MSWI heat output are not interchangeable with useful
  heat at the plant. District delivery remains a separate pending boundary.
- Record and warn about the previously reviewed grade, allocation, geography and
  supply-boundary approximations. Do not claim LCIA equivalence.

A mapping alone could not fix this because the shared callback never reads
source-family consumer identities. The necessary Python adaptation is limited to
loading those identities and applying the declared exclusions in its existing
relinker; no separate UVEK heat algorithm is introduced.

The audit also found 929 uncertain rows among those 959 purchases. Ordinary
compaction would merge 143 groups containing uncertain exchanges and retain only
the first distribution. The shared exchange summarizer must retain independently
uncertain or formula-bound rows; deterministic duplicates continue to compact.
This preserves the same technosphere coefficients while retaining statistical
information, and is a generic correctness fix rather than a UVEK algorithm.

## Verification log

- Initial heat/data/shared-adapter baseline: 46 passed.
- Heat aliases and existing family-map checks: 75 passed.
- Consumer protections, uncertainty regression and existing heat/transformation/
  electricity checks: 83 passed, two skipped. These groups overlap; counts are not
  additive.
- Final consumer-specific regression: 11 passed. Broad offline regression:
  3,425 passed, four skipped; this excludes licensed `test_process_*` integrations
  and the unchanged expensive `test_clic_uvek_*`, `test_clic_deferred_*`,
  `test_clic_rule_approvals.py` and `test_clic_partial_paperboard.py` batteries.
- All nine colleague-notebook code cells execute through the revised export.
  Independent comparison against the previous three-sector export verifies 870
  industrial and 100 buildings inputs, including regional biomass replicas, with
  940 uncertainty distributions retained (implicit positive signs and zero log
  locations may be made explicit by the existing writer). Signed heat totals
  agree per consumer/layer. Finite LCI for the three EUR heat layers, battery-cell
  production and viscose production. See
  [verification receipt](uvek-shared-heat-consumer-verification.json).
- Heat is not declared complete yet: delivered district-service boundaries and
  allocated conversion bases still need qualification.
- Independent constructor probes reproduce the next parameter-routing failure:
  both Cement and Fuels try to load biosphere release `2026` and raise the explicit
  UVEK-technosphere/biosphere-version error. Family/biosphere parameters must be
  forwarded as for the already adapted shared heat class; YAML cannot change a
  hardcoded constructor argument.
- A diagnostic cement inventory preparation (with the public scope guard bypassed
  only in a private inspection driver) fails on unreviewed compatibility rule
  `bp-a50439c1a11df652` for the ecoinvent 3.9 cut-off to UVEK 2025 edge. This is
  unfinished mapping/review work, not permission to disable the review gate.

Unrelated notebook, emissions-review and historical audit edits present at goal
activation are preserved. The emissions draft is evidence to review later, not an
automatic approval or an excuse to restore an alternative sector engine.


## Heat: retain the delivered district boundary

Checkpoint `57d0f9b8` fixes useful heat at the plant. The separate, previously
approved district-delivery decision is now mapped in `heat_markets.yaml`:
use the complete native Swiss delivery prototype, replace only its reviewed
plant-heat input with the region's secondary heat market, and retain the native
heat coefficient, network service, uncertainty and all other inputs. No extra
loss multiplier, pumping burden or heat credit is introduced. Generic delivery
names and source/limitation provenance make the changed generation basis explicit.

The shared Heat class delegates copying to its existing `fetch_proxies` helper
and uses the existing consumer relinker. The ecoinvent configuration is empty,
so this optional delivery adaptation does not alter the original route. YAML
alone cannot link a copied delivery service to IAM markets created at runtime.
There is no standalone native-sector engine.

An actual run exposed an existing helper bug: `fetch_proxies(relink=False)`
constructed copies but returned an empty dictionary. Fix the return assignment
so the documented flag disables upstream relinking, rather than discarding its
outputs. The default `relink=True` behavior is unchanged; a regression exercises
the real shared copier, not only a mocked delivery template.

Verification: 82 focused tests pass. The actual notebook run exports 13,430
activities, including 13 regional deliveries. All source heat/network quantities
and checked uncertainty fields are retained, all technosphere links resolve
inside the export, three material/service delivery inputs are relinked, and
1 MJ of NEU delivered district heat has finite LCI. The 970 ordinary heat inputs
and five representative heat/material LCIs were rechecked on this export.
See [delivery receipt](uvek-shared-heat-delivery-verification.json).

Remaining heat audit: preserve the independently documented conversion bases for
allocated fuel-cell heat and the reviewed methanol/hydrogen supplements; do not
infer physical efficiencies by inverting allocated inventory burdens or use a
generic calorific value where the reviewed source states another basis.


## Heat: documented conversion bases

Checkpoint `a5700068` retains delivered heat. The final active-role comparison
reproduced two smaller source-basis errors in the shared default calculation:
0.904522613 for methanol heat instead of the reviewed 0.9 (20 MJ/kg), and
0.931372549 for industrial hydrogen heat instead of 0.929745494 (120.21 MJ/kg).
The two allocated fuel-cell roles separately document 0.33 physical LHV heat
conversion, which cannot be recovered by inverting allocated fuel burdens.

Declare these six already-reviewed factors and their evidence under the roles'
`uvek_aliases`. The common final-energy-to-delivered-heat conversion reads an
optional documented factor, retaining its existing supplier-availability gate,
positive combustion-factor bound, volume conversion, normalization and diagnostics.
Absent a declaration, the original supplier-weighted inventory calculation runs.
The ecoinvent route retains that original calculation. No LCI exchange, allocation
or extra energy/heat credit is changed. This small generic hook is necessary:
filters alone cannot correct an inappropriate calorific value or an allocated
fuel-efficiency inference. Scalars match the recorded source review; inventory
or reference-basis changes require renewed review, not a guessed adjustment.

Verification: 88 focused tests pass; the broad offline group passes 3,443 tests
with four skips (same exclusions as above). All nine notebook code cells execute
through the 13,430-activity export. Actual EUR/NEU supplier selections use the
four active supplemental factors and retain complete supplier inventories. The
two fuel-cell factors have positive-volume conversion tests and prior native
source documentation; no MESSAGE scenario certification is claimed. The 970
ordinary heat inputs, 940 uncertainty distributions, three delivered inputs,
13 delivery services, internal supplier links and representative finite LCIs
were rechecked. See [heat qualification receipt](uvek-shared-heat-qualification.json).

Heat is qualified for the current shared three-sector REMIND workflow. Steam,
ambient components and network-only services retain their explicitly distinct
physical boundaries; they are not silently mapped to useful heat. Final
qualification alongside the remaining sector updates is still part of this goal.

## Cement preparation: monoethanolamine 3.9

The first preparation refusal (`bp-a50439c1a11df652`) is an existing reviewed
whole-monoethanolamine correspondence whose Premise approval covered only cut-off
3.6. Inspect the actual cut-off 3.9 GLO market and both RER/RoW ethanolamine
producers: all have positive 1 kg references, and producer product information
explicitly describes pure monoethanolamine. The native RER factory also has a
positive 1 kg reference and the previously reviewed mass-allocated product basis.

Extend only this operation's approval ledger with the inspected 3.9 profile,
primary metadata/inventory hashes, target reference check and exact rule/helper
fingerprint. Retain factor +1 and all existing allocation, market-to-factory,
geography, omitted native solid-waste and incomplete-emission warnings. Do not
repeat allocation, infer dilution, rescale yield or reconstruct market freight.
No runtime Python or Brightpath rule change is needed. The standing auto-approval
for analogous reviewed product/reference-basis cases applies; the evidence is
recorded in the ledger for later review.

Two focused checks pass, including fresh/cache harmonization, unchanged signed
amount/formula/uncertainty, non-mutation, and refusal to inherit this approval for
3.9.1 or consequential profiles. Cement preparation advances to a different,
still-unreviewed natural-gas heat correspondence (`bp-e4e35c0ff2032249`). Cement
is not operational yet; its shared family/biosphere routing also remains pending.


## Cement preparation: European natural-gas heat

The next rule (`bp-e4e35c0ff2032249`) incorrectly selected delivered district
heat for a plant-gate heat market. Inspect the actual 3.9 cut-off source: 54
positive one-MJ producer references, with no direct biosphere or ancillary inputs.
Compose its aggregate technology quantities from four previously reviewed native
heat-output services, retaining complete inventories and adding no district
network, loss correction, fuel-energy substitution or co-product credit.
Country mix, capacity/cycle, heat grade and allocation differences remain explicit
screening limitations. Only the inspected 3.9 cut-off profile is admitted.

Brightpath commit `4c0fddb439d944ca44624205f3f7b5214381085b` contains the frozen
mapping/review data, maintainer replay step and tests. No Brightpath runtime Python
module changes. New/existing gas-heat tests pass (36), resource/schema/manifest
regressions pass (101), full generation replays byte-identically and the wheel
contains the matching resource/evidence/manifest. All 530 prior Premise closure
fingerprints remain valid. Premise independently approves this new rule/recipe
only for 3.9 cut-off and pins the published Brightpath revision. Its two focused
Cement scope/cache checks pass; installed GitHub provenance and 531 total validated
scopes were checked in the user's regular environment.

The unchanged Brightpath alpha version caused pip to retain the older revision
on a normal upgrade. A targeted force reinstall fixes the existing environment;
clean installations already fetch the exact requirement. The colleague setup
and source guide now include this upgrade case. Restart kernels after replacement.

Cement is still pending. The next failed audit lists 11 remaining correspondences
in one cohort. Review those together by physical category rather than repeatedly
running the full preparation to reveal one refusal at a time. This does not relax
any gate: the audit already enumerates the unapproved operations.

## Cement preparation: eight supplies and onshore well construction

Inspect the actual 3.9 cut-off source references, market producers and native
2026 endpoints before admitting nine existing correspondences. All native
endpoints are nonempty positive-unit products/services. Pure liquid oxygen,
manufactured bitumen sealing and delivered tap water retain their kg basis;
the 10 MW diesel generator retains its fuel-input MJ basis. The latter uses
0.0234 kg diesel in the source versus 0.02336 kg in the complete native service;
this does not justify the factor previously reviewed for a different small
power generator. Market-to-factory and Swiss/European geographic approximations,
internal uncertainty replacement and unquantified score differences are explicit.

Solvent hazardous incineration, copper municipal incineration and bitumen
sanitary landfill match the documented material, wet composition and treatment
function. Reinforcement-steel dismantling has exactly 0.626 MJ of building-machine
diesel on both sides. These four negative source references become positive
native treatment-service references with factor -1. Do not add an energy or
metal recovery credit, or mistake dismantling for secondary-steel production.
The solvent descriptions interchange net heat/electric labels; no credit is
inferred from that inconsistent prose.

Onshore well construction keeps one metre, not a whole well. Despite inconsistent
prose about energy coverage, actual source/native inputs both contain 31.6 kg of
drilling-test petroleum, 200 kg cement, 210 kg reinforcing steel, 270 kg barite,
20 kg bentonite and 60 kg lubricating oil per metre. Keep the complete native
construction service and warn about waste fates, freight, cement grade, geology
and vintage; do not add ongoing field-operation energy, venting or flaring.

The Premise approval ledger records identities, complete inventory/metadata
hashes, reference checks, producer witnesses, decisions and warnings. Approvals
cover only the inspected 3.9 cut-off profile, not patch releases, consequential
or reverse conversions. No runtime Python or Brightpath mapping change is
needed for these nine cases. Eleven focused tests pass, covering their exact
scopes, fresh/cache conversion, signed amounts, formulas, uncertainty and
caller non-mutation, together with the two earlier cement decisions. Black and
Ruff pass for the test file.

Two cases remain in this cohort: extend the dedicated polyethylene endpoint's
reference review after verifying its actual 3.9 composition; replace the empty
concrete-disposal marker with complete inert-landfill operation and facility
services. Neither is approved by this batch. Cement preparation and the shared
sector update remain unfinished.

## Cement preparation: retained treatment burdens and successful preparation

Brightpath `aff7e30a6ebe6ba5cc41e5fa0ef3942a1f8c80ae` adds the two remaining
3.9 cut-off treatment decisions in this cohort. Polyethylene retains the
complete dedicated municipal incinerator: all 42 wet composition coordinates
and both heating values match the actual native 2026 service. Append exactly
3.9; preserve the old 3.6 target, factor and reference-review profile. Updating
the parent's closed fingerprint is necessary because it includes every profile.
Premise proves the old fingerprint against the frozen original parent before
refreshing only that operation's 3.6 closure. No blanket fingerprint refresh.

Concrete no longer selects an empty disposal marker. The helper retains 1 kg
of the complete native generic inert-landfill operation plus 1.48e-9 facility
units. Each original European source market has a single deterministic +1
supplier and no biosphere exchanges; only these wrappers are flattened. Keep
both source coefficients and their independent lognormal distributions,
pedigree and scale metadata. Use the previously reviewed Swiss operation and
facility functions as explicit geographic/background screening proxies; retain
heat, materials, waste fate, land-class, use/aftercare and vintage limitations.
No dry-mass, density, capacity, demolition, recycling, collection or recovery
credit is inferred. Physical exchanges are not omitted.

The frozen review, maintainer replay, packaged mappings, manifest and wheel
contents agree. No Brightpath runtime module changes. The final treatment and
incinerator group passes 22 tests; the wider treatment group had 151 passes and
two old-stage test expectations, subsequently corrected and included in those
22 passes. The resource/schema/generation group had 129 passes and two stale
storage assertions. Storage checks now pass all ten tests after updating
historical counts and validating evidence-bearing review structure independently
of a handwritten whitelist. Committed baseline counts (794 reviews, 3,993 direct
targets) already disagreed with those old assertions; this change adds one review.
Scientific correspondence authorities and exact decisions retain separate tests.
Premise's 13 focused cement approval/cache/sign/scope checks pass.

Actual encrypted REMIND SSP2-NPi 2050 **inventory preparation succeeds**, yielding
12 additional records (four capture/storage services and eight converted helpers).
This diagnostic bypasses only the public sector-availability guard; it does not
run or certify cement transformation/export. The next step remains shared Cement
family/biosphere routing and native clinker/cement selectors, followed by the
ordinary sector validation and a combined build. The public UVEK guard still
admits biomass, electricity and heat only. Existing three-sector notebook behavior
is unchanged. Premise pins the published Brightpath revision; existing kernels
must restart after replacement of the installed same-version alpha package.

## Shared cached handovers: retain consumer uncertainty

The combined cement audit reproduced an existing shared relinking defect.
Electricity updates cache a handover from the native country supplier to a
renamed IAM market. `relink_datasets()` looked up consumer uncertainty only under
the old name, so the new electricity input lost its distribution before Cement
started. A private stage trace confirmed the loss already at Cement construction;
no cement recipe or geographic split caused it.

A 17-line generic correction also indexes the original uncertainty under the
exact cached target identity. It changes no provider selection, signed amount,
share or market topology. Four normal/lognormal positive/negative reproductions
fail before the correction and pass after it; the related shared transformation,
heat-consumer and native cement group passes 61 checks. The corrected actual
four-sector export retains aggregate recipe quantities and the corresponding
scaled uncertainty. No new statistical correlation model is inferred when the
ordinary algorithm splits a supplier geographically.

This source change is necessary because a YAML alias cannot restore a lost
consumer distribution. It belongs to the existing shared transformation;
there is no UVEK-specific relinking engine. The four pre-existing Ruff findings
in that module remain identical to its committed baseline. The final broader
suite and combined graph evidence are recorded with the cement qualification.

## Shared cement transformation qualified for REMIND

The actual four-sector REMIND SSP2-NPi 2050 workflow runs without bypassing the
public availability guard and exports 13,695 linked activities. It contains 13
clinker markets (including the ordinary World aggregation), 24 regional clinker
producers and 182 cement producers across 14 grades. EUR's clinker market uses
the IAM's baseline/MEA-CCS shares; supplier product labels may remain native.
Seven representative cement/capture/electricity/heat LCIs are finite. All
technosphere links are internal. Original cement clinker doses and their
uncertainty remain unchanged; other recipe inputs retain aggregate signed
quantities and the normal geographic relinking's scaled distribution parameters.
The original 970 heat inputs and 940 uncertain heat rows were independently
rechecked, with five further heat/consumer LCIs.

YAML supplies five native/generated clinker-route aliases, their source-fuel
aliases, an exact positive-one-kg Swiss clinker template, 14 exact cement producer
pairs and explicit limitations. Native clinker is a rotary-kiln industry average;
the documentation does not prove a dry/wet subclass. Its use as the baseline for
the ordinary IAM route transformations is a screening proxy. Source grades,
clinker doses, non-clinker recipes, signed slag allocations and reference bases
remain distinct. No separate cement market layer is invented where the native
source has none.

Small source hooks are necessary: forward family/biosphere axes into the existing
Cement class; use the canonical biosphere release; let its existing template,
consumer and producer selectors read the YAML pairs; recognize capitalized hard
coal; and validate the actual mapped clinker producers rather than skipping them
because of ecoinvent-only labels. The shared efficiency, CCS, market, producer
regionalization and relinking sequence remains unchanged. Existing ecoinvent
selectors remain defaults. The separate generic cached-rename uncertainty repair
is documented above. No alternative UVEK cement module is introduced.

The public guard now permits explicit cement updates, while the no-argument
three-sector default stays unchanged. Other sectors remain guarded until they
are qualified. Focused native-selector, sign, energy-validation and original
cement/background regressions pass 57 tests. The shared handover group passes
61 tests. Final broad regression and evidence details appear in the qualification
receipt. Fourteen existing lignite-IGCC CO2 sanity warnings remain visible; do not
claim that all electricity validation findings are resolved. No error was found
in the recorded four-sector phases. No MESSAGE/IMAGE qualification is claimed.

See [cement qualification receipt](uvek-shared-cement-qualification.json). The
original three-sector colleague notebook remains usable; its guide explains how
to opt into cement in the same update call. Remaining registered sector updates,
additional IAM coverage and the existing CO2 warnings are still goal work.

Final broad offline regression: **3,456 passed, four skipped**, eight warnings,
76.44 seconds, with the same explicit exclusions as the earlier heat run.
Black and documentation-reference checks pass. No new Ruff findings: the
transformation and validation modules each retain their four committed baseline
findings. The BAFU source still contains 11,947 records and retains modification
time `2026-09-22T10:55:01.512934`.

An additional actual licensed ecoinvent 3.10 cut-off REMIND SSP2-NPi 2050
cement update succeeds with the original public API and default ecoinvent family.
Its recorded cement and contract phases have no findings. This is an update
regression, not an additional ecoinvent export/LCI certification.

## Remaining-sector dependency baseline

A diagnostic preparation of all 15 remaining built-in sectors reaches the first
3.9 cut-off cohort and reports **476 independently unapproved correspondences**:
372 heuristic matches and 104 curated/reviewed matches of other kinds. This is
an initial cohort count, not the total remaining across all versions or evidence
that every rule needs a new physical model. Public support remains the qualified
four sectors; the diagnostic only bypasses the preparation availability guard.

The batch contains clear product/function mismatches (pitch to paper-industry
pitch dispersants, nylon 6-6 to nylon 6, and holmium oxide to lithium manganese
oxide), as well as apparently reusable correspondences whose exact source
profile still needs inspection. Do not admit the whole cohort or infer validity
from a reference sign, confidence or product-name similarity. First isolate the
fuels dependency subset, review common materials/services by physical category,
and correct the wrong matches in Brightpath data. The already qualified sectors
must retain their closed approvals and regression evidence.

The isolated fuels preparation narrows its first refusal to **15 operations in
the 3.12 cut-off cohort**, rather than the all-sector cohort above. Review these
as product/reference-basis, utility delivery, waste-service and metal-boundary
groups. In particular, pure zinc must not silently become zinc concentrate, and
low-pressure delivered gas must not silently become gas at production. Existing
reviewed gas/water/wastewater helpers may be reusable only after confirming their
actual profile and inventory scope. Fuels preparation has not succeeded yet;
its shared constructor's family/biosphere routing also remains pending.

## Fuels 3.12: eight inspected existing correspondences

Inspect exact 3.12 source references/market producers and native 2026 references
before adding eight scopes: European primary aluminium, active NaOH, active
trisodium phosphate, virgin HDPE granulate production, film-extrusion service,
hazardous-waste underground deposition, and European medium/low-voltage grids.
Every selected native endpoint is nonempty with a positive unit reference.

The chemical names do not justify dilution factors. Source NaOH is explicitly
without water; the prior independent native chlor-alkali coproduct inspection
establishes an active-product basis despite the native 50%-solution title.
Source trisodium-phosphate production explicitly describes 100% active substance;
the native description also says all data refer to 100% acting substance despite
mentioning a 56% solution. Preserve factor +1 for both, not 2 or 1/0.56. Keep
source/native allocation and process differences explicit; the source caustic
mix is predominantly membrane, while native uses 55.1% mercury cells. No new
solvent, water-balance, hydration or repeated-allocation correction is invented.

Both film-extrusion descriptions define one kg of processing for 0.976 kg film
and exclude the converted resin. Retain factor +1 for the service and preserve
caller resin separately. The underground-deposit market has a negative source
reference while native Herfa-Neurode treatment is positive; factor -1 follows
those references, not the caller sign. Native long-term safety assumptions are
not proof of zero flooding risk. Market-to-factory, delivery, generation mix,
technology, allocation, geography, internal uncertainty, vintage and native
emission-coverage differences remain visible screening limitations.

The existing utility authority receives the two new 3.12 scopes; earlier
version-specific reviews and all prior closed scopes remain unchanged. Eight
fresh/cache harmonization checks pass, including signed amount/formula/normal
uncertainty, exact target, non-mutation and refusal to inherit these additions
for 3.11 or consequential. Ruff and diff checks pass. No runtime Python or
Brightpath mapping changes are needed for this batch. Seven other operations in
this fuels cohort remain unfinished, including profile-specific utility helpers,
product/delivery corrections and the market-average hydrogen boundary.
