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

## Fuels 3.12: corrected delivery, product and market interfaces

Brightpath `6d24057f6b06aa8150b8fb5e2c0925e1d21a1e5d` supplies seven exact-profile
decisions in frozen migration/review data. No Brightpath runtime module or
Premise sector Python file changes are needed for this preparation batch.

- High- and low-pressure gas retain the original cubic-metre caller basis using
  **36 MJ** of the corresponding complete native gas-at-consumer service. Actual
  3.12 product information explicitly supplies that NCV at 15 C/1013 mbar. Do not
  reuse the 3.6 legacy 38.256575270163715-MJ calibration, infer equal Sm3/Nm3
  states or select production-gate gas for a delivered low-pressure purchase.
- European water retains its actual non-CH/CH shares and source order. Wastewater
  retains all three route coefficients and 43 direct emissions. The only changed
  outer-vector label relative to 3.9 is Strontium -> Strontium II with the same
  publisher UUID, kg, surface-water compartment and quantity; the existing
  helper already uses that canonical 3.10 definition. Complete volume-based
  sewer/treatment proxies retain their known model limitations.
- AE7 retains its three coconut/palm-kernel/petrochemical shares and all three
  barge/lorry/train freight quantities through the corresponding native services.
  A coconut-only factory does not represent that portfolio. Zinc metal now uses
  complete primary SHG zinc, including refining, instead of concentrate.
- Hydrogen receives an explicit kilogram-based market helper. Native Swiss
  25-bar SMR is its disclosed baseline screening proxy for a source portfolio
  containing 88.65867411151911% SMR and the remainder industrial/refinery
  co-product hydrogen. Pressure, allocation and geography differ; no new
  compressor, liquefaction, heat or avoided-production credit is inferred.
  **This interface must still follow the ordinary future IAM hydrogen market.**
  Preparing the helper alone does not establish that link or qualify Fuels.

Earlier selected 3.6 gas and 3.9 water/wastewater targets, factors, reference
reviews and complete helper closures are proven unchanged before refreshing
only their three closed parent fingerprints. Seven new 3.12 approvals bring the
validated unique scope count to 557. Their separate ledger entries cite the new
packaged review; no old authority is broadened implicitly. Uninspected releases,
consequential and inverse routes remain refused. Native model, country mix,
allocation, vintage and biosphere differences remain warnings, without claimed
LCIA equivalence or new physical omissions.

The new dependency/storage group passes 44 tests, and 15 Premise scope/sign/cache
cases pass. The earlier water/wastewater/gas runtime checks pass; their replay
fixtures now use the correct historical stage, including three component rules
already superseded by older heat/steel reviews before this fuel work. Fourteen
focused authoring/staging checks pass. Source rules and review cards are restored
from their frozen evidence for these tests, not waived or rewritten in runtime
resources. The obsolete gas review card remains in the frozen evidence and is
removed only from the active runtime index when unreferenced. Full generation
replays byte-identically, canonical biosphere data remain unchanged, the wheel
contains exact evidence/resources/manifest, and Black/Ruff checks pass.

Actual fuels preparation clears this 3.12 cohort and reaches **51 independently
unapproved 3.9 correspondences**. These include new material/function checks and
version extensions; molybdenum trioxide -> concentrate is a visible boundary
mismatch requiring correction. This is not a fuel-sector success. The shared
Fuels constructor also reproduces a version-axis error even when explicitly
supplied `family="uvek", biosphere_version="3.10"`, because its own lookup still
uses technosphere release 2026. Family/filter and biosphere forwarding hooks,
market-helper relinking, subsequent cohorts, normal validation and export remain
unfinished work. The public qualified scope remains the four existing sectors.

The pinned companion is installed in the regular premise environment and all
557 closed scopes validate. An actual public four-sector rebuild with the new
resources still has 13,695 activities, 13 clinker markets and 39 electricity
markets, with no validation errors and the same 14 existing IGCC warnings. No
additional export is created for this regression. See the
[fuel preparation receipt](uvek-fuels-312-preparation-verification.json). The earlier full export/LCI
qualification remains the separate cement receipt.


### Seven 3.9 fuel material/facility reviews (10 November 2026)

The actual ecoinvent 3.9 cut-off inventories and native 2026 providers support
seven narrowly scoped correspondences: rigid polyurethane foam, bulk PVC, RER
and RoW hot-rolled 18/8 stainless steel, an organic chemical plant, an organic
chemical storage facility, and milled ferric oxide (Portafer). Every source and
native reference is positive one in the same unit, with a nonempty native
inventory. The steel includes hot rolling; no extra rolling service is added.
The storage unit represents the complete four-tank, 16,000-m3 facility, not one
tank or one cubic metre. Plant and storage lifetime assumptions do not introduce
annual-output or lifetime scaling. Both PVC descriptions explicitly use a
suspension/emulsion production mix as their bulk-PVC approximation.

The ledger retains primary inspection hashes, producer witnesses and explicit
market-delivery, geography, process-model, allocation, vintage, uncertainty and
biosphere-coverage limitations. Original source inventories/comments remain
private. Existing 557 scopes are unchanged; seven additions give **564 validated
unique scopes**. No production Python, YAML or Brightpath resource is changed.
No approval is inferred for different releases, consequential models, or the
physically mismatched chemical correspondences that remain under review.

Twenty-six focused scope/sign/formula/uncertainty/fresh-cache tests pass, including
all seven new cases and the previous 3.12 batch. Black and Ruff pass for the
changed test file. A real licensed preparation rerun confirms all seven clear:
**44 unapproved 3.9 correspondences remain, down from 51**. This diagnostic bypasses
the public sector-availability guard only to inspect preparation; it performs
no fuel transformation or export. Fuels remains unqualified, and the four-sector
public scope is unchanged. The full goal remains active. See the
[preparation receipt](uvek-fuels-39-material-preparation-verification.json).


### Eight 3.9 treatment, supply and infrastructure reviews (10 November 2026)

Primary source/native inspection supports complete potable-water anion-resin,
mixed-plastic and PVC municipal incineration services. Their source references
are -1 kg of waste and native references +1 kg of service: factor **-1** preserves
a positive disposal burden for an ordinary negative ecoinvent waste demand.
The corresponding waste compositions, moisture contents and heating values are
documented, while net energy, allocation, emissions and residue models differ.
The historical source ordinary-incineration records describe later replacement
by extraction technology; these identities do not represent separately named
fly-ash-extraction processes. No extra recovery or energy credit is introduced.

Five further +1 correspondences cover packed milled quicklime, deionised water
from ion exchange, a km of high/middle-pressure gas-distribution infrastructure,
a complete gas service station, and allocated diethanolamine. Actual plant and
station capacities/lifetimes agree; no annual-output or lifetime multiplier is
applied. The nonempty native diethanolamine inventory retains its documented
40/40/20 mass allocation and its legacy `xx` title. Feed-water quality, chemical
production, geographical, delivery, vintage, allocation, biosphere coverage and
uncertainty limitations remain explicit. Original proprietary inventories stay
private; the ledger retains primary hashes and inspected reference bases.

All previous 564 scopes are retained; the total is **572**. Thirty-four focused
scope/sign/formula/uncertainty/fresh-cache tests pass, including the preceding
3.12 and 3.9 batches. Black and Ruff pass. The actual licensed preparation rerun
confirms all eight cases clear, leaving **36 unresolved 3.9 correspondences**,
down from 44. This diagnostic does not run fuel transformations or export.
No production Python, YAML or Brightpath resource changes are needed for this
batch. Fuels remains unqualified and the full goal remains active. See the
[service preparation receipt](uvek-fuels-39-service-preparation-verification.json).


### Necessary shared-fuel version-axis correction (10 November 2026)

A direct shared `Fuels` constructor call with `family="uvek"`,
`version="2026"` and `biosphere_version="3.10"` failed because the constructor
still looked up biosphere flows using 2026. The callback also rejected the
explicit family/biosphere arguments. New regressions reproduce three failures
before the correction; the two default ecoinvent constructor cases already pass.

The two small production adaptations follow the existing shared heat/cement
pattern: `NewDatabase` forwards the two axes to the original fuel callback;
`fuels/base.py` passes them to the existing constructor, selects the appropriate
`InventorySet`, and uses the biosphere axis for flow lookup and the existing
vehicle-carbon refresh. YAML cannot repair a runtime lookup using the wrong
version argument. Ecoinvent defaults and all generation, blending, carbon,
regionalisation and relinking algorithms remain in the shared engine. No
alternative UVEK engine, dataset-specific Python branch or new public argument
is introduced. The unqualified public fuel gate stays closed and is tested.

The actual regular conda environment now constructs UVEK 2026 fuels with
3.10 biosphere definitions (4,362 flow keys) and the native UVEK filter family.
This is a constructor check with an empty inventory, not an actual fuel-sector
update. The focused fuel/carbon/filter/database suite passes 244 tests. After
adding dispatch and gate cases, the broader offline regression suite passes
**3,464 tests**, with four skips and eight dependency/data warnings. Licensed
process and separate migration-heavy suites are explicitly excluded from that
number; the separate 34 reviewed-mapping checks above pass independently.
Black, Ruff, diff whitespace and generated-reference checks pass. See the
[axis correction receipt](uvek-fuels-background-axes-verification.json).

The next correspondence work must correct three physically wrong chemical
targets: activated silica is chemically precipitated material, not mined sand;
the granular activated-carbon market combines virgin production and reactivation,
not carbon-black manufacture; and molybdenum trioxide requires roasting rather
than concentrate alone. Actual primary source producer vectors and native
candidate evidence have been collected privately to resolve their complete
process boundaries. The European low-voltage market mapped to a Swiss native
grid likewise needs a corrected regional target before approval. None of these
remaining mappings is approved by the runtime correction.


### Three corrected 3.9 native endpoints (10 November 2026)

Brightpath `92e89c4acdb7d2904c97ef82b7856d93f6deacc7` corrects the European
low-voltage market's Swiss-only target to the complete RER grid; solid sodium
silicate's spray-powder target to the solid furnace process; and polyurethane
waste's construction-transport wrapper to the dedicated incinerator. Only the
inspected 3.9 cut-off identities qualify. The waste factor is -1; grid and solid
silicate factors remain +1. Actual source/native sand, soda and steam quantities
match per kg of furnace silicate. Neither the common erroneous phosphate prose
nor generic formula metadata justifies an invented concentration, hydrate or
stoichiometric adjustment. Grid geography, source-market delivery, technology,
allocation, emissions, residue, vintage and uncertainty limitations stay explicit.

All previous 572 approval scopes and their complete ledger operations remain
unchanged. Three additions give **575 validated scopes**, verified against the
Git-installed companion in the regular conda environment. No runtime Python
module changes occur in either repository. One maintainer-only frozen-review
replay stage and its tests accompany the mapping data. Full generation is
byte-identical, the canonical biosphere is unchanged, and the built wheel
contains the exact review, resource and manifest. Black and Ruff pass for the
new replay/test files; the existing generator's two import-order Ruff findings
are reproduced before and after its three added stage lines.

The endpoint/previous-fuels/storage group passes 67 checks; Premise's exact
scope/sign/uncertainty/formula/fresh-cache group passes 37. The broader Brightpath
reference-sign suite passes 731 checks and fails one historical hard-coded count
assertion. The **previous committed companion also fails that same assertion**
(1,183 exact-profile rules versus its expected 996; the new total is 1,186).
This stale cross-review assertion remains open and is not hidden by a validator
waiver or a raised expected count. A proper historical-stage/current-review test
repair remains required before full-goal completion.

All three complete native providers have finite positive-one LCIs and supply
arrays. The original BAFU source remains at 11,947 activities with its modified
timestamp unchanged. These are native supplier checks, not prospective fuel
market/export qualification. The actual licensed preparation rerun clears all
three corrected rules and reduces this cohort from **36 to 33** unapproved
correspondences. The diagnostic bypasses only the public availability guard to
inspect preparation; no fuel transformation or export is run. See the
[endpoint preparation receipt](uvek-fuels-39-endpoint-preparation-verification.json).

Next: reconstruct the actual activated-silica, granular activated-carbon and
molybdenum-trioxide boundaries instead of approving sand, carbon black or ore
concentrate. Component inspection also finds a separate RER solid-silicate
market still mapped to spray powder; it has no Premise approval and must be
corrected before that chemical reconstruction is admitted. Further preparation
cohorts, fuel/IAM market linking and remaining registered sectors stay in scope.

Removed 116.6 MiB of obsolete private wheel/generator-replay artifacts after
verification; active caches, primary inspection evidence and Brightway databases
are retained.


### Activated silica, three ingredients and sign-test repair (10 November 2026)

Brightpath mapping commit `5a14c2c` replaces activated silica -> mined sand with
one explicit kg-based reconstruction. All 14 original input rows, five direct
water-related biosphere exchanges, 19 lognormal distributions and four local
parameters survive. Both separate silicate purchases use the complete solid
furnace product; no shares or coefficients are renormalised. Four negative
wastewater demands become positive treatment-service inputs through their
reviewed factor -1, preserving uncertainty. Thirteen existing steam/wastewater
helper recipes and their complete closures remain unchanged. Two native
waste-energy co-products inside the frozen wastewater plant are intentionally
empty under its documented 100%-disposal / 0%-energy allocation. They are neither
new zero-inventory substitutes nor extra recovery credits.

The RER solid-silicate market is independently corrected from 80% spray powder
to furnace-process solid product, based on matching primary sand/soda/steam
quantities. The global chemical-factory market uses a complete plant unit with
no lifetime or annual-output scaling. RoW sulfuric acid uses complete native
100%-active acid per kg, replacing a source portfolio that includes recovered
co-product acid. No concentration/hydrate multiplier, extra solvent, dry yield,
neutralisation, water balance or new allocation/recovery correction is inferred.
Native heat-network/industrial-heat boundaries, geography, grade, allocation,
delivery, vintage, internal uncertainty and biosphere differences remain
explicit screening limitations with unquantified score effects. Ingredient
bindings inside this closed helper do not approve unrelated source inventories.

All previous 575 scopes and complete approval operations are unchanged; four
additions give **579 validated scopes**. No runtime Python module changes occur
in either checkout. One maintainer-only replay stage and tests accompany the
frozen mapping/reconstruction data. Full generation is byte-identical, the
canonical biosphere is unchanged and the wheel contains exact evidence, mapping
and manifest. Black and Ruff pass for changed replay/tests; the generator's two
existing import-order findings are unchanged by its three added stage lines.

The focused Brightpath group passes 65 tests, with an additional strict-target
closure check passing. Premise passes 41 fuel-mapping/scope/cache checks. Stronger
per-field comparisons independently confirm all 19 rows' amounts, uncertainty,
pedigree fields and caller formulas, plus all parameter values, through fresh
and cached conversion. The broader **733-test reference-sign suite now passes**. Its old
count failure is repaired by replaying the original 996-rule authoring stage
without changing the count, sign or quantity assertions, while a new full replay
compares the complete current runtime resource to every frozen authoring stage.
This preserves current coverage rather than waiving validation or raising a
historical expected count. The test-only repair is Brightpath `09b84e9`; its
runtime and data are exactly those of the licensed probe's `5a14c2c` revision.

A native-matrix check expands the root and 13 child recipes into 38 native
supplier demands plus 130 distinct direct biosphere identities, all matching
the installed 3.10 biosphere. Supply and combined inventory are finite for 1 kg.
No database is written. After actual licensed preparation, the source still
has 11,947 activities and the unchanged 22 September modified timestamp.

The licensed preflight accepts the three previously blocked current-cohort
rules, reducing this cohort from **33 to 30** unresolved correspondences. The
additional RER ingredient scope is verified separately in direct fresh/cache
migrations and strict helper validation. It remains a diagnostic
preparation run, not a successful prospective fuel transformation or export.
Premise pins and the regular conda environment use companion `09b84e9`, whose
Git provenance, resource digest and all 579 scopes are checked. Only tests/docs
differ from the probe revision, so its runtime/data results carry unchanged.
Removed 116.7 MiB of obsolete private wheel/replay artifacts; active caches,
primary source evidence and Brightway databases remain. See the
[silica preparation receipt](uvek-fuels-39-silica-preparation-verification.json).

Next: granular activated carbon and molybdenum trioxide, plus the other 28
current-cohort identities, further preparation cohorts, shared fuel/IAM linking
and the remaining registered sector updates. The full goal remains active.

### Ash dependency review for granular activated carbon

Primary inspection of the two ecoinvent 3.9 cut-off ash markets distinguishes
allocation-empty receivers from actual disposal. The Swiss market sends
46.9906787% to five empty cement receivers and 53.0093213% to incineration with
fly-ash extraction. The RoW market sends 21.1641095% to 46 empty cement,
concrete and brick receivers, 77.9874560% to residual-material landfills,
0.6408694% to sanitary landfill and 0.2075652% to incineration. These are
source fractions, not a new normalized disposal mix. Both markets have -1 kg
references; all 51 empty receiving inventories also have -1 kg references,
zero price and no nonproduction exchanges. Sixteen other receiving inventories
have actual disposal burdens. The two market inventories contain eleven
freight rows in total, which must remain separate.

The existing three-fate RoW candidate is not a complete source-market
reconstruction and must not be used as one. Preserve the original disposal
fractions and freight. The standing autonomous-review authorization permits
an explicitly audited omission of each proven empty receiving inventory:
this preserves its zero source LCI without inventing a native treatment,
recovery credit or positive-reference recycled stock. Retain its identity,
quantity and primary inventory hash in the eventual conversion audit. This
avoids broadening Brightpath's positive-reference zero-stock validation to
negative-reference services. Nonempty disposal branches still require complete
native suppliers and explicit technology/geography warnings, including the
Swiss fly-ash-extraction difference. No new runtime change is justified by
these findings.

The [primary-source receipt](uvek-fuels-39-ash-investigation.json) records the
51 independently checked empty receiver identities and hashes, market hashes,
signed group totals and row counts. Its generation asserts reference signs,
zero prices, strictly empty nonproduction inventories, positive freight and
one-kg market balances. Raw licensed inventories remain outside the repository.
This is a reviewed implementation decision, not an accepted mapping, lower
unresolved-case count or successful fuel-sector run. Next implement the exact
ash closure alongside granular-carbon production/reactivation; independently
reconstruct MoO3 roasting with its source molybdenite supply portfolio.

### Finished molybdenum oxide: licensed fuel preparation advances to 29 cases

Brightpath `3019e58` replaces the incorrect oxide-to-concentrate correspondences
with three complete helper stages: source mineral market, roasting and delivered
oxide market. Seven mineral producer rows retain the source primary/co-product
shares, including 17.8813867% primary mining, rather than adopting the older
native market split. Complete native main-product and copper co-production
concentrates are explicit stock-mass screening proxies. Their purity and a
contained-metal conversion are not established; no speculative Mo-content
multiplier is introduced. All eight original freight rows across the two
supply-chain stages remain separate.

Roasting retains 1.112 kg mineral supply, 1.665232 MJ natural-gas heat,
1.333e-11 blast-furnace capital units and 0.0623 kg direct sulfur dioxide per
kg oxide. Native district heat has explicit utility-boundary/geographic
limitations. The source is a rough estimate: no new theoretical sulfur balance,
acid recovery, metal refining, extra fuel or omitted source-process correction
is invented. Caller quantities, uncertainty and formulas survive.

Three exact 3.9 cut-off scopes give **582 validated scopes**; every earlier
approval operation remains unchanged. Other versions and consequential remain
unapproved. No runtime Python modules change. Frozen review data, a maintainer
replay, mapping resources, tests and installation/documentation pins accompany
the change. Brightpath and the regular conda environment use the pushed Git
revision; the colleague notebook remains unchanged.

The new/adjacent Brightpath tests pass 58 checks, and two full frozen-resource /
historical-stage replay checks pass. Complete generation is byte-identical,
canonical biosphere unchanged, and wheel data match the review/resource/manifest.
Premise's full helper fresh/cache check passes, validating all closed scopes and
per-field quantity/uncertainty/formula retention. Black/Ruff pass for changed
replay/tests. Native-matrix expansion includes three helpers, eight native leaf
demands and one direct biosphere term; supply and combined LCI are finite.

Actual licensed preparation accepts the finished-oxide rule, reducing the
current cohort from **30 to 29** unapproved correspondences. The two nested
production/mineral rules are verified separately in strict migration and cached
preparation; they are not additional current-cohort requests. Other mappings
still stop preparation: no prospective fuel update or export is qualified.
The source remains at 11,947 activities and its unchanged September timestamp.
Removed 116.7 MiB of verified obsolete generated wheel/replay files, retaining
receipts, active caches and primary evidence. See the
[molybdenum preparation receipt](uvek-fuels-39-molybdenum-preparation-verification.json).

Next: implement granular activated-carbon production/reactivation and its full
ash-service split, then the other 28 current-cohort cases, further preparation
cohorts, shared fuel transformation/linking and all remaining sectors. The full
goal and original Premise workflow requirements remain active.

### Granular activated carbon: full production/reactivation and ash split

Brightpath `b4161c0` replaces carbon-black/fibre/mercury-disposal matches with
seven reviewed helper recipes: four granular-carbon production/reactivation
inventories, their market, and the Swiss/RoW ash markets. Source market shares
remain 31.6% virgin production / 68.4% reactivation, with RER and RoW entries
separate. All four product references are positive, including reactivation.
Each retains 72 direct biosphere entries and three parameters; parameter units,
uncertainty and pedigree survive. Together the four inventories contain 331
lognormal exchanges and two deterministic spent-carbon stock inputs. The
existing reviewed positive-reference spent-carbon cut-off stock and its full
freight remain unchanged; reactivation itself is not made burden-free.

Virgin production keeps the original three kg of coal feedstock and all country
rows. Complete native regional-storage supplies replace mine-only candidates;
grade, moisture, ash, delivery, technology, allocation and geographic proxies
are explicit limitations, without an invented LHV/carbon/yield multiplier.
Native complete electricity, heat, water and infrastructure inputs remain.
All 288 direct biosphere entries match the 3.10 publisher UUID, compartment and
unit; 24 updated labels are identity renames, not substance/ionic-mass proxies.

The two ash-service helpers preserve all sixteen nonempty receiving processes
and eleven freight rows at source quantities. Disposal fractions are not
renormalized. Exactly 51 independently verified production-only, zero-price
negative-reference receiving inventories are omitted with explicit source
identity, quantity, uncertainty, index and inventory-hash records. These use
the existing recipe `source_input_omissions` mechanism: helper provenance and
migration warnings retain every record, and Premise requires their exact
separate approvals. Removing those approvals is tested to refuse conversion.
No nonempty receiving inventory is omitted, no recovery credit is added and
positive-reference zero-stock validation is unchanged. Native Swiss incineration
lacks fly-ash extraction; technology, residue, allocation and emission differences
remain explicit warnings. Negative ash references become positive services with
factor -1 and consistently scaled uncertainty/formulas; positive freight remains
positive. Score effects of the native proxies remain unquantified.

An independent primary-source comparison checks all 419 source nonproduction
rows: 368 retained target rows and 51 explicitly audited empty receivers,
including every quantity, uncertainty, formula, bound and pedigree field, plus
all twelve parameters and nested source-identity links. The native-matrix check
expands eleven new/existing helpers into 53 native demands and 72 distinct direct
biosphere identities; supply and combined inventory are finite. It writes no
database. The source still has 11,947 activities and its unchanged September
modified timestamp.

The final Brightpath verification set passes **843 tests**, including the full
733-test reference-sign suite and current frozen-resource replay. Premise passes
three fresh/cache, explicit omission-refusal and neighbouring oxide checks.
Seven new closed 3.9 cut-off scopes give **589 validated scopes**; every earlier
approval operation remains unchanged. Existing non-3.9 profile entries on the
RoW ash rule remain unchanged and receive no new approval. Full generation is
byte-identical, canonical biosphere unchanged, and wheel review/resource/manifest
bytes match. Black/Ruff pass for changed replay/tests. No runtime Python modules
change in either checkout. Installation and colleague/user guides pin the pushed
Brightpath revision; the regular conda Git installation is verified.

Actual licensed preparation accepts the granular-carbon market, reducing the
current cohort from **29 to 28** unapproved correspondences. Other mappings still
stop preflight, before generating this closure; completed helper audits and all
51 omission warnings are separately verified through strict fresh/cached
conversion. This is not a successful prospective fuel transformation or export.
Removed 194.9 MiB of superseded/verified generated replay and wheel artifacts,
retaining logs, source evidence, active caches and databases. See the
[carbon preparation receipt](uvek-fuels-39-carbon-preparation-verification.json).

Next: the 28 current-cohort correspondences, beginning with perimeter-insulation
polystyrene foam and metallurgical alumina; further preparation cohorts, shared
fuel transformation/linking and all remaining registered sectors still follow.
The full goal remains active; eligible missing mappings are unfinished work.

### Eight material markets: licensed fuel preparation advances to 20 cases

Brightpath `025c0ca` qualifies eight exact ecoinvent 3.9 cut-off material markets:
perimeter EPS slab, chromium oxide flakes, magnesium oxide, class-1 nickel,
chemical-factory equipment, molecular iodine, crushed limestone for mill and
flexible polyurethane foam. Every one of the 22 source producer entries and
29 original finished-product freight entries retains its quantity, uncertainty,
formula, bounds, pedigree and order; none is merged or renormalized. Complete
native production stocks replace source production inventories as explicitly
warned screening proxies. This does not reproduce their individual routes or
source process emissions, which are not added again. No new physical source
input omission, zero-burden supplier or recovery credit is introduced.

The factory correction is substantive: source construction represents one kg
of equipment stock, not one complete chemical plant. Native **Facilities,
chemical production, kg** matches nine independently checked source quantities
(both steels, combined concrete, electronics, solder, inorganic chemicals, rock
wool, electricity and heat). The former plant-unit candidate separately contains
12.6 million kg of facilities plus buildings and disposal. Both source market
shares now use the complete kg stock at factor +1; no plant-mass, lifetime,
capacity or annual-output multiplier is applied. Native heating technology and
internal freight still differ. Other unit-based organic-chemical-plant mappings
remain unchanged.

Other material limitations are explicit. Perimeter EPS differs in density,
conductivity and additives; no 33/30 mass multiplier or equal thermal-service
claim is made. Chromium source product information says Cr(III), while actual
producer descriptions and two matching reaction-input quantities point to the
recorded wet flakes process represented by native CrO3. This unresolved label /
caller-intent inconsistency is warned, not silently corrected or generalized
into a Cr2O3/CrO3 substitution. MgO uses the complete native process including
its existing fossil-CO2 term, without an added theoretical calcination balance.
Nickel uses a historical global Class-I mix for all eight source stock entries,
with grade, route/recycling/allocation and important-impact sensitivity warnings;
source 99.70%+ versus native nominal 99.5% does not justify an invented purity
multiplier. Native iodine and limestone retain their complete inventories with
geography, age, process and infrastructure limitations. Flexible PUR keeps the
20% MDI / 40% high-density TDI / 40% low-density TDI shares and nine freight
rows, but all use complete native TDI/polyol flexible foam: MDI chemistry,
viscoelastic performance, density grades, additives, losses, emissions and waste
fates are not reproduced. Those whole-product proxy differences remain warnings
with unquantified score effects.

The existing 3.12 nickel selected rule, native target, factor, shared review and
warnings are byte-identical. Its full-rule approval fingerprint necessarily
changes when the new 3.9 profile/helper is appended. Exactly that one previous
fingerprint is refreshed after proving the selected behavior and shared review
unchanged; every other previous approval operation is identical. Eight additions
give **597 validated scopes**, checked in the regular conda Git installation.
No other release or system-model approval is inferred. The historical deferred
material test restores its original nickel stage after checking unchanged 3.12
behavior; the current complete frozen replay and new 3.9 tests independently
verify the added profile.

Brightpath passes 175 new/adjacent mapping tests, 70 existing deferred-material
regressions and two full frozen-resource / historical-initial-stage replay
checks. Premise passes eleven strict fresh/cache and adjacent carbon/oxide
checks. Independent comparison against the licensed source checks all 51 market
rows field by field. All eight individual native-matrix LCIs have finite supply
and inventory. Full generation is byte-identical, canonical biosphere unchanged,
and wheel review/resource/manifest bytes match. Black/Ruff pass for changed
replay/tests. No runtime Python modules change. Installation and colleague/user
guides pin the pushed Brightpath revision; the colleague notebook is unchanged.

Actual licensed preparation accepts all eight current-cohort correspondences,
reducing **28 to 20** unapproved mappings. It still stops on other unreviewed
correspondences, beginning with metallurgical alumina. The existing unit-planning
diagnostic remains recorded; it is not suppressed or waived. No prospective
fuel update or export is qualified. The source remains at 11,947 activities and
its unchanged September modified timestamp. Removed 117.2 MiB of verified
obsolete generated replay/wheel files, retaining source evidence, logs, active
caches and databases. See the
[material-market preparation receipt](uvek-fuels-39-material-markets-preparation-verification.json).

Next: metallurgical alumina, copper/zinc oxides, silica-sand composition and KOH
basis, followed by the other current-cohort heat/treatment correspondences,
further preparation cohorts, shared fuel transformation/linking and all remaining
registered sectors. The full integration goal remains active.

### Oxide delivery and six services: licensed preparation advances to 11 cases

Brightpath `a69e39b` qualifies nine exact 3.9 cut-off correspondences: European
metallurgical alumina, copper/zinc oxide markets, RER high-voltage grid electricity,
condensing/modulating gas-boiler heat above 100 kW, polypropylene incineration,
brick collection/final disposal, dry mineral-wool landfill and limestone-residue
landfill. The oxide markets introduce six helpers. All 35 actual 3.9 market/import
input identities, units and quantities independently match earlier reviewed
native bindings, while uncertainty, formulas, bounds, pedigree and parameters
are read anew from 3.9. Alumina retains all three import helpers separately from
its market-level shipping. CuO and ZnO retain two producer shares and four
freight modes each, without substituting metal, adding production inventories
again or applying purity/oxidation-yield multipliers. Native production, feedstock,
grade, allocation, geography, emissions, internal freight, vintage and uncertainty
differences remain explicit screening limitations.

The six direct services use complete native inventories. High-voltage electricity
retains native signed exchanges, loss/infrastructure quantities and grid emissions
per kWh, with an explicit historical-country-mix proxy; it introduces no new IAM
or voltage algorithm. Boiler heat retains the useful-MJ service and technology
band, with geographic/operating differences and no second fuel/efficiency factor.
The four waste references convert negative source kg to positive native treatment
kg with factor -1, scaling uncertainty and formulas consistently. Source and native
polypropylene describe the same 15.9% wet-mass water/heating coordinates; source
water/dry-matter ratio is not mistaken for wet-water share. Brick retains the
one-kg inert-waste basis and documented density, with final disposal already
inside the complete chain rather than added again. Mineral-wool landfill retains
one kg dry waste, with sealing, leachate, site, geographic and vintage differences.
Limestone residue is explicitly a moisture/composition proxy: source has 0.5%
wet-mass water, native has 5%. Original wet-waste kg remains the service basis;
no dry-load multiplier, extra water, disposal or recovery credit is invented.
Score differences remain unquantified. Caller quantities and separate emissions
are preserved; source process emissions are not added over complete native models.

Previously approved 3.8 oxide selected rules/shared reviews and all six legacy
helper recipes are unchanged, as is the 3.6 polypropylene selection. Exactly four
old full-rule fingerprints change when new 3.9 profiles are appended. Only those
four are refreshed after proving identical old selected behavior, shared reviews
and complete helper closures; every other previous approval operation is identical.
Nine additions give **606 validated scopes**, checked in the regular conda Git
installation. No other release/model approval is inferred. Historical authoring
tests restore their own earlier profile tables only after checking this selected
behavior; current complete replay and new-profile conversion tests independently
cover the extension.

Brightpath passes 72 new sign/delivery/service checks, 127 related heat/incinerator/
material regressions and two complete frozen-resource / historical-initial-stage
checks. Premise passes seventeen new/adjacent strict fresh/cache checks. Independent
comparison against licensed source vectors verifies all 35 retained oxide rows
field by field. All nine native-matrix LCIs have finite supply and inventory.
Complete generation is byte-identical, canonical biosphere unchanged, and wheel
review/resource/manifest bytes match. Black/Ruff pass for changed replay/tests.
No runtime Python modules change. Requirements and user/colleague guides pin the
pushed Brightpath revision; the colleague notebook is unchanged.

Actual licensed preparation accepts all nine new current-cohort decisions,
reducing **20 to 11** unapproved correspondences. Other mappings still stop
preflight, beginning with silica sand; the existing unit-planning diagnostic
remains recorded rather than suppressed. No prospective fuel update or export
is qualified. The source still has 11,947 activities and its unchanged September
modified timestamp. Removed 117.3 MiB of verified obsolete generated replay/wheel
files, retaining primary evidence, logs, active caches and databases. See the
[oxide/service preparation receipt](uvek-fuels-39-oxide-services-preparation-verification.json).

Next: silica-sand co-product allocation, KOH/ethanol/ether quantity bases, remaining
biogas/heat markets and wastewater/aluminium/waste-heat correspondences; further
preparation cohorts, shared fuel transformation/linking and all remaining
registered sectors still follow. The full integration goal remains active.


### Heat and recycling preparation checkpoint — 11 November 2026

Seven inspected source-3.9 cut-off approvals reduce the actual licensed fuel
preparation cohort from **11 to 4**: silica sand, potassium hydroxide, ethanol
and diethyl ether remain. Six direct Brightpath matches change; its existing
allocated sewage-gas rule and every earlier Premise approval remain unchanged.
Native old-scrap aluminium replaces a primary/secondary mix, ENTSO-E gas heat
replaces Swiss-only candidates, and useful biogas heat and volume-based sewage
treatment retain complete native inventories and explicit boundary warnings.

Native waste heat is structurally empty because its documented allocation puts
100% of burdens on disposal and none on energy. The source heat inventory is
nonzero; this is explicitly an allocation/technology proxy with unquantified
score effects. Six native matrix LCIs are finite. The empty native co-product
produces solver residuals no larger than 1.24e-17, recorded separately from its
exactly empty direct inventory; no production Python change was needed. The
existing positive sewage-gas stock is independently empty at source, while its
paired sludge treatment carries burdens. Neither case licenses generic empty
suppliers or silent omissions.

All **804 tests pass** (55 new Brightpath, 733 sign regressions, 16 Premise
new/adjacent checks). Complete deterministic replay and wheel bytes match; the
canonical biosphere is unchanged. The pushed Brightpath revision is installed
from Git in the regular conda environment and all **613 scopes** validate.
No runtime Python modules or notebook cells change. Requirements and colleague
guides pin that revision. The public fuel guard remains closed; this diagnostic
preparation run does not qualify fuel transformation or export. Source data
still has 11,947 activities and its original modification timestamp. Verified
obsolete replay/wheel files were removed; source evidence and active caches
remain. See the [heat/recycling receipt](uvek-fuels-39-heat-recycling-preparation-verification.json).

Next: resolve the four observed product-basis/allocation cases, then further
preparation cohorts, the shared fuel update and export, and remaining registered
sectors. The full integration goal remains active.


### Four remaining 3.9 markets resolved — 11 November 2026

All four previously observed 3.9 approvals now pass actual licensed preparation.
Four derived market helpers retain **35 source rows**: eighteen producer entries
and seventeen freight entries, with signs, uncertainty, formulas and order,
without omissions or renormalization. No runtime Python modules change.

Silica sand retains both dried-sand producers and the chemical co-product. All
twelve nonproduction flows of that co-product equal 0.0312 times the paired
cryolite inventory. Only that allocated burden is proxied by complete native
cryolite at 0.0312 kg/kg; native hydrogen-fluoride versus source fluosilicic-acid
chemistry is explicitly warned. This is not physical cryolite consumption,
ordinary quarry sand or a recovery credit. All five market freight rows remain.

KOH, ethanol and ether use active substance kg. The old ecoinvent usage guide
section 5.1.11 documents that convention for dissolved chemicals and solvents;
concentration names alone do not imply a wet-mass factor. Original native
ethanol records explicitly state dry basis for CN/US/RER, and the CH source and
native shares/utilities independently agree. KOH's source documentation
describes rescaling its older production basis. No 1/0.856, 1/0.997 or 1/0.9995
multiplier is introduced. Complete native processes replace source technology
and allocation with warnings: eleven ethanol branches preserve country matches
where available and explicitly use RER/BR geographic proxies elsewhere; native
ether hydration also proxies the source ethanol-based route. Native precursor
and upstream transport boundaries remain part of the complete stock inventory.

All **768 checks pass**: 31 new Brightpath, 733 reference-sign regressions and
four strict fresh/cache Premise checks. Independent licensed comparison verifies
all 35 source rows field by field, and four actual weighted native-market LCIs
are finite. Complete replay and wheel bytes match; canonical biosphere is
unchanged. Four exact source-profile approvals preserve all previous operations,
with 617 validated scopes. No other release or system model is inferred.
Requirements and colleague guides pin the pushed Brightpath revision; the
notebook remains unchanged. Verified obsolete replay/wheel artifacts were removed.

Preparation now reaches a **3.8 cohort with 58 unapproved correspondences**; it
has not succeeded overall. All 58 roots and their same-product suppliers (262
activities) were inspected. Twenty-five root vectors match 3.9 exactly, which
does not establish matching supplier closure or target behavior. Comparing their
direct same-product supplier vectors found no fully identical root-plus-supplier
set (including uncertainty/formulas). No approval is inferred from root equality
alone. Triage groups contain ten energy/combustion,
eleven transport/water, seven chemical/process, eleven material/infrastructure
and nineteen agriculture/biomass cases. These are planning groups, not waived
requirements. The public fuel guard remains closed; transformation/export and
other sectors remain unfinished. The full goal stays active.

See the [four-market receipt](uvek-fuels-39-final-markets-preparation-verification.json)
and [grouped 3.8 triage](uvek-fuels-38-preparation-triage.json).


### Sixteen 3.8 native services reviewed — 11 November 2026

Actual licensed preparation accepts sixteen source-3.8 cut-off correspondences,
reducing this cohort from **58 to 42** unapproved cases. Fourteen existing
Brightpath rules remain byte-identical. Two combustion matches are corrected:
petrol machinery no longer maps to refinery-gas flaring, and agricultural diesel
uses an explicit average non-road diesel proxy rather than a particle-filter
model. Existing native grids (ID medium voltage, US/ENTSO-E low voltage), barge,
rail/pipeline freight, light fuel oil, gas-motor combustion, tap water and three
irrigation services retain their actual native functional quantities. Complete
native inventories replace source internal market mixes/production inventories
with recorded technology, geography, allocation, water and vintage limitations.
These source internal quantities are not claimed to be reconstructed.

Petrol native documentation has inherited diesel wording and a particle-filter
title; actual petrol supply is 0.02447 kg/MJ and the process describes
gasoline-powered four-stroke machinery. The complete native 2026 process is
absent from the intermediate 2025 catalog. Strict coverage correctly refused a
direct link. The existing recipe mechanism reconstructs all **four technosphere
and 21 biosphere rows**, preserving quantities, uncertainty and canonical flow
UUIDs, rather than inventing a 2025 catalog identity or relaxing validation.
No runtime Python module changes, additional source-input omission, energy
credit, calorific/engine-efficiency ratio or alternate Premise engine is added.

Three irrigation markets use complete Swiss sprinkler service per m3 supplied
to plants. Native efficiency is 75%; source surface/drip/sprinkler variants
state 45/90/75%. Technology mix, head, energy and geography differ. Both source
and native descriptions leave supplied-water and infiltration/evaporation flows
to crop datasets. Existing crop flows remain unchanged, with no invented
efficiency multiplier, water abstraction/return or balance correction.

All **836 checks pass**: 87 Brightpath targeted, 733 reference-sign regressions
and sixteen strict fresh/cache Premise tests. All sixteen native functional
LCIs are finite; source activity count and modified timestamp are unchanged.
Petrol reconstruction reproduces the installed native LCI after matching its
actual import precision: all 25 native matrix coefficients are exactly float32
versions of the original amounts. The recipe retains original nominal values.
The independently controlled LCI difference is 3.39e-12 or less, within the
original 1e-10 absolute tolerance; no tolerance is weakened. Complete generation
and wheel review/resource/manifest bytes match, canonical biosphere unchanged.
Sixteen exact new scopes preserve every earlier approval operation and yield
**633 validated scopes**. The colleague notebook is unchanged; requirements and
guides pin the pushed Git revision. Removed 234.8 MiB of obsolete old/final
replay and wheel outputs, retaining source evidence, logs and active caches.

The public fuel guard remains closed. Further mapping cases, actual shared fuel
transformation, consumer linking, validation/export and other registered sectors
remain unfinished. Next: chemical/heat/water references and construction
services, agricultural nutrient/packaging inputs (including wrong N-fertiliser
and pyrethroid matches), then remaining materials and feedstock boundaries.
The full integration goal remains active. See the
[native-service receipt](uvek-fuels-38-services-preparation-verification.json).


### Twelve nutrient and pesticide cases reviewed — 11 November 2026

Actual licensed preparation accepts twelve additional source-3.8 cut-off cases,
reducing this cohort from **42 to 30** unapproved correspondences. Nine existing
Brightpath rules remain byte-identical. Two nitrogen-fertiliser targets change
from plastic disposal and phosphoric acid to native average nitrogen supply;
pyrethroid production changes from polysulphide sealant to an explicitly broader
generic-insecticide production-burden screening proxy. No runtime Python, helper,
source-input omission, field-emission factor or alternative Premise engine is added.

Nine nutrient markets in ID/CH/US preserve their declared kg N, P2O5 and K2O
references at factor +1. Complete native historical RER nutrient supply replaces
the country-specific carrier mix and co-nutrient allocation, with geography,
manufacture, delivery and vintage warnings. No commercial-product, elemental-P/K,
crop-uptake, effectiveness, yield or avoided-nutrient multiplier is introduced.
The original native carrier sums (N 0.973; P2O5 1.223; K2O 1.0013) are explicit
consistency limitations, retained without silent normalization or a claim of
verified nutrient mass balance.

Unspecified pesticide and bipyridylium class retain complete native Swiss
production proxies. Native bipyridylium is itself estimated from generic
herbicide production. No native pyrethroid-specific supplier was found: the
selected European generic-insecticide inventory is not equivalent pyrethroid
chemistry, manufacture or toxicity. Its score effects remain unquantified and
important pyrethroid-production contributions require ingredient-specific
sensitivity/detail review. Separately listed application releases keep their
chemical identities and amounts; tests explicitly preserve Cypermethrin release.
No dose, pest-control efficacy or field-toxicity correction is inferred.

All **817 checks pass**: 72 Brightpath targeted, 733 reference-sign regressions
and twelve strict fresh/cache Premise checks. Twelve actual native functional
LCIs are finite; source count and modification timestamp remain unchanged.
Complete generation and wheel review/resource/manifest bytes match; canonical
biosphere is unchanged. Twelve exact scopes preserve every earlier operation,
yielding **645 validated scopes**. Requirements and colleague guides pin the
pushed Brightpath revision; the notebook is unchanged. Removed 117.4 MiB of
verified obsolete replay/wheel files, retaining source evidence and active caches.

Further inspection found that both agricultural packaging references are per kg
packed product, not kg plastic. Their battery-module target is wrong. Eighteen
HDPE/waste dependency roots and 94 root/supplier witnesses were inspected across
3.8 and 3.9; none of the root vectors is identical. Eight nonempty waste routes
include open burning, climate-specific dumping/unsanitary landfill and
incineration. These must not become empty recycling handovers or one common
incinerator. An explicit [pending packaging review](uvek-fuels-38-packaging-review.md)
records the required packed-product basis, plastic/forming masses, disposal
signs, freight and dependency qualification. It is not a completed mapping.

The public fuel guard remains closed. Shared fuel transformation, relinking,
validation/export, later source cohorts and other registered sectors remain
unfinished; the full goal stays active. See the
[nutrient/pesticide receipt](uvek-fuels-38-nutrients-preparation-verification.json).


### Whole fuel graph cohort diagnosis — 11 November 2026

The regular conda Git installation was used to inspect every currently selected
fuel source cohort, rather than stopping after the first 3.8 refusal. The graph
contains 117 source activities in six cohorts: 3.12 (14), 3.9 (29), 3.8 (50),
3.6 (six), 3.7 (eleven) and 3.11 (seven). The diagnostic reorders only whole
cohort groups in the full unchanged graph, retaining all datasets, exchanges,
version rows and foreign foreground targets. It calls the unchanged standard
harmonizer with normal policy and validation; source non-mutation is checked
after each call. Only the public fuel-sector guard is bypassed in the private
diagnostic; there is no transformation, export or source-database write.

The 3.12, 3.9 and 3.6 cohorts pass their migration stage before a later cohort
fails. Three cohorts expose **59 exact approval refusals across 55 distinct
correspondences**: thirty in 3.8, twenty-four in 3.7 and five in 3.11. The latter
five concern hydrogen, aluminium alloy, nickel, stainless steel and HDPE. The
3.7 cohort includes metalworking/construction/pipeline dependencies, material
supply, auxiliary services and waste routes. Shared identities can now be
reviewed together across versions with independently inspected reference bases.
No approval is inferred merely from a shared name or prior-version evidence.

These are current preflight refusals, not a guarantee that no later missing
links, role-binding or validation errors will appear. Candidate targets are
not qualified proxies; physically wrong candidates still need correction.
The scan covers the current fuel graph, not all remaining registered sectors.
It provides an explicit work queue without treating the thirty 3.8 cases as
the total remaining task. The full goal remains active. See the
[all-cohort diagnostic](uvek-fuels-all-cohort-preparation-diagnostic.json).


### HDPE dependency profiles and disposal follow-up — 11 November 2026

Independent source-3.8 and source-3.11 HDPE stock helpers retain both separate
virgin-production shares and all three original freight rows, with quantities,
uncertainty and formulas, without normalization. Complete native European HDPE
and generic rail/road/container shipping provide documented stock/geographic/
fleet proxies. Exactly four positive nonempty recycled-to-generic 3.8 supplies
(1.173141240030732e-13 kg/kg total) are omitted under standing same-kind
auto-approval, with exact signed source statistics and portable audits. The
inspected 3.11 market has no such entries and has no omission. No empty recycling
handover, virgin substitution, generic trace threshold or avoided-product credit
is introduced. Historical global 3.9 omission text is retained for that original
profile; the current-profile recipe and approval audits distinguish these cases.

The previously selected 3.9 and 3.10 rules, factors, warnings, shared reviews
and two old helper closures remain identical. Only two prior full-rule
fingerprints are refreshed, after proving those invariants, because the full
rule now includes two additional target profiles. Every other previous approval
operation is unchanged. Two new exact scopes give **647 validated scopes**.
No runtime Python module changes; requirements and colleague guides pin the
pushed Brightpath revision, and the notebook remains unchanged.

All **975 checks pass**: 239 new/adjacent Brightpath regressions, 733 reference
sign regressions and three strict fresh/cache/separate-omission-approval Premise
checks. Two weighted native market LCIs are finite; ten retained source rows
are independently compared field by field. Source count and modification time
remain unchanged. Complete replay and wheel data bytes match; canonical
biosphere is unchanged. Removed 117.5 MiB of verified obsolete generated
replay/wheel artifacts, retaining source evidence and the public calculation tools.

The combined regression run exposed mutation of the shared cached mapping in
three recent replay tests and the new HDPE replay test. Those tests now mutate
deep copies. Older authoring fixtures restore their historical HDPE stage only
after proving the new resource preserves old selected behavior and closure;
current new-profile tests and full frozen replay independently cover the
extension. Newly approved 3.11 acceptance is tested, while 3.7/3.12 remain
refusal cases for the older-profile test. No validation gate is relaxed.

Whole-graph licensed diagnosis confirms the 3.11 HDPE refusal is cleared:
current fuel preflight has **58 profile-specific refusals across 54 identities**
(3.8: thirty; 3.7: twenty-four; 3.11: four). The 3.12, 3.9 and 3.6 migration
stages still pass. This is one cleared current refusal plus a necessary 3.8
packaging dependency, not two completed packaging services. Role binding, fuel
transformation/export and other registered sectors remain unfinished.

Packaging inspection now covers 45 source-3.8 waste markets and fourteen
terminal disposal classes, with corresponding 3.9 source witnesses. Existing
selected PE open-burning/open-dump rules still point to incineration: reference
sign validation alone does not qualify that technology. Their dependent older
waste-market helpers also require scientific review. PP open disposal and
same-climate PE unsanitary landfill are not exact substitutions: full vectors,
moisture, carbon/methane, dust, uncertainty and land-use differ. They are not
silently relabelled as equivalent PE open disposal. No new packaging waste
closure or disposal proxy is qualified by this HDPE change.

Doka's [public original calculation tools](https://www.doka.ch/publications.htm)
were downloaded and inspected read-only as a route to proper burning and
climate-specific dump models. Cached examples are PVC/mixed waste, not verified
PE calculations. No macros, workbook edits or new environmental recipes were
executed. The source tool archive and workbook hashes are retained privately;
raw licensed inventories are not committed. Next: independently calculate and
verify PE open disposal from the public model, close the packaging waste chain,
and continue the broader mapping/sector work. The full goal remains active.
See the [HDPE receipt](uvek-packaging-hdpe-preparation-verification.json),
[packaging review](uvek-fuels-38-packaging-review.md) and updated
[whole-cohort queue](uvek-fuels-all-cohort-preparation-diagnostic.json).

## Mining: regional markets and native-consumer handoff qualified

The optional YAML market template now connects positive native disposal services
to the unchanged negative-waste treatment mixer. A distinct complete native
impoundment supplier preserves its 77 biosphere exchanges, while positive native
service identities forward consumers to the ordinary regional fate mixes. The
shared mixer AST, original treatment shares and ecoinvent filters are unchanged.
YAML alone could not supply a missing market or bridge the reference conventions;
the necessary helpers remain in the existing Mining class.

The ordinary six-sector REMIND SSP2-NPi 2050 update/export passes without gate or
validation bypass: 14,783 activities, 12 tailings markets, all 57 native callers
retained, 144 signed mining LCIs and 20 additional combined-market signed samples.
The numerical identity audit accounts for installed float32 matrix serialization
and independently corrects solver residuals, without changing runtime data or
validation tolerances. The default three-sector call remains unchanged.
See [mining review](uvek-mining-market-review.md) and
[combined qualification](uvek-mining-combined-qualification.json).

The next renewable probe correctly fails target coverage: its hardcoded
ecoinvent selectors match none of seven reviewed native wind generators.
This is an unfinished filter/mapping integration, not IAM inapplicability.
[The frontier receipt](uvek-renewable-filter-frontier.json) records those exact
identities. Fuels and the other twelve sectors remain unfinished. Removed
1,504,732,378 bytes of obsolete task-owned heat diagnostic snapshots, retaining
current databases, scripts and aggregate verification receipts.

## Renewable: exact native templates through the existing variant workflow

The renewable filter gap is resolved with exact YAML identities for seven native
wind generators and 21 connected turbine components. The shared copying loop
uses those aliases and limits supplier renaming to the selected components,
retaining substations and other unrelated infrastructure. Explicit background
family/biosphere arguments now reach the same existing constructor and base.
Ecoinvent keeps its original name terms and construction-relinking defaults.
No separate UVEK engine or inferred direct-drive material recipe is introduced.

The actual normal seven-sector REMIND SSP2-NPi 2050 run exports 14,902 activities,
including 119 wind variants, with graph validation and normal reports. All copied
exchange vectors match the source templates after their declared name changes;
all 238 signed variant LCIs and 119 strict source/variant LCI comparisons pass.
The default three-sector call remains unchanged. See
[renewable review](uvek-renewable-variant-review.md) and
[qualification](uvek-renewable-qualification.json).

Seven sectors are now qualified for this source/scenario. Fuels and eleven other
registered sectors remain unfinished. A fresh carbon-removal probe still stops
at the 40W pump correspondence for the 3.9 cut-off source: the existing evidence
covers 3.6 only. Review that additional source profile rather than assume that
an approval for another version establishes the same producer specification.

## Carbon-removal preparation: pump scope extension

The 40 W pump correspondence now covers the inspected/explicitly inferred
requested 3.6–3.12 cut-off profiles, including the required 3.9 case. The same
complete native pump is used per positive unit with signed caller quantities
and uncertainty retained. The requested 3.7 case explicitly uses 3.7.1 functional
reference evidence; no unpatched-vector or score equivalence is claimed.
All prior 1,244 fingerprints remain, and 1,250 scopes now validate. Brightpath's
compiled migration resource is unchanged; no runtime Python modules change.

Brightpath has 34 final pump cases passing; the earlier combined review/scoping
checkpoint passed 115. Premise's completed targeted suite passes 22 checks.
The broader legacy matrix was deliberately interrupted after 181 passes and is
not reported as complete. Source scopes, review hashes and the unchanged compiled
resource are independently verified. Actual native pump LCIs are finite for both
signs. The new Git dependency pin and review/verification are recorded in
[pump review](uvek-pump-profiles-review.md).

The actual CDR preparation reaches the next unreviewed correspondence: CH spent
solvent mixture to complete native hazardous incineration, with opposite reference
signs. CDR remains unqualified, along with the other unfinished sectors.


## Carbon-removal preparation: wet spent-solvent scope extension

The spent-solvent correspondence now includes the required 3.9 cut-off source
and requested 3.6–3.12 scopes. The complete native hazardous-incineration service
matches all 42 documented wet-composition coordinates. The existing factor −1
converts the negative waste reference to positive native treatment demand once;
wet mass, real credits, formulas and uncertainty retain their meaning. Requested
3.7 explicitly uses limited 3.7.1 functional-reference evidence, without claiming
inspection of unpatched coefficients.

All 1,250 prior fingerprints remain; six additional scopes are registered.
Brightpath's 34 tests and Premise's 17 targeted tests pass. Native signed LCIs
are finite, source metadata are unchanged, and packaged wheel evidence matches.
No runtime Python modules or compiled conversion rules change. See
[solvent review](uvek-spent-solvent-profiles-review.md) and its verification receipt.

The real encrypted REMIND SSP2-NPi 2050 CDR probe passes this correspondence and
now stops at `bp-3e3963da2137593c`, another independently scoped correspondence.
Validation remains enabled. No CDR update/export or additional sector
qualification is claimed; the seven-sector qualification remains unchanged.


## Carbon-removal preparation: correct the Jakobsberg component match

The former shared-cogeneration-components match for the source labeled heat
pump is replaced by the native heat exchanger for the same 160 kWe Jakobsberg
installation. Plant documentation, both 590 kg steel inputs and total effluent
support that interpretation; conflicting source product text and transport/
water differences remain explicit warnings. Use the complete native process
per signed unit, without capacity/lifetime/operation scaling. Only independently
inspected cut-off 3.8–3.12 scopes are registered. See
[component review](uvek-cogen-heat-exchanger-review.md).

All 1,256 prior fingerprints remain, with five additional scopes. Brightpath's
95 mapping cases and 106 resource checks pass. Premise's four new fresh/cache/
refusal cases and eight existing harmonizer/ledger cases pass. Actual signed
native LCIs are finite. Runtime modules are unchanged; only the mapping-authoring
helper and generator have small maintainer additions to replay the JSON decision.
The [receipt](uvek-cogen-heat-exchanger-verification.json) records an initially
failed Unicode-string test assertion and the successful corrected-test rerun,
as well as two confirmed pre-existing generator import-order lint findings.

The real encrypted REMIND CDR preparation passes the corrected component and
now stops at the 100 kW absorption-chiller market (`bp-7cf08aa845443b7b`). Normal
validation stays enabled. CDR remains unqualified; the seven-sector exported
qualification is unchanged and the full integration goal remains unfinished.


## Carbon-removal preparation: chiller and turbine profile reviews

Two existing 3.6 construction correspondences now cover directly inspected
cut-off 3.8–3.12. Complete native 100 kW absorption-chiller and 10 MWe turbine
inventories are retained per signed unit, without operational energy, capacity,
lifetime or additional equipment factors. Source/native travel, useful-heat
basis, geography and background differences remain warnings. Unpatched 3.7 and
3.7.1 are not newly approved. See [equipment review](uvek-cdr-equipment-profiles-review.md).

All 1,261 prior scopes remain; ten are added. Brightpath's 60 mapping tests and
Premise's 16 targeted checks pass, native signed LCIs are finite, and the compiled
migration resource and runtime Python remain unchanged. The real CDR probe now
reaches a 3.12 correspondence (`bp-0c3c05a7a1c1e60a`). CDR stays unqualified;
no additional sector export is claimed.


## Carbon-removal preparation: exact 3.12 steel and wastewater scopes

Two additional source scopes are registered without changing compiled rules.
Native rolled 18/8 steel is retained per signed kg. Complete native class-1
municipal treatment is a screening proxy per m3 with factor -1; the source COD
0.42534 versus native 0.1556 kg/m3 and regional stage/sludge differences remain
explicit warnings. All 1,271 prior scopes and operations remain unchanged.
Brightpath's 16 and Premise's seven focused cases pass using the local Brightpath
checkout; native signed LCIs are finite. Publication and installed-package
qualification are not yet claimed. See [review](uvek-cdr-312-steel-wastewater-review.md).

The small-boiler heat, European delivered-water and MDEA correspondences are
still under review. CDR remains gated, with no update/export qualification.


## Carbon-removal preparation: small biomethane useful-heat correction

The source below-100-kW condensing/modulating biomethane heat service now uses
the native 50 kW useful-heat service instead of the prior 300 kW match. Both
reference 1 MJ useful heat, so the signed factor is +1. Keep the complete native
inventory and its estimated 109.5% LHV efficiency at 30% load; warn about those
operating assumptions, temperature, emissions, fuel supply and geography.
Only the inspected cut-off 3.12 scope is verified. No extra fuel/boiler/electricity
or operating-hours multiplier is added. See [review](uvek-cdr-small-biomethane-heat-review.md).

The single compiled supplier rule and manifest change; runtime Python does not.
The existing authoring helper gains an 11-line replay wrapper and three generator
import/call lines. All 1,273 old approval fingerprints remain and one is added.
The 26 Brightpath and 16 Premise combined checks pass using local source imports;
the built wheel contains the exact reviewed resources. Actual native signed
LCIs are finite. A real diagnostic passes heat, steel and wastewater and now
stops at European tap water; the unrelated-alcohol MDEA correspondence is also
still unapproved. CDR stays gated, with no sector update/export qualification.


## Installed-package replay and coordinated publication

The installed GitHub package passes the 16 focused Premise checks with every
current approval fingerprint validated. Brightpath's 26 mapping cases pass.
The resource group passed 105 checks and exposed one stale expected target count;
updating that count for the reviewed 50 kW destination makes the remaining case
pass. The final package revision differs from the tested runtime revision only
in that test assertion/comment; installed mapping/resources match exactly.

The later committed-notebook repeat completed its normal three-sector update.
Its export encountered a confirmed SQLite busy wait alongside another active
Jupyter session. Only the owned duplicate run was stopped, and its zero-row
registration removed; no new export is claimed. The earlier complete nine-cell
run, 39 markets and finite EUR/NEU inventory checks remain the handoff evidence.
The detailed [installed receipt](uvek-installed-batch-verification-2026-11-13.json)
keeps that interruption separate from passing tests and records 175 MB of completed
temporary wheel/resource copies reclaimed. Source projects and unrelated edits
are preserved. The full goal remains unfinished, with seven qualified sectors.


## Storage policy: obsolete exports and checks without new databases

The project audit found 77 databases and 40 superseded duplicate task exports,
after protecting source/biosphere, current notebook references, calculation
setups, parameterized databases, dependency references and current qualification
milestones. The first lock-aware batch retires five older exports, removing
67,085 activity rows and 94,956,010 bytes of processed files. The registry now
contains 72 databases. All other pre-existing database names and the source/
biosphere metadata/counts are preserved. SQLite has 2,725,228,544 free bytes
available internally for reuse; no VACUUM or physical-file shrink is claimed.
The remaining 35 initial duplicate candidates are for later short batches,
subject to rechecking current references and locks. See the
[cleanup receipt](uvek-review-storage-cleanup-2026-11-13.json).

An actual reviewed ecoinvent-3.12-to-UVEK heat migration was checked using
transient foreground columns in a memory-filesystem datapackage, combined with
existing read-only native/biosphere datapackages. Both signed 2.5 MJ demands
have finite supply/inventory and exactly match the native reference. No new
Brightway database, registered activity or processed file is created; the
registry and source metadata remain unchanged. See the
[no-write LCI receipt](uvek-no-write-lci-verification-2026-11-13.json).
This proves the selected mapping/LCI check path, not a new whole-sector export.
Use non-exporting graph checks and selected in-memory LCIs between qualification
milestones, reserving new Brightway exports for meaningful integration/export
checks. The colleague notebook remains unchanged. The full goal is active,
with European water and MDEA next in the current CDR inventory.


## European delivered-water correction, checked without a new database

The exact Europe-without-Switzerland source market now uses complete native
RER delivered water rather than CH. Preserve signed kg once. Source fresh
producers sum to 1 kg and a separate self-market input is 0.12867 kg; 1.12867
is therefore not a simple producer-only loss coefficient to add to the native
model. Native treatment mix, water balance, pressure, abstraction geography,
electricity and vintage remain proxy limitations. See [review](uvek-european-delivered-water-review.md).

Three old CH-target fingerprints are retired explicitly and seven source-profile
grants are registered; all 1,271 non-water scopes/operations are unchanged.
Unpatched 3.7 coefficients remain unavailable and unverified; its retained
permission uses an explicitly limited 3.7.1 functional witness. Exact patch and
cross-model callers are refused. The compiled supplier/manifest and maintainer
replay change, with no runtime Python edits. The 90 Brightpath, 23 primary
Premise, six related scope and 12 installed-wheel checks pass (overlapping groups).

A transient-foreground LCI matches native RER water exactly for both signs,
without a new Brightway database, activity or processed file. The real CDR probe
passes water and stops at MDEA; no CDR update/export is claimed. Commits and the
GitHub dependency pin await the next publication batch; the local environment
uses the verified committed-source wheel. Seven sectors remain qualified.

## MDEA reconstruction and the next CDR graph check

The wrong alcohol match for the exact 3.12 cut-off MDEA market is replaced by
a native-derived screening reaction model plus retained finished-product delivery.
The audited methylamine/ethylene-oxide feeds, electricity and MJ heat are kept;
existing chemical-plant MJ heat is reused rather than treating kg of steam as MJ.
Native processing/emission assumptions, the missing MDEA-specific 3.10 biosphere
release, uncertain loss fates, allocation, purification and unquantified score/toxicity
differences are explicit warnings. This is not an exact MDEA plant model.
See [review](uvek-mdea-312-review.md) and [verification](uvek-mdea-312-verification.json).

One scoped grant is added; all 1,278 previous grants are unchanged. The 17 mapping,
10 resource-storage and 14 Premise mapping/cache regression checks pass. The
in-memory helper graph binds nine native suppliers and 15 exact biosphere flows;
both signed LCIs are finite and agree with independently flattened native demand
plus direct emissions to a maximum absolute difference of 1.43e-14. There are no
new Brightway databases, registered activities or processed files, and no runtime
Python edits. The frozen review and minimal maintainer replay/generator hook,
compiled mappings, Premise approval data, tests and notes are the changes.

The encrypted REMIND CDR diagnostic now passes inventory preparation and returns
from the shared CDR update. Exhaustive validation then reports one
`GRAPH.NEW_FORBIDDEN_CYCLE` component. The public CDR gate stays closed; no CDR
export or sector qualification is claimed. The next check traces its introduced
edges against both the prepared inventory and raw-source baseline. Seven of 19
sectors remain qualified. Changes await the next coordinated publication batch;
the installed local Brightpath wheel matches committed source after kernel restart.

The subsequent edge trace finds that this flagged component already exists in
the prepared graph; CDR introduces no internal edge or node into it. All 232
differing component identities are explained by native versus canonical
electricity product aliases. With the existing aliases applied to the identity
comparison, the component is contained in the known baseline. See the
[diagnostic](uvek-cdr-cycle-alias-diagnostic.json). Next: align the standalone
CDR family normalization and filters, then verify exhaustive validation and
actual technology coverage. Do not suppress the cycle rule. Three installed
local-wheel MDEA checks also pass (overlap with the primary Premise cases).

## Standalone CDR family handoff and support-role bindings

The existing shared CDR path now receives the selected family and biosphere.
This fixes the reproduced standalone electricity-alias/baseline mismatch without
changing CDR algorithms. Explicit support YAML slug/role bindings and one new
registry role group preserve the original support selection under UVEK's strict
published-role checks. All previous registry values and original filters remain
unchanged; there is no new physical proxy, omission or quantity conversion.
The maintainer generator replays these data bindings.

The real standalone update restores 12,214 records and passes exhaustive
validation: 11 regional markets, 73 regional main CDR records and 165 regional
support records (overlapping counts). There are no missing energy matches among
436 checks on 48 DAC records. The complete transformed graph passes 168 signed
in-memory LCIs for 84 regional CDR activities and markets, without new Brightway
databases, registered activities or processed files. The 41 focused/shared-energy
and 64 requirement/hybrid/activity-map regressions pass. Four pre-existing unused
CDR imports are preserved to avoid unrelated source changes; other changed Python
files pass Ruff and all changed Python files pass Black.

See [review](uvek-cdr-family-review.md) and
[verification](uvek-cdr-family-verification.json). Combined-sector and actual-export
qualification remain pending. The public gate still admits seven sectors and its
no-argument default remains biomass/electricity/heat. Changes await a later batch.

## CDR qualified through the public eight-sector workflow

The ordinary public update now accepts `cdr`, after a combined run of biomass,
electricity, heat, cement, steel, mining, renewable and CDR passed exhaustive
validation with zero errors. The actual Brightway export is
`BAFU:2026-REMIND-SSP2-NPi-2050-eight-cdr-20261113-120504`, with 15,137 activities
and all 11,947 native source codes retained. Its 148,624 technosphere exchanges
link inside the target and its biosphere exchanges link only to the selected
3.10 biosphere. All 168 signed LCI checks of 84 written regional CDR activities
and markets are finite and reverse correctly. Default scenario/change reports
were also generated by the standard export path.

The combined graph had already passed another 168 complete in-memory signed
LCI checks without a Brightway write. The 87 existing standard-sector/background
and 31 public-scope/fuel-axis tests also pass; these overlap the previous groups.
The documentation references were regenerated and checked. The default update
still selects biomass/electricity/heat. Other unqualified sectors remain gated.
Qualification covers the inspected encrypted REMIND SSP2-NPi 2050 workflow,
not every other IAM scenario or technologies absent from that scenario.

One private export preflight failed on an incompatible diagnostic-script import
before writing any database; the corrected public run above completed.
The overlapping battery probe met the writer's SQLite lock during construction,
so that result says nothing about battery mapping. It is being retried after the
export writer exits. No user's kernel was stopped. Two now-unused private graph
snapshots, totalling 1,154,649,969 bytes, were removed after their checks; receipts
and scripts remain. Exactly one new milestone Brightway database was created.

See [qualification](uvek-cdr-qualification.json). Eight of 19 sectors are qualified
for the inspected scenario; 11 remain. CDR commits await the next publication batch.

A final standalone `ndb.update(["cdr"])` check also passes with the ordinary
default checkpoint persistence, no public-gate bypass, exhaustive validation and
no Brightway write. After the export writer exited, battery preparation was
retried and now reaches an actual mapping decision: exact 3.9 cut-off
`market for hydrogen fluoride` (RER) to native `Hydrogen fluoride, at plant`
(GLO), rule `bp-0bbfeb07da304c0c`. Product/solution basis, production boundary
and market delivery need inspection before this correspondence can be approved.

## Battery: first five source-3.9 material decisions

Five exact cut-off correspondences are now independently reviewed: HF and TCE
markets, ultrapure-water production, glass fibre and electronic-grade silicon.
The two market wrappers retain all original final-delivery quantities and
producer entries separately; the other inputs use complete native products.
Reference signs/units, caller uncertainty/formulas and separate foreground
emissions remain intact. Native HF's inconsistent CAS, unverified assay, TCE's
historical cumulative boundary and missing infrastructure/land use, water
recovery and equipment differences, glass feedstock/heat boundaries and silicon
allocation/concentration conventions are explicit screening limitations.
There is no added caller-exchange omission or runtime Python change.

The 37 Brightpath mapping/storage cases and 25 Premise fresh/cache/refusal and
existing-approval regressions pass. Ten signed transient LCIs and five independent
native-reference LCIs pass; maximum absolute inventory difference is 8.21e-10.
No Brightway database, activity or processed file is created. All previous approval operations and 1,279
scope fingerprints remain unchanged; five scoped grants bring the
set to 1,284. The verified committed-source wheel is installed, and its unused
43,843,967-byte binary copy was removed. Running notebook kernels need a restart
to load the updated data. The colleague install pin is updated for the next batch.

The actual battery retry clears all five cases and adds no new unapproved rule
in that cohort: 195 becomes 190 unique unreviewed correspondence rules. Those
are not dataset counts or all remaining source profiles. The next error is an
unsafe sodium-fluoride → sodium-arsenide match, which remains refused pending
correction. Battery preparation, update, export and wider coverage are unfinished.
Eight of 19 sectors remain qualified. See [review](uvek-battery-first-materials-39-review.md)
and [verification](uvek-battery-first-materials-39-verification.json).

## Battery: two exact 3.9 reviews reused

Independent source/native inspection confirms the existing copper-scrap market
and RoW chemical-heat helpers. Two new scoped Premise grants leave all previous
operations and Brightpath resources unchanged. Copper retains its production-only
allocated cut-off stock and all freight; heat retains its seven make-up-water
quantities and both physical water releases around one native MJ heat service.
Internal source uncertainty/formulas are not reconstructed; caller signs, units,
uncertainty/formulas and separate emissions are preserved. Geographic, technology
and vintage differences remain explicit warnings. No runtime module changes.

Six new fresh/cache/refusal tests and four signed transient LCIs plus two native
reference LCIs pass without a database write. The actual battery retry clears
exactly two rules, 190 → 188 in the inspected 3.9 cohort, with none newly added.
Aluminium's generic-Chinese-rail → coal-freight match and the incorrect sodium
fluoride → sodium arsenide match remain unapproved. Battery remains unfinished;
eight of 19 sectors are qualified. See [review](uvek-battery-reused-39-review.md)
and [verification](uvek-battery-reused-39-verification.json).

## Second bounded storage cleanup

Five more superseded task exports were deleted after a fresh retention/dependency
audit: 65,912 activity rows and 94,159,069 bytes of processed cache files. The
project now has 68 databases. All other existing database names and source/biosphere
metadata are preserved; independent read-only SQL confirms the deleted rows are
absent and the native/biosphere counts remain 11,947/4,362. No VACUUM was run.
The shared SQLite file has reusable free pages but does not shrink automatically;
the initial page-count diagnostic used the previous project connection, so that
baseline was discarded and no SQLite byte-reclamation delta is claimed. Routine
mapping/LCI checks continue in memory; database writes are reserved for export
qualification and useful review milestones. See [receipt](uvek-review-storage-cleanup-2026-11-13-batch-2.json).

## Battery: sodium fluoride and exact reagent delivery

Three new exact 3.9 cut-off correspondences replace the sodium-fluoride arsenide
match and retain the NaF reaction/delivery and RoW HF delivery interfaces. Four
new helpers preserve the reaction inputs, five direct biosphere exchanges and
uncertainty, all final delivery, and the source's caustic-soda cell-technology
shares. Active NaOH uses factor +1, supported by ecoinvent's documented legacy
naming convention. Existing general alkali/electricity parents and all 1,286
prior scopes remain unchanged; three new grants bring the count to 1,289.
Native HF co-product, technology, geography, allocation, assay/CAS, native grid
and vintage limitations remain warnings, without extra credits or omissions.

Twenty-nine Brightpath mapping/storage cases and nine new Premise fresh/cache/
refusal cases pass; nine existing review/drift cases also passed. Six signed
ordinary LCIs are finite and reverse, and three independent native-demand
inventories agree after unchanged-matrix numerical refinement, max difference
4.55e-13. The initial small LU discrepancy and residual correction are logged;
no physical coefficients or comparison tolerances were relaxed. No database,
activity or processed file was written. The real battery retry clears exactly
three rules: 188 → 185 in the inspected 3.9 cohort, with none newly unapproved.
The next reported case is hydrated lime. Battery remains unfinished; eight of
19 sectors remain qualified. Changes await a later batch publication. See
[review](uvek-battery-fluoride-39-review.md) and
[verification](uvek-battery-fluoride-39-verification.json).

## Battery: two exact 3.9 whole-service interfaces

Independent inspection extends the unchanged complete native barge and average
steel-processing correspondences to source 3.9. Barge is a full tkm service;
metal working is processing per kg, with the native 0.227 kg loss steel retained
and the principal material still owned by the caller. The source processing
market's two regional producer shares are represented by one native average
service as an explicit geographic proxy; their internal mix is not reconstructed.
All previous approval operations remain unchanged. Ten fresh/cache/refusal tests
and four signed transient LCIs plus two native-reference comparisons pass with
no database write or runtime module change. The next joint battery retry will
verify these grants alongside the seven new material cases. See
[review](uvek-battery-reused-services-39-review.md) and
[verification](uvek-battery-reused-services-39-verification.json).

## Battery: seven native-material and delivery reviews

Hydrated packed lime (production and market), PCl5, metre-based data cable,
unit-based 500kW inverter and two methylchloride markets now have exact reviewed
3.9 correspondences. Six market wrappers retain source producer entries and final
freight separately, including a distinct light-commercial cable-delivery entry.
Native complete products retain packing, raw-material freight and equipment
end-of-life where already included. Explicit warnings cover chemical/function
bases, thermal boundaries, native text inconsistencies, historical cumulative
coverage, geography, allocation, internal uncertainty, vintage and unquantified
score differences. No physical omission or runtime module change is introduced.

Forty-one Brightpath mapping/storage tests and 34 Premise material/service/scope
regressions pass. Fourteen signed transient LCIs plus seven independent native
reference comparisons pass without a database write; unchanged-matrix numerical
refinement and its initial roundoff are documented. Seven new scopes append to
1,291 prior scopes without changing them, giving 1,298. The real joint battery
retry clears these seven rules and both barge/steel-processing services: 185 →
176 unreviewed correspondences in this 3.9 cohort, with none newly added. Next is
an incorrect unspecified-cable → capacitor match, which remains refused.
Battery is unfinished; eight of 19 sectors remain qualified. See
[review](uvek-battery-native-materials-39-review.md) and
[verification](uvek-battery-native-materials-39-verification.json).

## Battery: generic cable with retained regional waste services

The capacitor mismatches for generic cable production/market are replaced by
explicit native fabrication and original water, waste and delivery quantities.
The European mixed-plastic group retains country treatment mixes and collection
freight. Forty-five new helpers reuse 23 frozen reviewed dependencies, preserving
uncontrolled open-burning/dump/unsanitary-climate inventories instead of replacing
them with Swiss controlled disposal. Controlled native services and the newly
screened ordinary-incinerator proxy for CH fly-ash extraction retain explicit
technology, ash/recovery, carbon-metadata, geography, allocation and score warnings.
Only the two cable scopes carry forward the four existing trace HDPE omissions;
no new exchange omission is approved. A fourth scope independently inspects the
unchanged source-3.9 rolling-mill capital service, without output/lifetime scaling.
All 1,298 old scopes remain unchanged; the count is now 1,302.

Thirty-five Brightpath mapping/storage and 16 Premise sign/cache/scope/omission-
gate tests pass. Eight signed transient LCIs and four independent native-demand
comparisons pass with no database write. The corrected diagnostic includes direct
foreground biosphere definitions absent from native emissions; matrices and
comparison tolerances are unchanged. The real battery retry clears exactly three
pending roots: 176 → 173 in the inspected 3.9 cohort, with none newly added.
Standalone cable production is also verified but was not a pending root there.
Next is WEEE shredding; battery remains unfinished. Eight of 19 sectors remain
qualified. No runtime module changed. See
[review](uvek-generic-cable-39-review.md) and
[verification](uvek-generic-cable-39-verification.json).

## Battery: WEEE shredding and native Euro V freight

WEEE uses factor -1 from negative waste reference to the complete positive native
kg service. Its mechanical chain, facility coefficient and all 15 direct air
emissions match, while electricity, native freight, waste heat, geography and
internal uncertainty differ and remain explicit proxy warnings. The freight
refinement uses nine matching native Euro V diesel size/duty suppliers, weighted
by the existing native fleet conditional on Euro V. Caller tkm remains unchanged;
native size/journey distributions, model provenance and operating intensities are
proxies with unquantified score differences. The earlier unconditioned fleet
draft missed Roman-numeral EURO-V names, was withdrawn before publication and has
no active grant. No source exchange omission or runtime module change is added.

The final mapping/storage run passes 29 Brightpath cases, followed by eight
profile guards for both models (four overlap), and 13 Premise cache/sign/scope/
drift cases pass. Four signed transient LCIs and two independent native-demand
comparisons pass using the final Euro V helper, without a database write.
Two final scopes append to all 1,302 unchanged prior scopes, giving 1,304.
A fresh battery retry after the refinement confirms exactly both rules cleared:
173 → 171 in this 3.9 cohort, with none newly unapproved. Next is an unsafe
unmounted LCD → mercury match. Battery is unfinished; eight of 19 sectors remain
qualified. See [review](uvek-weee-freight-39-review.md) and
[verification](uvek-weee-freight-39-verification.json).

## Third bounded storage cleanup

Five more superseded task exports were deleted after a fresh retention audit,
including actual and declared metadata dependencies and current notebook
references: 69,496 activity rows and 96,704,977 processed-cache bytes removed.
All other existing database names and native/biosphere metadata are preserved;
independent read-only SQL confirms no remaining rows/links to the retired names,
11,947 native source and 4,362 biosphere rows, and the 15,137-activity eight-sector
CDR milestone retained. The project now has 63 registered databases.
No VACUUM ran. SQLite gained 2,818,109,440 reusable bytes internally while its
physical file size remains unchanged. Routine mapping checks continue in memory.
See [receipt](uvek-review-storage-cleanup-2026-11-13-batch-3.json).

### LCD module correspondence — 13 November 2026

Corrected both mercury matches to the complete native unmounted LCD module, retaining market delivery. Mapping and Premise cache/sign checks pass, as do in-memory LCIs without database writes. The fresh source-3.9 battery cohort has 170 unapproved rules (previously 171), next Swiss waste polyvinylfluoride. Eight of nineteen sectors remain qualified. See `uvek-lcd-module-39-review.md` and its verification JSON. Queued for the next batch push.

### PVF treatment and collection — 13 November 2026

Retained Swiss PVF collection freight and reviewed the complete native incinerator as a warned proxy for fly-ash-extraction treatment. Negative waste references convert to positive treatment demand; source-matrix balance, sign/uncertainty/cache checks and in-memory LCIs pass without database writes. The fresh source-3.9 battery cohort falls from 170 to 169 unapproved rules; next unsorted iron scrap. Eight of nineteen sectors remain qualified. See `uvek-pvf-treatment-39-review.md` and verification JSON. Queued for the next batch push.

### Unsorted iron scrap market — 13 November 2026

Reused the exact existing production-only cut-off stock, preserving the original market rail/lorry freight and uncertainty. Avoided introducing BAFU sorting/pressing burdens into unsorted stock supply. Profile/dependency guards, fresh/cache sign checks and in-memory LCIs pass without database writes. The source-3.9 battery cohort falls from 169 to 168 unapproved rules; next Swiss municipal-waste incineration. Sorted scrap remains a separate nonempty supply. Eight of nineteen sectors remain qualified. See `uvek-unsorted-iron-scrap-39-review.md` and verification JSON.

### Ordinary MSW incineration — 13 November 2026

Reviewed the existing complete Swiss native process for exact source-3.9 cut-off treatment, with explicit manganese-speciation, water/oxygen coverage, heat-basis, internal freight and recovery-boundary warnings. Rule and treatment sign factor remain unchanged. Fresh/cache sign/uncertainty tests and in-memory LCIs pass without database writes. Fresh battery preparation reduces the inspected source cohort from 168 to 167 unapproved rules; next methane sulfonic acid. Eight of nineteen sectors remain qualified. See `uvek-msw-incineration-39-review.md` and verification JSON. Queued for the next batch push.

### Methane sulfonic acid — 13 November 2026

Replaced three unrelated-chemical matches with retained RER/RoW acid inventories and their original market shares/delivery, using 42 unchanged upstream correspondence scopes and five existing helpers. Separately reviewed the complete native SO3 geographic proxy. All source rows and seven direct biosphere exchanges per producer remain; questionable source gas-feed basis is explicitly flagged without correction. Mapping/storage, fresh/cache sign/uncertainty checks and in-memory LCIs pass without database writes. Battery cohort falls from 167 to 166 unapproved rules, next ilmenite-mine infrastructure. Eight of nineteen sectors remain qualified. See `uvek-methane-sulfonic-acid-39-review.md` and verification JSON. Queued for the next batch push.

### Ilmenite built-site infrastructure — 13 November 2026

Reconstructed the original built-site buildings and five land-use flows, replacing the complete hard-coal-mine match. Preserved the asset market and two building producer shares; source site/building lifetime conventions and geographic/old-building proxies remain warnings. Mapping/storage, fresh/cache sign/uncertainty tests and in-memory LCIs pass without database writes. Battery cohort falls from 166 to 165 unapproved rules, next RoW unleaded petrol. Eight of nineteen sectors remain qualified. See `uvek-ilmenite-mine-39-review.md` and verification JSON.

### RoW delivered fossil petrol — 13 November 2026

Replaced the older refinery-only match with complete native fossil petrol at regional storage, explicitly replacing the internal delivery model. Caller signed kg and separate emissions remain; refinery/blend, country/fleet, electricity, loss, evaporation and other model differences are warnings. Native ethanol-containing mixes excluded by source fossil-only function. No helper or runtime module change. Mapping/storage, fresh/cache sign checks and in-memory LCIs pass without database writes. Battery cohort falls from 165 to 164 unapproved rules, next ilmenite recultivation. Eight of nineteen sectors remain qualified. See `uvek-petrol-row-39-review.md` and verification JSON. Queued for next batch push.

### Ilmenite recultivation — 13 November 2026

Replaced the bauxite restoration matches with the source-explicit native Swiss limestone proxy. Retained positive m2, nominal 7.44 MJ machinery service and both matched land transformations; source site/fuel/uncertainty assumptions remain warnings. No helper, omission or runtime-module change. Mapping/storage, fresh/cache and in-memory LCI checks pass without database writes. Battery cohort falls from 164 to 163 unapproved rules; next global natural-gas heat. Eight of nineteen sectors remain qualified. See `uvek-ilmenite-recultivation-39-review.md` and verification JSON. Separate aluminium-scrap coal-freight mismatch recorded for correction before scope reuse. Queued for next batch push.

### Regional useful natural-gas heat — 13 November 2026

Preserved global RoW/RER and European CH/non-CH wrapper coefficients and uncertainty, reusing two existing source-3.9 native ENTSO-E supply scopes. Swiss market uses the native Swiss heat mix with explicit source/native CHP/boiler-portfolio and boundary warnings; no representativeness, fuel-efficiency or electricity-credit correction. Two mapping wrappers, no runtime module change. Mapping/storage, fresh/cache and in-memory LCI checks pass without database writes. Battery cohort falls from 163 to 162 unapproved rules, next electric-arc-furnace dust. Eight of nineteen sectors remain qualified. See `uvek-natural-gas-heat-groups-39-review.md` and verification JSON.

### Electric-arc-furnace dust market — 13 November 2026

Retained both reviewed landfill shares and all eight collection-freight entries. Explicitly audited the two allocated-empty clinker waste interfaces; no invented treatment burden or recovery credit. Source sign/uncertainty and exact omission-scope checks pass, with finite signed in-memory LCIs and no database writes. Fresh battery preparation reduces the inspected source-3.9 cohort from 162 to 161 unapproved rules; next electric-arc-furnace slag. Eight of nineteen sectors remain qualified. See `uvek-eaf-dust-market-39-review.md` and verification JSON. Queued for the next batch push.

### Electric-arc-furnace slag market — 13 November 2026

Retained both previously reviewed landfill inventories/shares and all eight collection-freight entries. Audited the exact recovery handoff into empty cut-off stock, without a recovery credit or invented zero-service helper. Mapping/storage, fresh/cache sign and literal-omission checks and in-memory LCIs pass without database writes. Fresh battery preparation reduces the inspected source-3.9 cohort from 161 to 160 unapproved rules; next sorted, pressed iron scrap. Eight of nineteen sectors remain qualified. See `uvek-eaf-slag-market-39-review.md` and verification JSON. Queued for next batch push.

### Sorted iron and new aluminium scrap — 13 November 2026

Retained three sorted-iron producer shares and all market delivery, with explicit complete-native sorting/collection proxies. Preserved exact empty new-aluminium cut-off stock; corrected coal-specific rail and restored source delivery uncertainty. Mapping/storage, fresh/cache and signed in-memory LCIs pass with no database writes or runtime Python module changes. Fresh battery cohort falls from 160 to 158 unapproved source-3.9 rules; next sodium sulfate. Eight of nineteen sectors remain qualified. See `uvek-scrap-delivery-39-review.md` and verification JSON.

### Sodium sulfate production and market — 13 November 2026

Retained original producer shares, finishing utilities and delivery rather than replacing the market by the different native powder mix. Complete same-chemistry Mannheim/natural-source proxies have explicit allocation, energy, resource/water and uncertainty warnings. Five existing utility/freight scopes and the regional heat recipe remain unchanged. Mapping/storage and in-memory signed LCIs pass without database writes. Fresh battery cohort falls from 158 to 157 unapproved source-3.9 rules; two producer interfaces were tested independently. Next spent-solvent treatment; eight of nineteen sectors remain qualified. See `uvek-sodium-sulfate-39-review.md` and verification JSON. Held for the next batch push.

### Spent-solvent hazardous-waste incineration — 13 November 2026

Reviewed the existing exact waste/16.5%-water native service without changing its mapping or -1 treatment factor. All 51 source direct biosphere exchanges match native2026; two native-only waste-heat emissions and reversed energy labels in native prose are explicit warnings. No energy-recovery credit inferred. Signed/uncertainty/profile/cache checks and in-memory LCIs pass without database writes. Battery cohort falls from 157 to 156 pending source-3.9 rules; next ammonium sulfate. Eight of nineteen sectors remain qualified. See `uvek-spent-solvent-39-review.md` and verification JSON. Queued for next batch push.

### Ammonium sulfate nitrogen-reference correction — 13 November 2026

Replaced RoW ammonium-sulfate → cobalt-sulfate mismatch and corrected the European market's factor-one nitrogen-basis error. Native same-CAS delivered supply now uses documented factor +0.21 kg N/kg nominal compound. Whole supply/grade/allocation differences remain explicit proxies; no extra freight, helper, credit or runtime Python module. Mapping/storage, fresh/cache and signed in-memory LCI checks pass without database writes. Battery cohort falls from 156 to 155 unapproved source-3.9 rules; next liquid argon. Eight of nineteen sectors remain qualified. See `uvek-ammonium-sulfate-39-review.md` and verification JSON.
