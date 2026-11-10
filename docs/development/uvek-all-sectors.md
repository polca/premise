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
