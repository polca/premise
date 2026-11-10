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
