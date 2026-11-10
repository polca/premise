# UVEK through the standard Premise workflow

Status: implemented and verified for the three-sector REMIND workflow below.
The earlier native pilot export is not evidence of standard regional-market behavior.

Baseline: Premise `release/2.6.0`, commit
`8f13df153f61687c302a460bd0584458688f3749`.

The shared biomass, electricity and heat entry points must run their normal
transformations. Regional electricity markets must include applicable REMIND
regions such as EUR and NEU, with scenario technology shares, voltage chains and
consumer links. Compact storage and the CLIC provider remain independent of
this correction. No alternate UVEK sector algorithm is an acceptable substitute.

## Necessary Python changes

- Remove the three UVEK early returns that bypass the shared transformations.
- Forward the source family and biosphere version into the existing Biomass and
  Heat constructors, mapping lookups and biosphere lookups. YAML cannot select
  UVEK aliases while the caller explicitly requests ecoinvent aliases, nor make
  release 2026 an ecoinvent biosphere version. These are parameter-routing
  adaptations; the original sector transformation sequence is retained.

Every additional Python change must be justified here by a reproduced failure
and an explanation of why mapping data alone cannot resolve it.

## Mapping decisions

2026-11-10: the shared geography mapper rejected three native labels before
biomass could start. Extend its existing missing-geography YAML; do not add a
UVEK-specific geography lookup algorithm or alter the Brightway source.

- ERCOT maps to REMIND USA.
- LBFAC32 is the exporter's deterministic alias for “Europe without Switzerland”.
  The export repair regression in brightcon-2026-material explicitly records
  this alias. Reuse the existing label's representative REMIND EUR equivalence;
  Europe outside Switzerland is not identical to REMIND EUR.
- CPA is the legacy “Centrally Planned Asia and China” region, confirmed by the
  [ecoinvent 2 methodology report](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/overview_methodology_ecoinvent%20version%202.pdf).
  Use CHA as an explicit screening representative for its two native coal
  datasets. The legacy region spans more than one modern IAM region; no country
  production shares are available here. Keep that geographic approximation
  visible rather than claim exact boundary equivalence. Source labels,
  inventories and signed coefficients remain unchanged by this YAML mapping.

## Required verification

- Actual REMIND SSP2-NPi 2050 biomass/electricity/heat run and Brightway export.
- Inspect exported regional markets, shares, voltage chains and consumer links.
- Finite LCI checks and relevant tests; preserve the ecoinvent default route.
- Log remaining scientific limitations and all deviations from the baseline.

## Shared-path findings

- The standard biomass builder can already preserve a transient technology label.
  Use it to check the residual share by declared IAM role, rather than require
  the word “residue” in a supplier name. Keep the legacy name-based fallback for
  validation callers without that label. This keeps the share check active.
- Fuel purchases need mapping-based selection because native dry-kg wood-chip
  and pellet supplies do not use ecoinvent's “market for wood chips” names.
  Store eligible direct-consumer codes before relinking, and validate those
  consumers, including capitalized native heat labels. Indirect district-heat
  distribution services are not direct wood-fuel purchases. Preserve original
  quantities/uncertainty and remove obsolete explicit input keys on retargeting.
- The Geothermal alias names an approved native-context inventory absent from
  the UVEK source and public CLIC archive. Previously the alternate electricity
  callback loaded it. Declare its existing JSON resource in the electricity
  mapping and prepare mapping-declared inventory resources in the common
  inventory-preparation stage. Reuse the existing supplier-linking loader;
  validate its destination context, retain owned-resource provenance and include
  resource hashes in the inventory semantic fingerprint. This is inventory
  preparation, not a replacement electricity transformation or a new proxy.

During implementation, the first shared electricity run created regional markets
and its supplier-vector check refused the missing geothermal technology. No
check was disabled. The resource declaration resolved that missing supplier;
the final export and independent graph checks are recorded below.

## Final architecture and changes from the pilot

The three callbacks now run the original shared sector classes and transformation
sequences. The nine separate electricity, biomass and heat engine/planner modules
(3,718 production lines) are removed, along with their superseded native-topology
tests. Their reviewed source data, decisions and historical documentation remain.
Synthetic record fixtures used by the unrelated emissions regressions are retained
in a neutral test helper. Current regressions exercise the shared transformations,
not the retired native topology. The emissions update and its uncommitted GAINS
draft are outside this three-sector qualification and are unchanged.

The standard regional supplier selection/weighting, eligible IAM efficiency
updates, residential-PV voltage placement and regional upstream relinking replace
the pilot's country-mix weighting and frozen-generator behavior. Corresponding
YAML limitations now describe this behavior. This is not a claim of numerical
LCIA equivalence with the historical pilot.

Additional necessary Python adaptations, each prompted by an actual run:

| File | Necessary adaptation | Why mapping data alone was insufficient |
| --- | --- | --- |
| `core/transformation.py`, `inventory/inventory_normalization.py` | Apply declared reference-product aliases to activities and their linked exchanges before building the shared index. Preserve source labels, codes, amounts and uncertainty. | The standard relinker correctly rejects changing a native grid product into a differently labelled regional product as a disappeared product. Updating a filter cannot normalize both sides of a link. The YAML supplies every alias; there is no alternative market algorithm. |
| `electricity/electricity.py` | Read mapped native market names/voltages and mapped country loss coordinates; retain the existing market equations, regional weighting, voltage chain, supplier selection and relinking. | These inputs were previously hardcoded to ecoinvent labels and loss extraction. Native labels require exact selection so photovoltaic production mixes are not mistaken for old grid markets. The ecoinvent mapping is empty, retaining its original defaults. |
| `energy/biomass.py` | Use family aliases, mapped fuel-purchase selection and the existing transient market-technology labels. Clear obsolete input keys when replacing a purchase. | Hardcoded “market for wood chips” labels miss native supplies. Keeping an old qualified input key after retargeting would preserve the wrong physical supplier. No producer weighting/market-construction alternative is added. |
| `energy/heat.py` | Route family/biosphere parameters into the existing mappings and shared class; remove native dispatch. | Without parameter routing, the shared class selects ecoinvent identities and interprets 2026 as a biosphere release. The transformation sequence is unchanged. |
| `inventory/activity_maps.py` | Permit generated background helpers only where a mapping explicitly opts in and the record is marked as a generated helper. | The old native-only filter excluded the resolved cubic-metre gas supplier used by the converted CCS plants, despite the YAML selecting it. Published roots remain excluded unless scientifically role-bound. |
| `database/new_database.py`, `inventory/prepared_resources.py` | Prepare mapping-declared, reviewed native-context inventories before sector updates; link atomically, validate target context, preserve resource hashes/provenance. | Geothermal and the retained electric boiler are absent from the native source/public archive. The existing general workbook path assumes an ecoinvent source profile. Reuse the existing data-resource linker instead of adding a sector algorithm or pretending a supplier name creates an inventory. With no resources, existing ecoinvent manifest/cache semantics remain unchanged. |
| `validation/validation.py` | Check declared biomass roles/eligible direct consumers, native electricity product capitalization and mapped hydro suppliers. Identify delivered hydrogen rather than upstream methane/gas in its production label; recognize capitalized fuel names. | Name substrings falsely reported missing residual biomass/electricity/hydro and combustion CO2 from carbon-free hydrogen. Checks, numerical tolerances and failure gates remain active. Legacy direct callers without the new optional mapping metadata retain their former defaults. |

No new public `NewDatabase` arguments, background-label requirements or alternative
sector-selection flags are introduced. Compact storage and CLIC preparation remain
the independently authorized implementation. The original notebooks and unrelated
working-tree edits are not part of these commits.

## Mapping corrections and evidence

- Remove source-only CH location restrictions from heat aliases: after the shared
  class regionalizes a source template, the same name/product must match its IAM
  clone. Restricting the runtime filter to CH hid the correctly created clone.
- Add native PEM-pipeline hydrogen and SMR hydrogen supply identities to their
  matching fuel classifications. Retain the packaged 120 MJ/kg hydrogen LHV;
  no inferred concentration, density or output scaling is introduced.
- Extend gas-fuel aliases to the resolved cubic-metre gas-market helpers, with
  explicit generated-helper permission. Preserve the original natural-gas LHV.
- Express the already-approved electric-boiler inventory as data: retain its
  verified workbook hash, 1 MJ reference, exact 0.2805836139169472 kWh draw and
  native Swiss medium-voltage supplier. No plant inventory is invented.
- Prepare eight one-for-one, same-unit infrastructure/SF6 supplier aliases for
  the unchanged standard market equations. Native Swiss networks and European
  SF6 production are explicit screening proxies for standard supplier locations;
  SF6 factory-versus-delivered-market boundaries remain unquantified. All actual
  native inventories remain upstream. No additional loss or credit is added.
- Map 81 native country-grid loss ratios into the shared loss equations. High
  voltage input ratio `f` becomes self-consumption `1 - 1/f`; medium/low ratios
  become upstream distribution loss `f - 1`. Each individual voltage chain has
  the original effective input ratio. Native production-volume metadata are
  zero, so terminal generator-geography coefficients from the native global
  production portfolio supply explicit relative *weighting proxies*, not
  measured national production volumes. Their geography can represent wider
  fleets. UCTE/NORDEL terms without native grids are not assigned invented losses.
  This approximation is warned at runtime and recorded on exported markets.
- The first complete export was singular: the same forest-road/sawmill supply
  templates were listed in both the procedural forestry pass and the final
  biomass technology pass. The second pass copied an already-emptied handover,
  yielding a regional supplier buying exactly one unit of itself. Remove the
  two duplicate native procedural mappings in YAML. The shared regionalization
  algorithm and its production balance checks are unchanged. A regression guards
  against this overlap returning.

## Qualification record

The actual encrypted REMIND SSP2-NPi 2050 run uses the regular Brightway project
`bafu-2026-ecospold2-biosphere-310`, source `BAFU:2026-reimported` and
`ecoinvent-3.10-biosphere`. It calls the normal `NewDatabase`,
`update(["biomass", "electricity", "heat"])` and `write_db_to_brightway` APIs.

Observed after the mapping overlap correction:

- 13,417 exported activities; source remains 11,947 activities with unchanged
  source modification timestamp `2026-09-22T10:55:01.512934`.
- 39 base electricity market groups: high/medium/low voltage for all 12 REMIND
  regions plus World, including EUR and NEU. The standard period-average markets
  are also created.
- All 405 native import/production/grid market records have one input to the
  correct regional voltage market. Some consumers retain the standard country
  handover, which now supplies the IAM market; this is not an old generation mix.
- 2,590 links to regional electricity markets and 1,069 purchases through native
  country handovers. A Swiss deionised-water consumer links directly to NEU.
- All 33 native photovoltaic production mix templates remain nonempty and are
  not selected by the grid-market aliases.
- Every technosphere input in the exported database resolves inside that export.
  EUR/NEU medium/low chains link to their own region's upstream voltage market.
  High-voltage generator purchases sum to one; the live shared supplier-vector
  and heat/biomass checks pass without suppressed errors.
- Finite LCI supply and inventory arrays for EUR high-voltage electricity, NEU
  low-voltage electricity and EUR secondary heat.

Qualification covers
this three-sector REMIND workflow; it does not certify all IAM models, scenarios
or the other 16 standard sector updates. Existing CLIC final omissions and partial
recipe limitations remain visible; no claim of impact completeness is made.

The final geography audit also corrects legacy `MRO`, `NPCC` and `US-SPP` labels
for REMIND. Without an explicit equivalence, the shared mapper returned World
for the first two and NEU for US-SPP. The previously inspected UVEK hard-coal
fleet documentation identifies US EPA/DOE power-station/fuel data. Use USA for
these generator-fleet labels; MRO/NPCC are representative source lookups, not a
claim that the physical NERC areas exclude Canada. Native record locations stay
unchanged. This is mapping data, not a bespoke sector geography algorithm.


## Final verification, 2026-11-10

The cleaned implementation exported
`BAFU:2026-premise-remind-SSP2-NPi-2050-standard-20261110-123954`
in the project above. The complete build/export took approximately 45 seconds
with locally available inputs. Independent inspection of this final database
confirmed the counts, all 12 non-World regional voltage chains, market generation
shares, supplier links and the three finite LCI calculations reported above.
The source database was not modified.

- Broad offline regression: **3,415 passed, four skipped**. This excludes the
  licensed `test_process_*` integrations and the expensive, unchanged
  `test_clic_uvek_*`, `test_clic_deferred_*`, `test_clic_rule_approvals.py` and
  `test_clic_partial_paperboard.py` migration-review batteries. It is not a claim
  that every test in the repository was run.
- After the final constructor cleanup and geography corrections, focused shared
  sector, preparation, mapping, transformation and validation checks:
  **241 passed, two skipped**. The actual run/export was repeated as well.
- Black: all 14 changed/new Python files pass. Ruff: zero new findings;
  13 existing findings in the modified baseline files remain outside this change.
- Generated documentation reference check and whitespace checks pass.
- A full licensed ecoinvent scenario integration was not rerun. The existing
  ecoinvent route is covered by the offline regression; qualification of other
  IAM configurations and sectors remains separate.

Initial geography corrections were committed as `b639a838`. Implementation
commit `712ccac6` contains the shared-route adapters, mapping data,
retirement of the alternative engines and corresponding regression tests.
Brightpath files and unrelated working-tree edits are not part of this goal.
