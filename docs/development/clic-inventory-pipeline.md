# CLIC inventory pipeline — implementation and rollout gate

The CLIC provider replaces eligible public foreground inventories with native
CLIC records. Workbooks containing licensed ecoinvent-derived recipes remain
bundled and continue through premise's existing source profiles, migration,
gap filling and linking. User-supplied inventories are also preserved.

**Premise 2.7 uses this hybrid pipeline automatically.** The target range starts at ecoinvent **3.7**;
3.5 and 3.6 background builds are rejected before downloading inventories.
Older native inventory sources remain supported for migration into newer
backgrounds. Cutoff and EN15804 CLIC builds do not fall back to public workbook
replacements. Consequential builds use the workbook pipeline because CLIC
inventories are published against cutoff backgrounds and cannot be migrated
reliably across ecoinvent system models; the fallback and its reason are recorded
in the inventory manifest.
Version 3.7 also retains compatible workbooks because reviewed CLIC routes start
at 3.8. Commercial policy rejects both unassessed compatibility fallbacks.

## Current interface

```python
from premise import NewDatabase

database = NewDatabase(
    # Existing scenario, source database, biosphere and explicit IAM key arguments
    **build_arguments,
    clic_options={
        "cache_dir": "/path/to/private/premise-clic-cache",
        "migration_policy": "pragmatic",  # Default; all warnings are recorded.
    },
)
database.update(["electricity", "fuels", "steel"])
database.write_inventory_recipe("inventory-recipe.json")
```

`update` prepares inventories lazily, before the scenario is forked or certified.
`prepare_inventories(sectors)` can prepare the same scope explicitly. Multiple
calls expand the union of required sectors without changing canonical names.
The public `inventory_source` selector has been removed from `NewDatabase` and
`PathwaysDataPackage`. When `system_model="consequential"` is requested,
premise automatically selects compatible workbooks before inventory preparation and
records the requested sectors and fallback reason in the manifest. Other CLIC
builds do not silently substitute public workbook replacements.

Pragmatic migration accepts inferred reverse routes, their aggregation/proxy
losses, overlapping packaged rules using the engine's deterministic last-rule
precedence, and documented UVEK proxies. Source and target validation, complete
link coverage and valid unit conversions remain mandatory. All accepted issues
appear in conversion audits and provenance. Use `migration_policy="strict"` to
restore the previous gate; `allow_documented_proxies` can be set explicitly in
either mode and otherwise follows the mode's default.

## Resolution and distribution

1. Discover the latest version of the daily Zenodo archive, initially record
   [23935622](https://zenodo.org/records/23935622). Verify its SHA256, payload
   hashes, record counts and archive format before accepting any inventory.
2. Resolve explicit, reviewed role-to-slug bindings in
   `premise/data/clic/requirements.json`. Restrict role roots using scenario IAM
   arrays at the requested year, excluding zero technologies. Respect reviewed
   target-specific source replacements so obsolete solar templates enter only
   when required upstream, without also becoming primary electricity templates.
   Include procedural requirements such as cement capture templates,
   steel templates, battery capacity markets and hydrogen distribution.
3. Traverse retained workbook dependencies, stopping at public CLIC replacements.
   Import retained records only from the selected dependency cone. Keep source
   companion workbooks only when a profile affects an actually retained record.
   Public records already replaced by CLIC do not trigger unused workbook guards.
4. Choose the latest published semantic version of each required CLIC root and
   upstream supplier, except where a recipe pins another public version.
   Qualified current supplier identities take precedence over stale uploader
   database/code identifiers. Ambiguous suppliers and unit changes fail.
5. Verify selected unpinned versions against the live CLIC API. Broad selections
   use catalog pages; small selections use per-dataset metadata. If a required
   version is newer than the archive, stop with `ClicSnapshotOutdated`. There is
   currently no native-inventory API delta endpoint integrated into this provider.
   A successful default build therefore cannot silently use yesterday's version.
6. Harmonize source cohorts with Brightpath, then match all exchanges against
   the actual destination inventory and biosphere. Keep migration audits.

The whole compressed archive is downloaded once, because Zenodo distributes a
single ZIP. Only the selected roots and upstream activities are imported and
harmonized. Live freshness checks transfer metadata, not inventory packages.

## Commercial CLIC policy (development)

The user-facing behavior is documented in
[`reference/clic-commercial.rst`](../reference/clic-commercial.rst), with the
complete replacement catalogue in
[`reference/clic-commercial-mappings.rst`](../reference/clic-commercial-mappings.rst).
The catalogue is generated from the runtime YAML by
`python docs/_tools/generate_documentation_reference.py`; its existing `--check`
mode checks for drift in CI. Update the YAML and regenerate the include when
changing rules, rather than editing the generated catalogue by hand.

Set `clic_options={"license_policy": "commercial"}` to screen the complete
selected CLIC foreground graph before harmonization and import. The default
`license_policy="default"` preserves existing selection and output behavior.
The commercial policy permits CC0-1.0, CC-BY-3.0, CC-BY-4.0 and CC-BY-SA-4.0.
Attribution and ShareAlike obligations are retained; commercially permitted
does not mean unrestricted redistribution. Non-commercial, missing, undisclosed
and unrecognized declarations block preparation. An explicit non-commercial
source-license note alongside an otherwise permitted declaration also requires
review, without asserting that the declaration is necessarily invalid.

Reviewed approximate replacements are selected from the user's native ecoinvent
background using `premise/data/clic/commercial_replacements.yaml`. Reviewed rules
cover cutoff targets 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11 and 3.12, plus
3.12 EN15804. The registry includes chargers/converters, conventional
PV, battery cells/packs/controls, vehicle components and selected electrolyzer
materials/electronics. Rules state identities, applicable targets, evidence,
conversion, limitations and confidence. Low-confidence proxies include LTO/NMC955
chemistry substitutions, coarse vehicle-component manufacturing, PFSA represented
by PTFE resin, and SiC wafer manufacture represented by electronic-grade silicon
wafer area. These are manufacturing-burden approximations; they do not change the
consumer's assumed operating efficiency, conductivity or cycle life.

Older backgrounds need broader proxies where a chemistry or material was not
yet represented. These include LFP manufacturing represented by the older LMO
inventory in 3.8, NMC532 by NMC111, NMC622/NMC955 by NMC811, and PTFE by its
monomer where resin manufacture is unavailable. The latter omits polymerization
and forming and is a lower-bound manufacturing proxy. Supplier-specific notes
and confidence follow the actual selected fallback into the report.

Replacement processes use the native inventory's exchanges rather than the
restricted recipe's coefficients or behavior-bearing metadata. Whole-parent
replacement removes only upstream activities no longer required by another
retained activity or root. Pack-without-electronics rules remove the native BMS
purchase (or direct boards in the older LMO design), renormalizing remaining
inputs to a kg excluding those electronics. Separate control purchases remain.
PV installations scale by rated capacity; their metadata identifies the actual
proxy technology and capacity so subsequent module-efficiency updates can run.
Native manufacturing geography remains intact, with documented location proxies.
Native replacements already belong to the destination background. Their target
and inventory fingerprints are checked before they pass to the adapter's exact
supplier/biosphere matching. They bypass background migration and a second
EN15804 scrap conversion; ordinary CLIC cohorts retain the migration pipeline.

Ten reviewed background-link rules also support permissively licensed CLIC
consumers whose supplier identity is absent in an older target. These preserve
the consumer's purchase quantity and license, document the actual native
supplier, and undergo strict target-catalog validation after migration. Unknown
links still fail. Background-link substitutions are reported separately from
whole-activity substitutions and participate in the locked replacement plan.

Methane-pyrolysis hydrogen uses an independently reconstructed foreground from
[Zhang et al. (2026), Table 1](https://doi.org/10.1016/j.ijhydene.2026.154768),
whose published version is CC-BY-4.0. Per kg hydrogen it purchases 5.7 m³ natural
gas and 10.3 kWh electricity and emits 0.04 kg fugitive methane. The source's
200-bar electricity requirement is retained for the 100-bar Premise identity,
a conservative pressure proxy. The source excludes plant construction; the
3 kg solid-carbon output receives no credit. Native target markets provide the
background, so the paper's reported impact score is not reproduced. The original
CLIC recipe with conflicting source-license notes supplies no coefficients.

Essential requirements without a reviewed available replacement still stop the
build. `ClicLicenseError.issues` contains all blocked datasets with one
root-to-supplier path each; the message displays the first ten. Unselected
inventories do not affect the result. Neither caches nor pinned versions bypass
checks. Live mode checks declarations for all used CLIC records, including
specific pinned versions; replaced original records are not used inventories.
Offline/snapshot mode assesses the verified archive. The archive itself contains
mixed-license material; this policy governs the foreground used in the build.

The manifest's `license_policy` records the policy fingerprint, selected licenses,
source notes, citations and conditions. Commercial recipes restore this policy
automatically on replay; a changed policy fingerprint requires review and a new
recipe. Policy metadata contributes to retained-inventory and scenario cache
fingerprints. Report exports include an `inventory-licenses-N.csv` sidecar and a
`CLIC Licenses` workbook sheet. Substitutions also appear in an
`inventory-substitutions-N.csv` sidecar and a `CLIC Replacements` sheet, including
the substitution kind, actual supplier, conversion, confidence and limitations in the
manifest/CSV. The inventory sheet distinguishes the original published license
from the license of the inventory actually used. Commercial recipes additionally
freeze the registry fingerprint, target and actual replacement-inventory
fingerprints; changing the source coefficients, rules or target requires a new
reviewed recipe. Default builds do not add these files, sheets or policy fields.

The scope is CLIC foreground selection. Ecoinvent, retained workbooks and
user-supplied inventories require separate permission assessments. In particular,
consequential builds are rejected under this policy because their automatic
workbook fallback has not been assessed. The switch cannot establish ownership
of rights underlying a third-party declaration. See the
[CLIC license guide](https://clic.psi.ch/licenses/) and
[ecoinvent licensing terms](https://support.ecoinvent.org/licensing).

No claim of inventory or LCIA equivalence is made for approximate replacements.
Policy-on results should be reviewed using the exported substitution report.

### Commercial-policy validation, 2026-11-17

The November 16 archive (Zenodo record 24278693) was prepared against actual
licensed source databases, with all potential sector roots selected rather than
only the subset needed by one IAM scenario:

| Target | Roots | Prepared activities | Whole-activity substitutions | Pruned published activities |
| --- | ---: | ---: | ---: | ---: |
| 3.8 cutoff | 736 | 1,279 | 274 | 303 |
| 3.9 / 3.9.1 cutoff | 627 | 1,134 | 273 | 304 |
| 3.10 / 3.10.1 cutoff | 627 | 1,134 | 273 | 304 |
| 3.11 / 3.12 cutoff | 616 | 1,123 | 272 | 305 |
| 3.12 EN15804 | 616 | 1,123 | 272 | 305 |

All these graphs passed the license gate and exact destination supplier and
biosphere linking. Brightway LCI/LCIA checks for a PV installation, an LTO cell
manufacturing proxy and methane-pyrolysis hydrogen also passed on all eight
target combinations. The counts describe this archive, not a promise that future
CLIC releases will pass without further review. The registry contains 57
activity-family rules and 10 background-supplier rules; a rule can cover many
geographic variants.

With the policy off, all 1,432 prepared 3.12 cutoff activities matched the
pre-policy revision `4579ab66` exactly, including quantities and metadata.
Brightway calculations for the same frozen 616 root demands also matched
exactly across IPCC 2013 GWP100 and 16 EF v3.1 headline indicators: 10,472 score
comparisons, maximum absolute difference zero. Six individual Brightway
calculations per variant checked the batched matrix calculation.

With the policy on, the same comparisons quantify deliberate proxy effects.
For IPCC GWP100, 263 of the 616 roots changed by more than 1%. The following
groups use activity-name families and include affected downstream consumers:

| Family | Roots | Minimum change | Median change | Maximum change |
| --- | ---: | ---: | ---: | ---: |
| Photovoltaic | 239 | +0.1% | +145.4% | +324.3% |
| Battery | 48 | −31.5% | −0.5% | +45.0% |
| Methane-pyrolysis hydrogen | 1 | +12.4% | +12.4% | +12.4% |

These are untransformed foreground comparisons using one fixed 3.12 cutoff
background, not scenario-wide changes or validation of proxy accuracy. In
particular, native PV manufacturing inventories differ substantially from the
regional CLIC models. Review results and replacement limitations before using
this option for a comparative assessment.

A compact REMIND SSP2-NPi 2050 build completed all 20 sector updates after
starting with electricity and expanding the inventory selection incrementally.
It contained 35,071 scenario activities and 1,006 imported CLIC/retained-workbook
records, with 41 whole-activity substitutions in the IAM-selected subset.
Replaying the exported recipe with the same sectors restored commercial policy
automatically and reproduced all 1,006 imported records' identities, exchanges
and scientific metadata. Generated workbook activity UUIDs and CLIC pin/role
provenance markers were excluded from that scientific comparison. The two
credentialed electricity integration tests passed with default and commercial
policies; the ordinary suite passed 3,704 tests with 25 integration skips.

An alternating, otherwise idle preparation benchmark used eight measured warm
samples per mode after warm-up, with identical archive, roots and background.
Median times were 1.664 s before the policy changes, 1.677 s with the policy off
(+0.8%, 13 ms), and 1.718 s with it on (+3.3%). Inventory fingerprints were
checked on every sample. These local preparation timings exclude source
extraction, IAM updates and live metadata requests; they are not a guarantee of
identical runtime on other systems.

## Recipes and reproducibility

Pass `inventory_recipe` as a JSON/YAML path or mapping. A partial recipe can pin
individual roots or upstream suppliers without importing unrelated datasets:

```json
{
  "schema_version": 1,
  "versions": {
    "hydrogen-production-gaseous-30-bar-from-pem-electrolysis-from-grid-electricity": "1.0.0"
  }
}
```

Exported lockfiles additionally pin every selected version by numeric ID and
content SHA256, the complete root set, archive identity and resolved-graph
fingerprint, plus SHA256 checksums for retained workbooks. A complete lockfile
cannot acquire unrelated newly published foreground suppliers. Requesting a
broader sector scope than its locked graph fails rather than silently extending
the recipe. Its `namespace_slugs` also freezes which published activities needed
qualified names, so unrelated catalog additions cannot rename locked activities.
Workbook changes require an updated, reviewed registry and any
recipe that pins the old workbook hashes.

Source/target contexts, engine/resource fingerprints, proxy policy, uncertainty
choice, generated helpers and migration diagnostics remain in the manifest.
Recipes pin native dataset versions; changing the Brightpath release or target
context can change converted results, and is recorded separately.

User inventory purchases add their own required CLIC roots, including declared
replacement sources used for amount filling. Existing purchases from the user's
background database keep that background choice. Ambiguous public suppliers need
an explicit `clic slug` exchange field. User files stay local; manifests and
lockfiles record their hashes under `user_inventories`. Replaying a lockfile
requires the same files as `additional_inventories`.

## Cache boundaries

- Archive caches are keyed by content SHA256. Freshness discovery still runs
  on each online build; `offline=True` explicitly accepts snapshot freshness.
- Harmonized cohorts are keyed by their scientific records, external foreground
  dependencies, source/target catalog digests, Brightpath/resource versions and
  migration policy. An unrelated cohort remains reusable when another changes.
- Retained workbook preparation is keyed by workbook hashes, selected records,
  the source graph, CLIC resolution/linking and conversion settings. Hits skip
  Excel extraction and legacy inventory preparation for those records. JSON
  reference pairs are restored to Brightway tuples on cache reads; fast writer
  payloads also normalize list references from existing serialized inventories.
- Scenario certification includes the actual composed source graph and inventory
  semantics. Provider provenance survives trimmed source/scenario caches.

Cache payloads and migration audits are verified before reuse. Writes are atomic
and protected by process locks. User source data and licensed workbook records
must remain in private caches.

## Reports and exporters

Scenario and validation/change workbooks contain a `CLIC Inventories` sheet with
dataset slugs, native versions, role, source/target context and snapshot identity.
Retained workbooks appear with their content hashes. Adjacent JSON manifests and
directly reusable recipes preserve the full selection; migration audit files are
copied beside exports with verified hashes. Database metadata includes manifest
and recipe for Brightway, superstructure and scenario-array exports. Matrix,
SimaPro and openLCA exports also emit inventory sidecars.

## Raw conversion gaps and target compatibility

The audit below describes raw Brightpath conversion before the reviewed native
alternatives were added. Premise 2.7 uses the replacement registry for newer-source
assets on older targets, recording `older_target_compatibility` independently of
license screening. Compatibility recipes freeze those choices; destination
validation remains strict. The raw-route audit is not a failure report for the
reviewed hybrid builds validated above.

The complete all-cohort audit is in
[`brightpath-clic-target-blockers.json`](brightpath-clic-target-blockers.json), with
the standalone reproducer in
[`brightpath_clic_backward_repro.py`](brightpath_clic_backward_repro.py).
Across eight selected source cohorts and all canonical target versions, it finds
38 distinct unresolved identities: 4 first appear at 3.12 → 3.11, 8 at
3.11 → 3.10, 2 at 3.10 → 3.9, 8 at 3.9 → 3.8 and 16 at 3.8 → 3.7.
All 38 reproduce individually with valid source links and synthetic quantities.
Consequential targets also lack UVEK and cutoff-to-consequential migration routes;
the exact 3.10.1 consequential technosphere catalog is missing. EN15804 shares
the cutoff findings at its migration bridge; native adaptation and destination
linking remain separate checks. The solar-specific trace below covers only
the UVEK cohort, rather than all selected inventories.

The actual 2026-11-04 archive contains UVEK 2025 solar inventories. Brightpath
1.0.0a14 has a reviewed UVEK-to-3.12-cutoff route. Earlier targets require inferred
reverse steps. Those steps were the first blocker under the original strict
policy; pragmatic mode now permits them and records their limitations.

The first broad 3.7 cutoff conversion with pragmatic policy reaches target
validation with 98.84% technosphere and 99.34% biosphere link coverage in its
UVEK cohort. Five distinct unresolved identities affect 45 technosphere and
6 biosphere exchanges:

- Average chlor-alkali production of sodium hydroxide at RER.
- Mercury-cell chlor-alkali production of sodium hydroxide at GLO.
- A specific recycled-aggregate lean-concrete recipe at CH.
- Steam-reforming production of gaseous hydrogen at RER.
- Nitric-acid emissions to urban air. Version 3.7 has nitric acid to soil, which
  must not be substituted for an air emission.

Stepwise execution of the same UVEK cohort pinpoints the first invalid links:

| Completed conversion | Newly unresolved exchanges |
| --- | --- |
| UVEK 2025 → 3.12, then 3.12 → 3.11 → 3.10 | None |
| 3.10 → 3.9 | 27 average-production and 12 mercury-cell sodium-hydroxide purchases |
| 3.9 → 3.8 | 5 lean-concrete purchases, 1 steam-reformed-hydrogen purchase, 6 nitric-acid air emissions |
| 3.8 → 3.7 | None additional; the same 51 remain unresolved |

The relevant backwards-applied resources are
`ecoinvent-3.9-cutoff-ecoinvent-3.10-cutoff.json` for the first pair, and
`ecoinvent-3.8-cutoff-ecoinvent-3.9-cutoff.json` plus
`ecoinvent-3.8-biosphere-ecoinvent-3.9-biosphere.json` for the second group.
The sodium-hydroxide identities remain unchanged across 3.10 → 3.9, although
they are absent from the 3.9 catalog. Existing mercury-cell location replacements
in that resource cover the chlorine product, not the sodium-hydroxide coproduct.
Hydrogen is renamed correctly in 3.10 → 3.9 and first becomes unresolved on
3.9 → 3.8. The diagnostic trace retained intermediate unresolved links solely
for inspection; production validation remains enabled.

The first four need explicit target-compatible supplier choices; the last needs
an emission-preserving biosphere strategy. Consequential targets also encounter
the engine's UVEK cut-off-only export boundary. Patch target aliases 3.7.1 and
3.9.1 now pass through the same canonical version handling as `NewDatabase`.
Missing catalogs raise an error naming the required context. These actual data
and engine gaps remain release checks; strict-policy failures are no longer
treated as evidence that an inferred conversion cannot run.

## Maintainer workflow and remaining checks

The maintainer generators `dev/build_clic_role_registry.py` and
`dev/build_clic_hybrid_registry.py` derive a reviewable identity/dependency ledger.
Run the role generator first, then the hybrid generator. Review new bindings,
renames, units, scientific profile boundaries and procedural consumers before
updating the shipped registry. The ledger contains hashes and qualified
identities, without copying licensed inventory coefficients.

Run `benchmarks/benchmark_clic_inventories.py` with an existing trusted local
source cache and archive. It reports extraction/preparation stages separately
from CLIC conversion and caching. These inputs contain different native inventory
versions, so timing comparisons are not scientific result-equivalence claims.

Release checks still include full scenario certification for both inventory
backends, sequential sector expansion, user inventories, locked replay, supported
background versions/system models, report/checkpoint restoration, and all export
formats. Exported licensed inventories and detailed local build results must not
be committed to the repository.

## Measured development results, 2026-11-04

On the local Python 3.12 environment with Brightpath 1.0.0a14:

| Stage | Seconds |
| --- | ---: |
| One complete compressed archive download, 9.19 MB | 0.75 |
| Legacy preparation, 95 workbooks / 3,298 imported activities | 11.69 |
| Hybrid preparation, cold conversions and retained-source cache | 19.33 |
| Hybrid preparation, repeated with conversion and retained-source caches | 4.48 |

The broad selection imported 1,430 CLIC activities and 509 retained workbook
activities, reading or caching 17 workbook sources instead of 95. The measured
legacy cleaning/linking stages fell from approximately 2.39 to 0.73 seconds
(about 1.66 seconds saved); nested gap-filling timing is already included in product
preparation and must not be added twice. Strict Brightpath conversion makes the
first hybrid preparation slower overall. A warm legacy inventory pickle can
load in about 0.1 seconds, so the hybrid cached figure is a comparison with
uncached workbook preparation, not a claim to beat every existing cache hit.
These preparation timings exclude source cleaning, IAM loading, transformations,
live freshness checks and downloads. They are individual development runs, not
cross-machine performance guarantees or inventory-result equivalence tests.

A REMIND SSP2-NPi 2050 scenario with 15 sectors completed on the legacy and
compact backends against a trusted local ecoinvent 3.12 cutoff source graph.
The final compact build took about 40 seconds before report generation; locked
replay took about 38 seconds. These runs include roughly 9 seconds each for
construction/IAM loading and preparation, and 21 seconds for updates/certification.
The follow-up 15-sector 3.12 build with the pragmatic policy also passed, taking
about 46 seconds with a newly keyed conversion cache.
The scenario and validation reports list dataset versions and 7 retained
workbook sources. Matrix export passed validation. An isolated Brightway export
contained 34,737 activities and 3,303 biosphere flows; a hydrogen-supply LCI
calculation was finite and inventory provenance survived database metadata.

An IMAGE SSP2-M 2035 build also passed; it skipped the unavailable two-wheeler
fleet data through the existing sector behavior.
A 15-sector EN15804 build passed against the existing native ecoinvent 3.12
EN15804 database, using the existing guarded scrap adaptation before destination
matching. Its signed quantities and uncertainties are preserved. A two-scenario
cache-reload run also passed, including the behavior-bearing CLIC metadata.

After restricting targets to 3.7 onward and adding pragmatic-policy checks, the
offline regression run passed 3,238 tests with 4 skips in 78 seconds. Historical
inventory-source versions remain tested; pre-3.7 target parameter cases are
excluded from the supported target matrix.
Existing licensed `test_process_*` integrations require credentials;
an attempted full run received ecoinvent HTTP 401 responses. The offline suite
covers recipe/cache ownership, corruption recovery, provenance restoration,
user purchases, battery boundaries and vehicle aliases. The all-target rollout
gate above remains open.

Additional integration checks passed for incremental scenarios and for expanding
sector requirements across successive `update` calls. A real user workbook
selected only its 19 public CLIC dependencies and reproduced identical exchanges
with an exported lockfile; a separate user workbook correctly selected a
retained primary-steel template outside the requested sector. Both user-file
paths preserved the purchase coefficient in locked replay. Twenty complete,
one-activity public foregrounds with matching pinned native versions reproduced
the workbook pipeline's central coefficients and supplier identities; this does
not establish uncertainty-distribution or LCIA equivalence for all inventories.
Full 15-sector recipe replay preserved all 784 native version pins, 9 namespace
choices and 34,737 activities. Supplier structure, exchange quantities, active
uncertainty parameters and production volumes matched at a relative tolerance
of 1e-12. Generated UUIDs and unused distribution fields are excluded from that
comparison; floating-point arithmetic is not claimed to be bitwise identical.
The replay audit exposed two existing production-volume problems: electricity
markets used only the first requested technology, and a shared regionalized
template used the first IAM role that encountered it. The pipeline now sums all
selected electricity technologies and all IAM roles sharing a physical template.
The complete SimaPro export on the corrected development graph also completed;
its coverage report identified 2,093 excluded waste inventory indicators
(`Hazardous waste disposed` and `Non-hazardous waste disposed`) and 3,193
classification fallbacks. Review that coverage when using SimaPro results.
The complete openLCA export used the local `ecoinvent 3.12 LCIA Methods
2025-12-01.zip`. It contains 34,737 processes and 1,061,448 exchanges, including
333,089 linked technosphere exchanges. Archive CRC and provider closure passed.
Every flow, flow-property and compatible-unit reference resolves against the
inventory archive plus the original method package, matching the documented
two-step openLCA import. Of 693,622 biosphere exchanges, 626,781 reference method
package definitions; 66,841 exchanges use 257 elementary-flow definitions absent
from that package and preserved in the inventory ZIP. The export took about
153 seconds. This verifies serialization and reference integrity; an openLCA
application import, numerical engine comparison and Monte Carlo equivalence are
separate checks and are not claimed here.
