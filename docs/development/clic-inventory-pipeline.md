# CLIC inventory pipeline — implementation and rollout gate

The CLIC provider replaces eligible public foreground inventories with native
CLIC records. Workbooks containing licensed ecoinvent-derived recipes remain
bundled and continue through premise's existing source profiles, migration,
gap filling and linking. User-supplied inventories are also preserved.

**Rollout is pending.** The target range now starts at ecoinvent **3.7**;
3.5 and 3.6 background builds are rejected before downloading inventories.
Older native inventory sources remain supported for migration into newer
backgrounds. CLIC mode does not fall back to public workbook replacements.

## Current interface

```python
from premise import NewDatabase

database = NewDatabase(
    # Existing scenario, source database, biosphere and explicit IAM key arguments
    **build_arguments,
    inventory_source="clic",
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
The legacy `inventory_source="workbooks"` default remains in place during
development; it is not an automatic compatibility fallback inside CLIC mode.

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

## UVEK destination integration

The explicit inventory target resolver separates background family, release and
biosphere. The UVEK destination under development is **UVEK 2026 technosphere
with ecoinvent 3.10 biosphere**, using cut-off only. Its release must not be
interpreted as an ecoinvent version or select the legacy biosphere fallback.
Existing ecoinvent targets and their EN15804 bridge retain their current meaning.
`ClicInventoryProvider.prepare(..., family="uvek", version="2026")` converts
and links a selected graph to this context. It obtains biosphere identifiers
from the explicit ecoinvent 3.10 catalog, using the configured catalog provider.
Existing ecoinvent preparation keeps its current flow lookup. Cache keys and
manifests distinguish the two axes, and signed quantities, formulas and
uncertainty remain attached to their original exchanges.

When documented proxies are allowed, UVEK preparation also accepts Brightpath's
reviewed forward-helper `migration.technosphere_information_loss` findings and
retains them in audits. Omissions and biosphere losses retain separate gates;
pragmatic policy does not grant blanket permission to discard exchanges.

This preparation interface does not establish `NewDatabase` sector support;
source ingestion, retained workbooks and transformations have separate
integration checks. Technology-specific `uvek_aliases` and, where needed,
UVEK fuel aliases will share IAM roles with the existing `ecoinvent_aliases`.

The public Brightcon `ecospold2-biosphere310` bundle provides the initial
destination database for these checks. Its manifest records exclusions made by
the upstream mapping workflow. Import provenance must retain that manifest and
the upstream exclusions; they are not new omissions approved by premise.

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

## Current conversion gaps

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
