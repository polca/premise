# Audit of recent Python changes

User preference: keep implementation changes to the strict minimum; prefer Premise YAML mappings and Brightpath migration data. This audit does not restart the paused mapping/sector goal.

## Scope and finding

Inspected the five commits after `650126f4` through `4cbf36f3`, covering the period after the mapping-only preference was expressed. None changes a `.py` file. Two extend mapping/review data; three concern the explicitly requested PDF/notebook material. Notebook code is an explicitly requested deliverable, not a hidden runtime change.

Also scanned every commit ahead of the locally recorded tracking branches: 202 Premise commits (`origin/premise-CLIC..4cbf36f3`) and 111 Brightpath commits (`origin/main..6b6163d`). No fetch, push, reset or historical rewrite was performed. The scope includes all commits in those ranges, not an author-filtered subset.

| Earlier unpublished scope | Production Python files | Commits touching production Python | Test Python files | Maintainer/tool Python files |
| --- | ---: | ---: | ---: | ---: |
| Premise | 40 | 41 | 76 | 1 |
| Brightpath | 6 | 4 | 86 | 72 |

The earlier implementation is therefore **not mapping-only**. In particular, Brightpath review/generation scripts are Python too, even though they are not imported by the runtime library. These counts describe touched files, not newly created files.

## Corrections made in this review

1. Removed the custom Brightway metadata-label gate from `extract_uvek_source`. `NewDatabase` arguments declare family/release/system model, and `biosphere_name` selects the installed biosphere. Missing or stale custom labels no longer override those arguments. Actual supplier/biosphere links, finite quantities, empty-source checks and source preservation remain. No new switch, registration helper or fallback was introduced.
2. Moved the four IGCC fuel-to-CLIC-slug correspondences from `premise/electricity/uvek.py` to the existing `premise/iam_variables_mapping/electricity.yaml`, under the two relevant technology entries. One call to the existing mapping loader replaces the hardcoded table. The loaded dictionary is exactly equal to the former table; every other YAML entry is unchanged.

These are the only new runtime edits in this review: one unnecessary gate deleted and one mapping table relocated. Brightpath implementation files are unchanged. User-edited notebooks and the paused four-file emissions draft are left untouched.

## Why earlier runtime changes have been retained

| Group | Earlier work being retained | Why YAML aliases alone do not implement it |
| --- | --- | --- |
| Background selection and extraction | Separate UVEK 2026 from biosphere 3.10; select family-specific aliases; extract linked records | Treating publisher release 2026 as an ecoinvent biosphere version selects the wrong flow definitions. Ordinary source extraction also normalizes data differently. |
| CLIC/Brightpath integration | Separate target profiles, canonical biosphere linking, helper preparation and exact omission/approval records | Declarative mappings require runtime support to apply their units, signs, helper references and retained warnings correctly. |
| Native electricity/biomass/heat adapters | Preserve native voltage topology, dry-mass biomass boundaries and allocated heat/reference quantities | Existing ecoinvent transformations assume different provider names and structures. Using aliases alone would not preserve the reviewed native physical boundaries. |
| Cache/export corrections | Lossless native source caches and serialized exchange ownership | Missing exchange fields or wrong exported owners are data-handling bugs; a supplier-name mapping cannot repair them. |
| Emissions prototype | Caller-specific combustion variants, relative factors and audits; still publicly gated | One shared burner can serve different sectors. Relabeling that burner cannot apply different projections per use without changing unreviewed consumers. |
| Tests and maintainer tooling | Regression checks and reproducible derivation of migration resources | These do not alter production behavior, but are still code changes and are reported separately rather than hidden in mapping-only totals. |

This is a functional justification for retaining the existing work, **not proof that the larger native-sector adapters are the smallest possible design**. They remain substantial and should not be presented as minimal mapping extensions. No broad refactor or rollback was done merely to lower a file count: that would change the reviewed behavior and require new qualification.

## Working rule from here

- Put dataset identities, correspondences, factors and per-case decisions in the existing YAML/migration/review data wherever supported.
- Reuse the current mapping loaders and migration engine; avoid new per-dataset Python branches or separate review scripts.
- Limit any necessary Python edit to a missing general capability or a concrete bug, and explain why changing data alone cannot address it.
- Keep tests relevant to the changed behavior. Do not add another metadata/approval layer to a normal import workflow.
- Keep the wider goal paused. This audit does not authorize more sector implementation or silently remove previously implemented functionality.

## Verification

- 80 tests pass: native background extraction, source cache, public database dispatch, electricity behavior and family-specific activity maps.
- Formatting, Ruff and generated-reference checks pass.
- Real extraction succeeds for `BAFU:2026-reimported` in `bafu-2026-ecospold2-biosphere-310` with the five custom labels absent in memory: 11,947 activities / 417,072 exchanges. No database metadata was persisted by this check.
- The IGCC dictionary loaded from YAML exactly matches the previous Python literal; there are no other semantic electricity-mapping changes.
- No fresh scenario build/export was run as part of the audit. No unrelated user changes were reverted.

## Production-file inventory

The lists below account for every production Python file touched in the inspected unpublished ranges. The audit combined commit/file statistics, net-diff review of shared runtime paths and inspection of the native adapters; it is not a formal line-by-line minimality proof.

### premise

| Production file | Commits in the inspected range |
| --- | --- |
| `premise/core/transformation.py` | `864e26cc` |
| `premise/core/utils.py` | `b452fd5c`, `2491cb1a`, `97ce6926`, `76ba67a9`, `51b366ea`, `bc6f6c90`, `b3c04fa0`, `0d15fd55` |
| `premise/database/background.py` | `b452fd5c`, `bc6f6c90`, `033a47d3` |
| `premise/database/new_database.py` | `2491cb1a`, `1fd43988`, `bc6f6c90`, `b3c04fa0`, `0d15fd55`, `f877976b` |
| `premise/electricity/electricity.py` | `864e26cc` |
| `premise/electricity/uvek.py` | `864e26cc`, `a8e9b763`, `1fd43988`, `4773dd2e`, `97ce6926` |
| `premise/emissions/emissions.py` | `f877976b` |
| `premise/emissions/uvek_combustion.py` | `f256c569`, `91ec8f91`, `f877976b`, `0d200278` |
| `premise/emissions/uvek_emissions.py` | `f877976b`, `0d200278` |
| `premise/emissions/uvek_factors.py` | `f1b9f01d` |
| `premise/energy/biomass.py` | `9714d7e3`, `bc6f6c90` |
| `premise/energy/heat.py` | `b3c04fa0` |
| `premise/energy/uvek_biomass.py` | `b196ab5c`, `9714d7e3`, `bc6f6c90` |
| `premise/energy/uvek_heat.py` | `b3c04fa0`, `033a47d3` |
| `premise/energy/uvek_heat_basis.py` | `27ec8018` |
| `premise/energy/uvek_heat_consumers.py` | `63e7fd77`, `b3c04fa0`, `033a47d3` |
| `premise/energy/uvek_heat_distribution.py` | `7937d2b9` |
| `premise/energy/uvek_heat_markets.py` | `405dcbf1`, `033a47d3` |
| `premise/energy/uvek_heat_supplements.py` | `1494cc50`, `033a47d3` |
| `premise/energy/uvek_heat_suppliers.py` | `7f8356be`, `a35fbf06` |
| `premise/export/brightway2.py` | `3359e6fc` |
| `premise/export/brightway25.py` | `3359e6fc` |
| `premise/export/export.py` | `b452fd5c` |
| `premise/export/export_payload.py` | `3359e6fc` |
| `premise/inventory/activity_maps.py` | `72d93b42`, `c86c3bad`, `1fd43988`, `4773dd2e` |
| `premise/inventory/clic.py` | `0b9c2174` |
| `premise/inventory/clic_adapter.py` | `0b9c2174` |
| `premise/inventory/clic_download.py` | `509d647b` |
| `premise/inventory/clic_harmonize.py` | `0b9c2174`, `22aea75e`, `fce8f323`, `4af430c1`, `a450a277`, `76ba67a9` |
| `premise/inventory/clic_hybrid.py` | `38224fa8`, `1fd43988`, `a9c244d0` |
| `premise/inventory/clic_omissions.py` | `22aea75e`, `97ce6926`, `51b366ea` |
| `premise/inventory/clic_requirements.py` | `1fd43988`, `a9c244d0` |
| `premise/inventory/clic_rule_approvals.py` | `fce8f323`, `963f54cb`, `887e05f8`, `4af430c1`, `a450a277` |
| `premise/inventory/clic_target.py` | `46c5afdd` |
| `premise/inventory/clic_user.py` | `38224fa8` |
| `premise/inventory/inventory_imports.py` | `b452fd5c` |
| `premise/inventory/uvek_supplements.py` | `97ce6926` |
| `premise/reporting/change_report.py` | `b452fd5c` |
| `premise/reporting/clic.py` | `2491cb1a` |
| `premise/validation/sector_validation.py` | `b3c04fa0`, `f877976b` |

### brightpath

| Production file | Commits in the inspected range |
| --- | --- |
| `brightpath/background/execution.py` | `457ee566` |
| `brightpath/background/migration.py` | `7f3deb38`, `457ee566` |
| `brightpath/background/rule_approvals.py` | `457ee566`, `ad64c71d` |
| `brightpath/background/uvek_import.py` | `46e8e880` |
| `brightpath/core/policies.py` | `457ee566` |
| `brightpath/migrations/schema.py` | `46e8e880` |
