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

## Comparison with Premise 2.6.0

Baseline: local `release/2.6.0`, commit `8f13df153f61687c302a460bd0584458688f3749` (release preparation). Compared with the current tracked working tree at implementation commit `972b30a9`. The release commit is an ancestor of this branch. No tracked Python file has an additional uncommitted edit at the time of this comparison.

This includes all differences accumulated on `premise-CLIC` since the release, including work before the UVEK task. It is a different scope from the unpublished-commit audit above.

| Category | Modified | Added | Deleted | Total |
| --- | ---: | ---: | ---: | ---: |
| Runtime (`premise/`) | 43 | 26 | 2 | 71 |
| Tests (`tests/`) | 34 | 81 | 0 | 115 |
| Benchmarks, documentation and maintainer tools | 1 | 6 | 0 | 7 |
| All tracked Python files | 78 | 113 | 2 | 193 |

### Existing runtime files modified

```text
M  premise/_compat.py
M  premise/audit/__init__.py
M  premise/audit/campaign.py
M  premise/audit/engine.py
M  premise/audit/graph.py
M  premise/audit/rules.py
M  premise/batteries/battery.py
M  premise/core/logger.py
M  premise/core/transformation.py
M  premise/core/utils.py
M  premise/database/incremental.py
M  premise/database/new_database.py
M  premise/electricity/electricity.py
M  premise/electricity/photovoltaic.py
M  premise/emissions/emissions.py
M  premise/energy/biomass.py
M  premise/energy/heat.py
M  premise/export/brightway2.py
M  premise/export/brightway25.py
M  premise/export/export.py
M  premise/export/export_payload.py
M  premise/export/olca_export.py
M  premise/export/simapro_export.py
M  premise/fuels/markets.py
M  premise/fuels/utils.py
M  premise/iam/external.py
M  premise/iam/external_data_validation.py
M  premise/iam/pathways.py
M  premise/inventory/__init__.py
M  premise/inventory/activity_maps.py
M  premise/inventory/clean_datasets.py
M  premise/inventory/inventory_imports.py
M  premise/inventory/profiles.py
M  premise/materials/metals.py
M  premise/materials/metals_resource_boundaries.py
M  premise/materials/metals_rules.py
M  premise/reporting/change_report.py
M  premise/reporting/report.py
M  premise/reporting/score_comparison.py
M  premise/transport/vehicle_battery.py
M  premise/transport/vehicle_energy_use.py
M  premise/validation/sector_validation.py
M  premise/validation/validation.py
```

### New runtime files

```text
A  premise/database/background.py
A  premise/electricity/uvek.py
A  premise/emissions/uvek_combustion.py
A  premise/emissions/uvek_emissions.py
A  premise/emissions/uvek_factors.py
A  premise/energy/uvek_biomass.py
A  premise/energy/uvek_heat.py
A  premise/energy/uvek_heat_basis.py
A  premise/energy/uvek_heat_consumers.py
A  premise/energy/uvek_heat_distribution.py
A  premise/energy/uvek_heat_markets.py
A  premise/energy/uvek_heat_supplements.py
A  premise/energy/uvek_heat_suppliers.py
A  premise/inventory/clic.py
A  premise/inventory/clic_adapter.py
A  premise/inventory/clic_archive.py
A  premise/inventory/clic_download.py
A  premise/inventory/clic_harmonize.py
A  premise/inventory/clic_hybrid.py
A  premise/inventory/clic_omissions.py
A  premise/inventory/clic_requirements.py
A  premise/inventory/clic_rule_approvals.py
A  premise/inventory/clic_target.py
A  premise/inventory/clic_user.py
A  premise/inventory/uvek_supplements.py
A  premise/reporting/clic.py
```

### Runtime files absent from this branch

```text
D  premise/iam/extract_mapping.py
D  premise/reporting/metal_input_comparison.py
```

### Tests

```text
M  tests/lcia_regression.py
A  tests/support/check_installed_runtime.py
M  tests/test_activity_maps.py
M  tests/test_battery_capacity_validation.py
A  tests/test_brightway_exchange_owners.py
M  tests/test_brightway_writers.py
A  tests/test_clic_adapter.py
A  tests/test_clic_archive.py
A  tests/test_clic_deferred_acrylic.py
A  tests/test_clic_deferred_concrete.py
A  tests/test_clic_deferred_delivered_gas.py
A  tests/test_clic_deferred_materials.py
A  tests/test_clic_deferred_naocl.py
A  tests/test_clic_deferred_sorting.py
A  tests/test_clic_deferred_titanium.py
A  tests/test_clic_deferred_wafer.py
A  tests/test_clic_deferred_waste_markets.py
A  tests/test_clic_deferred_xanthate.py
A  tests/test_clic_download.py
A  tests/test_clic_final_goes.py
A  tests/test_clic_harmonize.py
A  tests/test_clic_hybrid.py
A  tests/test_clic_omissions.py
A  tests/test_clic_partial_paperboard.py
A  tests/test_clic_provider.py
A  tests/test_clic_recipe_source_omissions.py
A  tests/test_clic_reporting.py
A  tests/test_clic_requirements.py
A  tests/test_clic_rule_approvals.py
A  tests/test_clic_target.py
A  tests/test_clic_user.py
A  tests/test_clic_uvek_european_glass.py
A  tests/test_clic_uvek_fae_routes.py
A  tests/test_clic_uvek_hazardous_paint.py
A  tests/test_clic_uvek_kcl_site.py
A  tests/test_clic_uvek_legacy_components.py
A  tests/test_clic_uvek_mixed_heat_market.py
A  tests/test_clic_uvek_ng_heat_market.py
A  tests/test_clic_uvek_nitrate_cleanfill.py
A  tests/test_clic_uvek_nitrate_concrete.py
A  tests/test_clic_uvek_nitrate_concrete_delivery.py
A  tests/test_clic_uvek_nitrate_foam.py
A  tests/test_clic_uvek_nitrate_glass.py
A  tests/test_clic_uvek_nitrate_goods.py
A  tests/test_clic_uvek_nitrate_hdpe.py
A  tests/test_clic_uvek_nitrate_inert39.py
A  tests/test_clic_uvek_nitrate_market.py
A  tests/test_clic_uvek_nitrate_matching_service.py
A  tests/test_clic_uvek_nitrate_operating.py
A  tests/test_clic_uvek_nitrate_paint.py
A  tests/test_clic_uvek_nitrate_pe_country.py
A  tests/test_clic_uvek_nitrate_pe_row.py
A  tests/test_clic_uvek_nitrate_ps.py
A  tests/test_clic_uvek_nitrate_sand.py
A  tests/test_clic_uvek_nitrate_scrap.py
A  tests/test_clic_uvek_nitrate_steel_recycling.py
A  tests/test_clic_uvek_nitrate_water.py
A  tests/test_clic_uvek_nitrate_wool.py
A  tests/test_clic_uvek_nitrate_wool_europe.py
A  tests/test_clic_uvek_pe_collection.py
A  tests/test_clic_uvek_polymer_routes.py
A  tests/test_clic_uvek_purge_handover.py
A  tests/test_clic_uvek_steel_row_routes.py
A  tests/test_clic_uvek_target.py
A  tests/test_clic_uvek_teg_supply.py
A  tests/test_clic_uvek_wastewater_routes.py
A  tests/test_clic_uvek_workbooks.py
M  tests/test_database_metadata.py
M  tests/test_electricity.py
M  tests/test_export.py
M  tests/test_external_data_validation.py
M  tests/test_geomap.py
M  tests/test_heat.py
M  tests/test_inventory_profiles.py
M  tests/test_lcia_regression.py
M  tests/test_metals.py
M  tests/test_new_database.py
M  tests/test_pem_iridium_provider.py
M  tests/test_physical_audit_integration.py
M  tests/test_physical_inventory_audit.py
M  tests/test_process_ei310_conseq.py
M  tests/test_process_ei310_cutoff.py
M  tests/test_process_ei311_conseq.py
M  tests/test_process_ei311_cutoff.py
M  tests/test_process_ei312_EN15804.py
M  tests/test_process_ei312_conseq.py
M  tests/test_process_ei312_cutoff.py
M  tests/test_process_ei312_cutoff_increment.py
M  tests/test_process_ei38_conseq.py
M  tests/test_process_ei38_cutoff.py
M  tests/test_process_ei39_conseq.py
M  tests/test_process_ei39_cutoff.py
M  tests/test_remote_inventory.py
M  tests/test_simapro_export.py
M  tests/test_transformation.py
M  tests/test_utils.py
A  tests/test_uvek_activity_maps.py
A  tests/test_uvek_background.py
A  tests/test_uvek_biomass.py
A  tests/test_uvek_electricity.py
A  tests/test_uvek_emissions_callback.py
A  tests/test_uvek_emissions_combustion.py
A  tests/test_uvek_emissions_factors.py
A  tests/test_uvek_heat_allocated_conversion.py
A  tests/test_uvek_heat_basis.py
A  tests/test_uvek_heat_callback.py
A  tests/test_uvek_heat_consumers.py
A  tests/test_uvek_heat_distribution.py
A  tests/test_uvek_heat_markets.py
A  tests/test_uvek_heat_supplements.py
A  tests/test_uvek_heat_suppliers.py
A  tests/test_uvek_public_database.py
A  tests/test_uvek_source_cache.py
A  tests/test_uvek_supplements.py
M  tests/test_vehicle_energy_use.py
```

### Benchmarks and maintainer tools

```text
A  benchmarks/benchmark_clic_inventories.py
A  benchmarks/profile_sps_change_report.py
A  benchmarks/replay_public_change_report.py
A  dev/build_clic_hybrid_registry.py
A  dev/build_clic_role_registry.py
M  docs/_tools/generate_documentation_reference.py
A  docs/development/brightpath_clic_backward_repro.py
```

Reproduce with:

```sh
git diff --name-status release/2.6.0 -- '*.py'
```
