# Working on premise

## Repository and validation

- Use Python 3.12 or newer, as declared in `pyproject.toml`. Install an editable
  checkout with `python -m pip install -e '.[bw25]' pytest black` for modern
  Brightway development. The `bw2` extra is the separately constrained legacy stack.
- Keep commits focused on one behavior or workflow, with its relevant tests.
  Preserve unrelated local edits and check the complete staged diff before committing.
- Follow `CONTRIBUTING.md`: Black formatting and Sphinx-style docstrings. Format
  changed Python files and run the relevant existing tests.
- Licensed integration tests live in `tests/test_process_*.py` and require external
  data and credentials. Run focused offline tests first; report unavailable
  integration checks separately from passing tests.
- When changing supported versions or inventory selection, regenerate references
  with `python docs/_tools/generate_documentation_reference.py`, then run the same
  command with `--check`. Commit generated reference changes with their sources.
- Implementation modules live in domain packages such as `premise/inventory/`,
  `premise/database/`, `premise/reporting/` and `premise/export/`. Preserve the
  existing top-level compatibility import paths when changing these modules.

## CLIC inventory development

- The default remains `inventory_source="workbooks"`. The opt-in `"clic"` provider
  prepares the selected sector dependency graph lazily before scenario forks and
  certification. Do not silently replace failed CLIC resolution with public workbooks.
- Read `docs/development/clic-inventory-pipeline.md` for architecture, recipes,
  measured development results and outstanding rollout gates. Historical results
  in that document do not substitute for validating a new change.
- `premise/data/clic/requirements.json` is the reviewed role and dependency ledger.
  Its maintainer generators are `dev/build_clic_role_registry.py`, followed by
  `dev/build_clic_hybrid_registry.py`. Review identity bindings, units, retained
  workbook hashes and physical boundaries before accepting generated changes.
- Supported background targets start at ecoinvent 3.7. Older inventory and
  datapackage sources remain valid migration inputs; keep source-version validation
  separate from target-version validation.
- Preserve archive integrity checks, live freshness checks, deterministic recipe
  replay, source/target validation, complete linking and policy diagnostics.
  Pragmatic migration policy must keep accepted losses in provenance and audits.
- Keep inventory provenance through source/scenario caches, report workbooks,
  exported manifests and recipes, and database metadata.
- Relevant offline tests are `tests/test_clic_*.py`, plus inventory profiles,
  database, transformation, battery, vehicle-energy, reporting and export tests
  for the affected behavior. Use synthetic records for regression fixtures.

## Local data and outputs

- Keep `.env`, keys, credentials, licensed inventory coefficients, private caches
  and generated analysis outputs out of Git. `.env` and `outputs/` are local;
  existing ignored `export/`, `results/` and cache directories are also private.
- Pass IAM keys through the environment or explicit local arguments. Do not
  hard-code them in scripts or silently use an unencrypted IAM fallback.
- Benchmarks may read trusted local checkpoints and source caches. Keep their
  generated reports and captures in ignored directories, and distinguish timing
  comparisons from scientific or LCIA equivalence claims.
