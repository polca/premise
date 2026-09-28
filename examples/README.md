# Premise example notebooks

The notebooks are numbered in a suggested learning order. Each notebook is
self-contained: it declares its required project, databases, paths, and environment
variables instead of relying on state created by another notebook.

## Before you start

- Install either `premise[bw25]` for modern Brightway or `premise[bw2]` for the
  legacy Brightway stack.
- Import a licensed ecoinvent database and its matching biosphere when the notebook
  uses Brightway.
- Export the IAM decryption key as `PREMISE_KEY`; notebooks never store it.
- Review the configuration cell before running a notebook. Database names are
  examples and must match the active Brightway project exactly.

## Learning path

| # | Notebook | Main outcome | Requirements / cost |
|---:|---|---|---|
| 01 | [Quickstart with Brightway](01_quickstart_brightway.ipynb) | Build, update, and write a scenario database | Brightway, ecoinvent, IAM key; heavy |
| 02 | [Consequential scenarios](02_consequential_scenarios.ipynb) | Configure marginal mixes and consequential updates | Consequential ecoinvent, IAM key; heavy |
| 03 | [Custom inputs](03_custom_inputs.ipynb) | Use local IAM files and additional inventories | Local files, Brightway; heavy |
| 04 | [External scenario datapackages](04_external_scenario_datapackages.ipynb) | Inspect and apply a Frictionless datapackage | Network or local package, Brightway; heavy |
| 05 | [EcoSpold without Brightway](05_ecospold_without_brightway.ipynb) | Build directly from EcoSpold and export files | EcoSpold directory, IAM key; heavy |
| 06 | [Export formats](06_export_formats.ipynb) | Choose Brightway, superstructure, SimaPro, OpenLCA, or datapackage output | Built scenarios; heavy |
| 07 | [Sequential scenario arrays](07_sequential_scenario_arrays.ipynb) | Compare deterministic Premise scenarios through one modern-Brightway database | `premise[bw25]`, LCIA methods; very heavy |
| 08 | [Matrix export and LCA](08_matrix_export_and_lca.ipynb) | Export A/B matrices and calculate a static LCA | `bw_processing`, `bw2calc`; heavy |
| 09 | [Custom arrays with bw_processing](09_custom_arrays_with_bw_processing.ipynb) | Add synchronized parameter scenarios to exported matrices | Existing matrix export; moderate |
| 10 | [Incremental databases](10_incremental_databases.ipynb) | Attribute cumulative changes to ordered sector groups | Brightway, Activity Browser; heavy |
| 11 | [Reports](11_reports.ipynb) | Generate and inspect scenario and change reports | Updated `NewDatabase`; heavy |
| 12 | [Score comparison](12_score_comparison.ipynb) | Compare LCIA scores across registered databases | Existing Brightway databases and methods; moderate |

The script [validate_consequential_marginal_mixes.py](validate_consequential_marginal_mixes.py)
is a separate diagnostic for consequential marginal-mix equations and argument families.

## Execution conventions

- Run notebooks top-to-bottom in a fresh kernel.
- Keep credentials and licensed-data paths in environment variables.
- Treat output database names as durable analysis inputs; avoid relying on generated
  defaults.
- Full Premise builds are intentionally not executed in ordinary CI because they need
  licensed ecoinvent data and can be computationally expensive.
- Before publishing changes, clear machine-specific outputs and rerun the notebook in
  the appropriate Premise environment when data access permits.
