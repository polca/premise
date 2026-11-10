# UVEK review material — 10 November 2026

The development goal is **paused**. These materials document the last verified baseline and the unfinished work; they do not resume mapping development or qualify the full 19-sector scope.

- [Two-page PDF summary](uvek-review-summary.pdf)
- [Workflow notebook](../../../../examples/uvek_2026_review_workflow.ipynb)
- [Review snapshot and evidence hashes](review_snapshot.json)

## Run the notebook

Open `examples/uvek_2026_review_workflow.ipynb` in JupyterLab or your notebook editor and select **Premise UVEK review (Python 3.12)**. That kernel has been registered locally using `/private/tmp/premise-uvek-2026-venv/bin/python`.

Run the cells in order. The default reads the existing verified Brightway export, validates package integrity and complete links, and calculates six signed inventories. It does not rebuild a scenario, request a decryption key or write a database.

The configuration cell exposes three independent switches:

| Switch | Action |
| --- | --- |
| `RUN_BUILD` | Build REMIND SSP2-NPi 2050 through normal `NewDatabase`, apply biomass/electricity/heat, and obtain an exhaustive validation report. |
| `WRITE_BRIGHTWAY` | With a successful fresh build, write and inspect a new uniquely named database. Existing names are refused. |
| `RUN_EMISSIONS_PREVIEW` | Run the committed internal callback on a separate graph copy. This neither opens the public emissions gate nor exports a four-sector database. |

A fresh build requires the preserved encrypted IAM file, its valid decryption key, and the pinned CLIC cache/archive. The notebook checks the IAM checksum before requesting the key with `getpass`; no credentials are embedded or saved. The optional preview without a fresh build requires the preserved, hash-checked GAINS array derived from that encrypted scenario.

The local paths in the first cell can be adjusted if task artifacts move. The default frozen installation is `/private/tmp/premise-uvek-2026/emissions-gas-supply-final-wheel-install`, checked against its archived wheel. Both are deliberately retained. This isolates execution from the unqualified mappings currently in the checkout. The native source project/databases must already exist; the notebook does not import new backgrounds, select reference projects or overwrite source databases.

## Baseline and unfinished work

Premise mapping baseline: **2528051f**. The public three-sector export contains 12,622 activities. The verified internal emissions component covers 3,487 input coordinates but remains unavailable as a public sector update.

The paused, uncommitted draft changes four mapping files and proposes 137 additional inputs across 25 source inventories. Its 61 focused tests pass, but the broader signed-inventory run failed the absolute numerical-residual criterion. The cause remains undiagnosed. These draft changes are excluded from the notebook's frozen package. Two additional lean-burn cases are retained unchanged because source methane combines combustion and distribution leakage.

The source mappings have been left in place, with a private patch checkpoint under `/private/tmp/premise-uvek-2026/paused_generation_mapping_draft.patch`. No unfinished mapping changes are part of the review-material commit.

## Validation of the deliverables

The PDF was checked to contain exactly two pages and both pages were visually inspected. The notebook was executed top to bottom in default **existing-export read-only mode**: 12,622 activities, complete supplier/biosphere links and six finite signed LCI calculations. Source activity counts remained unchanged. The executed notebook and run receipt stay in the private task workspace; the distributed notebook has no saved outputs.

Fresh build/export and optional internal-preview branches are supplied for use but were **not executed during this pause**. Their public build/export method keywords were checked against the frozen installed API. Earlier scientific/public-export qualification is recorded in the repository verification reports; notebook readback is not a fresh public-build qualification.

No `.py` implementation files, licensed inventories, decryption keys or full inventory results are included in these materials. Numeric LCI checks do not demonstrate LCIA equivalence or impact completeness. Nothing was pushed.
