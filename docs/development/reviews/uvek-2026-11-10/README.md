# UVEK review material — 10 November 2026

The mapping-development goal remains **paused**.

- [Two-page plain-English PDF](uvek-review-summary.pdf)
- [Workflow notebook](../../../../examples/uvek_2026_review_workflow.ipynb)
- [Review snapshot and evidence](review_snapshot.json)

## Run the notebook

Open `examples/uvek_2026_review_workflow.ipynb` using your usual Python environment with Premise and Brightway installed. Restart the kernel if you ran the older notebook version with a different Brightway directory.

The notebook uses the regular Brightway directory and this existing project:

- Project: `bafu-2026-ecospold2-biosphere-310`
- Source database: `BAFU:2026-reimported`
- Biosphere: `ecoinvent-3.10-biosphere`

Run its four code cells in order:

1. Import Premise and Brightway, and select the project.
2. Prepare REMIND SSP2-NPi for 2050, entering your IAM key when prompted.
3. Apply biomass, electricity and heat updates.
4. Export the result to Brightway.

There are no temporary-directory settings, frozen-package requirements, manual validation cells, calculation checks, review receipts or internal emissions previews. The notebook uses the standard Premise build and export methods. It does not store your key.

## Review context

The PDF describes the verified mapping baseline at commit `2528051f` and the unfinished work at the pause. The three-sector export and internal emissions results described there are historical verification evidence; they are not a claim that the newly simplified notebook has been run through a fresh build in your regular project.

The paused generator/CHP draft remains uncommitted in four mapping files. It proposes 137 additional input entries across 25 inventories. Its focused tests pass, but a broader calculation failed its numerical-accuracy requirement; the cause remains undiagnosed. Two further lean-burn cases retain their original emissions because combustion methane and distribution leakage are combined.

The simplified notebook's setup was run successfully against the regular Brightway project, and its syntax and public method signatures were checked. A fresh build and export were not run while preparing this revision. The earlier read-only notebook's calculation checks remain historical evidence in the private task workspace.
