# Test the UVEK 2026 notebook

This guide is for a colleague running the review on her own computer. The example
builds REMIND SSP2-NPi for 2050, updates biomass, electricity and heat, exports to
Brightway, and provides a few cells for exploring the result. Other sector updates
are outside this review. No separate Brightpath checkout is needed.

## 1. Fork and select the development branch

1. Sign in to GitHub and fork [polca/premise](https://github.com/polca/premise).
2. Open a terminal. Replace `YOUR-GITHUB-USERNAME` below with your username:

```bash
git clone https://github.com/YOUR-GITHUB-USERNAME/premise.git
cd premise
git remote add upstream https://github.com/polca/premise.git
git fetch upstream premise-CLIC
git switch --create premise-CLIC upstream/premise-CLIC
git push --set-upstream origin premise-CLIC
```

This works even if the fork initially contains only the default branch. If your
clone already has a local `premise-CLIC` branch, use `git switch premise-CLIC`
instead of creating it again. Do not use the default branch for this test.

Record `git rev-parse HEAD` with your results. The branch contains the shared
UVEK workflow (`712ccac6`) and the GitHub Brightpath dependency (`6637ee4a`),
plus the notebook and guide supplied with this handoff.

## 2. Install in a separate environment

Install Git and Conda/Miniforge first. From the cloned Premise directory:

```bash
conda create -n premise-review python=3.12 pip -y
conda activate premise-review
python -m pip install -e ".[bw25]" jupyterlab ipykernel nbformat
python -m ipykernel install --user --name premise --display-name "Python (premise)"
```

The last command registers the kernel used by the notebook. If that kernel name
already belongs to an environment you need, register a different name and select
it manually in JupyterLab. Registering a kernel does not move Brightway projects.

Premise installs Brightpath directly from GitHub at
`92e89c4acdb7d2904c97ef82b7856d93f6deacc7`. Installing only the published
Brightpath alpha14 package is insufficient. Internet access is needed for the
installation, biosphere installation and first CLIC inventory download.

If you previously installed another GitHub revision, the unchanged alpha14
version number can make pip keep it. To update that existing environment explicitly:

```bash
python -m pip install --force-reinstall --no-deps "brightpath @ git+https://github.com/romainsacchi/brightpath.git@92e89c4acdb7d2904c97ef82b7856d93f6deacc7"
```

Restart the notebook kernel after upgrading. A fresh environment uses the pinned
revision automatically through Premise's requirement.

## 3. Prepare the source data once

The repository does **not** contain a Brightway project or the UVEK inventory.
You also need a Premise IAM decryption key from Romain; do not put it in Git.

The notebook expects these names in your **regular Brightway directory**:

| Item | Name |
| --- | --- |
| Project | `bafu-2026-ecospold2-biosphere-310` |
| Source database | `BAFU:2026-reimported` |
| Biosphere database | `ecoinvent-3.10-biosphere` |

If these are already installed, skip to section 4. If your existing project uses
different names, change the project, source and biosphere names in the notebook.
Do not set `BRIGHTWAY2_DIR` for this workflow. If it was set for earlier work,
unset it and restart Jupyter before proceeding.

Obtain the prepared **BAFU 2026 v1 EcoSpold 2 bundle mapped to biosphere 3.10**
from Romain through an appropriate data-sharing channel. It must include
`datasets/`, `ecospold2_reimport.py`, `manifest.json` and its audit files.
Retain the manifest and audits with your local data. The qualified input has
11,947 activities and 417,072 retained exchanges; 2,991 biosphere exchanges were
excluded by the upstream preparation. Those exclusions are separate from
Premise's documented inventory omissions.

The generated bundle is no longer included in the public Brightcon repository.
To reproduce it instead, follow the upstream
[download/repair/export instructions](https://github.com/Depart-de-Sentier/brightcon-2026-material/blob/22effb1b4e2c34da9b8060f6e0e76b0e4ad399a7/hackathon/lca-lineage/scripts/ecospold%20importer/README.md)
using BAFU 2026 v1 from openLCA Nexus and the applicable data terms. That preparation
has its own environment; then return to `premise-review` for the steps below.

In a Python session in `premise-review`, run the following **once**, changing only
the bundle path. Skip it if the source database already exists:

```python
import sys
from pathlib import Path
import bw2data as bd
import bw2io as bi

bundle = Path("/path/to/ecospold2-biosphere310")
sys.path.insert(0, str(bundle))
from ecospold2_reimport import reimport_ecospold2

if "bafu-2026-ecospold2-biosphere-310" not in bd.projects:
    bi.install_project(
        "ecoinvent-3.10-biosphere",
        project_name="bafu-2026-ecospold2-biosphere-310",
    )
bd.projects.set_current("bafu-2026-ecospold2-biosphere-310")
importer = reimport_ecospold2(
    bundle / "datasets", "BAFU:2026-reimported", "ecoinvent-3.10-biosphere"
)
importer.statistics()
if any(True for _ in importer.unlinked):
    raise RuntimeError("Source exchanges remain unlinked; do not write the database")
if "BAFU:2026-reimported" in bd.databases:
    raise RuntimeError("Source database already exists; do not overwrite it")
importer.write_database()
```

Use the bundle's re-import helper rather than the stock full EcoSpold 2 strategy
list: it preserves the prepared signed quantities and uncertainty without
reapplying migrations. No custom database metadata labels are required.

## 4. Run the notebook

From the Premise checkout, with `premise-review` activated:

```bash
jupyter lab examples/uvek_2026_review_workflow.ipynb
```

Select **Python (premise)** and run the cells in order. Enter your IAM key at the
password prompt. The first inventory download may take longer; subsequent runs
can reuse local caches. The notebook prints the exported database name, including
a timestamp so earlier runs remain available. Keep that name with your notes.

The source database is read, not overwritten. The scenario database is written
into the same project. Proxy, omission and geographic-approximation warnings
are expected review information; keep them if reporting an unexpected result.
Do not clear caches routinely or change the three selected sectors for this test.

## 5. Explore and report

The notebook includes examples to list regional markets, view EUR generation
suppliers, follow NEU low-voltage supply, search heat activities and calculate
an LCI for 1 kWh of EUR high-voltage electricity.

Look for:

- **39 base electricity markets:** three voltages for 12 REMIND regions plus
  World, including EUR and NEU. Additional period-average markets are expected.
- High-voltage mixes with scenario generation suppliers; medium-voltage markets
  buying their region's high-voltage electricity, and low-voltage markets buying
  their region's medium-voltage electricity, alongside network requirements.
- Original country grids/mixes acting as handovers to regional markets. Their
  presence alone does not mean the scenario failed to relink consumers.
- Regional biomass and heat markets and their suppliers. Search `biomass` or
  `heat`, then inspect the exchanges on an individual activity.

The LCI cell does not calculate an impact score. LCIA requires a separately
installed method; record its exact method tuple and functional unit when comparing
scores. A successful LCI solve does not establish impact completeness. Native
loss-weighting, geographic proxies, CLIC approximations and exclusions remain
screening limitations; see the [implementation audit](uvek-standard-workflow.md).

To reopen the export later, select the same project, set `export_name` to its
saved name, and run the exploration cells after the import cell. No rebuild is
needed. The database can also be inspected in Activity Browser using the same
Brightway directory/project, from an environment compatible with that tool.

Please send Romain the Premise commit, Python version, exported database name,
scenario, source-bundle manifest, and the names/products/locations of any
surprising activities. Include the full traceback for failures and relevant
warnings, without the IAM key. Clear notebook outputs before committing your
review copy. Changes can be pushed to your fork for a pull request against
`polca/premise`, base branch `premise-CLIC`.

## Handoff verification

On 2026-11-10, all nine notebook code cells were executed in order in the local
`premise` Conda environment (Python 3.13), with Brightpath installed from the
pinned GitHub revision rather than an adjacent editable checkout. The run
exported 13,417 activities, found 39 base regional electricity markets, and
produced finite supply and inventory arrays for the EUR high-voltage LCI.
The shared workflow was also previously qualified on Python 3.12. This verifies
the supplied source data and scenario, not every possible source import or IAM
scenario. The notebook is distributed with outputs and execution counts cleared.


Later shared heat fixes add 13 regional district-delivery services and connect
reviewed native heat consumers to the IAM markets while preserving their signed
quantities and uncertainty. The current qualified three-sector export therefore
contains 13,430 activities. The number of activities can also change with CLIC
archive revisions; the structural checks above are more useful than treating
one historical total as a fixed acceptance threshold. See the current
[heat qualification](uvek-shared-heat-qualification.json).

Cement is now available through the same shared workflow. To include it in this
REMIND test, add `"cement"` to the notebook's single `ndb.update(...)` call. The
original three-sector example remains valid. The checked four-sector export has
13,695 activities, 13 clinker markets and 182 regional cement producers covering
14 native grades. See [cement qualification](uvek-shared-cement-qualification.json)
for retained quantities/uncertainty, LCIs and remaining warnings. Other sector
updates and other IAM models are still being qualified; this does not extend
support to conversions between system models.
