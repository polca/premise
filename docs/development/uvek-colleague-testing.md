# Test the UVEK 2026 notebook

This guide is for a colleague running the review on her own computer. The example
builds REMIND SSP2-NPi for 2050, updates biomass, electricity and heat, exports to
Brightway, and provides a few cells for exploring the result. The optional
verified sector list now includes trucks (15 sectors in total; see the latest
verification below). No separate Brightpath checkout is needed.

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

Record `git rev-parse HEAD` with your results. Use the `premise-CLIC` branch,
not the default branch. Focused development commits are now published in batches
of roughly 5-10, so a local progress report can be ahead of GitHub. The published branch retains the runnable three-sector example. The wider
sector status is tracked separately; the notebook still defaults to biomass,
electricity and heat.

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
`02326bb7b334a36b6bab1d3cbeef2d0a4f157e19`. Installing only the published
Brightpath alpha14 package is insufficient. Internet access is needed for the
installation, biosphere installation and first CLIC inventory download.

If you previously installed another GitHub revision, the unchanged alpha14
version number can make pip keep it. To update that existing environment explicitly:

```bash
python -m pip install --force-reinstall --no-deps "brightpath @ git+https://github.com/romainsacchi/brightpath.git@02326bb7b334a36b6bab1d3cbeef2d0a4f157e19"
```

Restart the notebook kernel after upgrading. A fresh environment uses the pinned
revision automatically through Premise's requirement. This revision includes
the source-3.7 car corrections and historical-stock validation support required
by the current packaged approval ledger, even when only three sectors are run.
It also packages the mapping-only source-3.8 battery proxies and their exact
approval fingerprints. The required source-3.8 battery cohort now converts.
Battery inventory preparation now completes with 951 records. A tested battery
compatibility patch is supplied for review and remains unapplied; the
previously verified fifteen-sector workflow remains the operational baseline.

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

The qualified REMIND workflow also includes steel, mining and renewable. To
exercise all seven currently qualified sectors, use:

```python
ndb.update(["biomass", "electricity", "heat", "cement", "steel", "mining", "renewable"])
```

Then export using the notebook's existing `write_db_to_brightway` call. The
three-sector example remains the default. Steel preserves distinct native grades
and processing inventories; mining creates the configured regional tailings
markets and connects native disposal-service consumers without changing their
quantities. See [steel review](uvek-steel-other-producer-audit.md) and
[mining review](uvek-mining-market-review.md) for screening limits and verification.
The renewable step creates preparatory wind variants while preserving complete
source inventories; it does not itself calculate a direct-drive bill of materials.
See [renewable review](uvek-renewable-variant-review.md) and
[qualification](uvek-renewable-qualification.json). Fuels, metals and the other
remaining sectors are still being qualified.


## Fresh notebook check: 13 November 2026

All nine example code cells ran in order in the registered `premise` Jupyter
kernel, using installed GitHub Brightpath revision
`4ec9c53f05d5ed7ee2d8a55861d236fb58f3c907`. Only the
password prompt was automated in a private execution copy; the published
notebook keeps the interactive prompt and remains unchanged, with no outputs
or execution counts. No sector gate or validation was bypassed.

The run took about 96 seconds on the local review machine and exported
`BAFU:2026-remind-SSP2-NPi-2050-review-20261113-081929`, containing 13,626
activities. The source remains present with 11,947 activities. Independent
checks found the expected 39 base electricity markets and finite inventories
for 1 kWh of EUR high-voltage and NEU low-voltage electricity. Counts can change
with CLIC archive revisions; these figures are evidence for this run, not fixed
acceptance thresholds or a runtime estimate for another computer.

See the [two-page progress summary](uvek-progress-summary-2026-11-13.pdf) and
[notebook verification](uvek-colleague-notebook-verification-2026-11-13.json).
The seven-sector export remains separately qualified; this notebook rerun does
not qualify carbon removal or any other unfinished sector.


## Installed-package check after the latest mapping batch

The final dependency pin includes the reviewed small-boiler heat, steel and
wastewater cases plus the corrected resource-count test. All 16 installed-
package Premise checks pass; the final Brightpath follow-up changes only a test,
with identical package runtime and mapping resources. See the
[installed batch verification](uvek-installed-batch-verification-2026-11-13.json).

The later notebook repeat completed the ordinary three-sector update, but its
export waited on a SQLite lock with another active Jupyter session. Only the
owned duplicate run was stopped. Its empty export registration was removed
after confirming zero activity/exchange rows; other database metadata were
preserved. That repeat is not reported as a successful new export. The earlier
nine-cell run above remains the complete notebook/export evidence.

For a reliable export, finish other database writes and release open transactions
in the same Brightway project first. The example code and user workflow are
unchanged. Local notebook edits and unrelated worktree changes were not staged.

## Optional combined CDR check

The regular notebook's three-sector update remains the default. To exercise the
eight sectors qualified for the inspected REMIND SSP2-NPi 2050 scenario, restart
the kernel and run the notebook in order with this replacement update cell,
before its export cell:

```python
ndb.update([
    "biomass", "electricity", "heat", "cement", "steel", "mining", "renewable", "cdr"
])
```

This uses the same source project, BAFU database, biosphere and export workflow.
CDR adds regional removal markets and the corresponding capture/storage activities.
The qualification and remaining modelling limits are recorded in
[`uvek-cdr-qualification.json`](uvek-cdr-qualification.json).


## Mapping-only battery prerequisites — 15 November 2026

The latest pinned Brightpath commit adds reviewed chemical supply/delivery and tanker-service mappings for a future wet-PEseparator reconstruction. Installation and the example notebook workflow are unchanged. These checks do not qualify the battery update: preparation still reports35pending rules in its inspected first3.9cohort. Continue using the documented three-sector notebook or optional eight-sector qualified run above. See `uvek-separator-chemicals-39-verification.json` for scoped tests and limitations.


## Wet-process PE separator prerequisite — 15 November 2026

The pinned Brightpath version reconstructs the wet-process PE separator using reviewed BAFU inputs,retained manufacturing emissions and delivery. Existing HDPE trace omissions remain visible in its audit. The notebook and installation workflow are unchanged;the battery sector remains unqualified with34pending first-cohort rules. See `uvek-separator-wet-pe-39-verification.json` for evidence and limits.


## Further battery chemical prerequisites — 15 November 2026

The next pinned Brightpath version adds six reviewed chemical supplies,retaining the TiO2chloride/sulfate route split anddocumenting rare-earth ore-route/zirconia nano-grade proxy limits. No notebook workflow change is required. Battery remains unqualified with28pending first-cohort rules;continue using the documented qualified sector set. Details: `uvek-battery-chemicals-39-verification.json`.

The current pin also includes five reviewed source-3.8 truck delivery markets,
including a separate 39 MJ/m³ gas basis. This does not add trucks to the
qualified notebook workflow; its first inspected source cohort still has four
unresolved recycling/waste mappings. See
`uvek-trucks-five-markets-38-verification.json` for tests and limitations.

The current pin also includes two waste-market proxies that retain the original
source-3.8 treatment shares and collection transport. The qualified notebook
workflow is unchanged. Trucks still needs its two polymer-recycling chains;
see `uvek-trucks-two-waste-markets-38-verification.json` and
`uvek-trucks-recycling-inputs-triage.md` for progress and limitations.

The current pin includes thirty reviewed source-3.8 transport correspondences
needed by the remaining recycling chains, with matching diesel EURO standards
and explicit native fleet-weight limitations. The notebook and qualified-sector
workflow are unchanged; trucks remains unfinished. See
`uvek-recycling-transport-38-verification.json` for checks and scope.

The current pin includes seventeen reviewed source-3.8 material correspondences,
with complete packaging, retained delivery, matched soap/process bases and the
correct plated-sheet area unit. The acid recipe is reserved for its future
recycling parents and grants no global conversion. The notebook and qualified
sector workflow are unchanged. See `uvek-recycling-materials-38-verification.json`.


### Current verified scope: fifteen sectors, including trucks

The installed GitHub Brightpath revision and packaged Premise build pass the
normal live-archive update and Brightway export for these sectors together:

```python
ndb.update([
    "biomass", "electricity", "heat", "cement", "steel", "mining", "renewable",
    "cdr", "fuels", "ships", "buses", "two_wheelers", "trains", "cars", "trucks",
])
```

The example notebook's original three-sector call remains unchanged. For this
optional broader run, replace its update cell with the list above and use the
same export and exploration cells. Battery, final energy, emissions and metals
are still under review; do not add them to this qualified list.

The fifteen-sector export has 17745 datasets,
including 260 regional truck services and 73 regional/World truck markets.
All 272 signed checks of written car/truck services and markets have finite
LCIs. The separate combined physical checks preserve 6,397 redirected consumer
transport quantities, verify the IAM fuel factors and fleet shares, and retain
33 native mass-based battery inventories. All 218 installed-package regressions
pass. The three current truck waste markets retain their original fate shares
and signed emissions with reviewed native operating/incineration proxies.

Only a small legacy kWh-battery guard and the supported-sector list change in
runtime Python; fuel selection and market relinking use mapping files and the
shared Premise routines. Screening and recipe-signature limits remain in
`uvek-trucks-waste-and-update-review.md` and
`uvek-truck-market-alias-review.json`. Complete export evidence is in
`uvek-trucks-public-export-verification.json`. This verifies links, quantities
and finite inventories, without claiming source/native impact equivalence.


### Battery work in progress: six inputs resolved

The current pin adds reviewed source-3.9 pitch, nickel-sulfate, steel-disposal,
solvent-disposal and waste-oil portfolios. The inspected first battery group
falls from eight unresolved identities to two: European municipal waste and
lost-wax aluminium casting. Three production-only kiln receipts remain in
explicit omission audits; other disposal shares are not renormalized. No
production or generator Python code changes in this batch.

All 261 installed-package checks pass, and the normal live-archive preparation
of the qualified fifteen-sector list still imports the same 1,495 CLIC records.
The example notebook and verified sector list remain unchanged. Battery and
final energy are still under review; this checkpoint does not add them to the
qualified list. Details and limitations are in
`uvek-shared-pitch-nickel-39-review.md`,
`uvek-battery-waste-markets-39-review.md` and
`uvek-battery-six-installed-verification.json`.
