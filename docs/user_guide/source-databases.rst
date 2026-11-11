Source databases
==================

.. contents:: On this page
   :local:
   :depth: 1

*premise* can extract the ecoinvent database from:

* a `brightway2 <https://brightway.dev/>`__ project that contains the ecoinvent database
* ecospold2 files, that can be downloaded from the `ecoinvent <https://ecoinvent.org>`__ website

.. note::

        The ecoinvent database is not included in *premise*.
        You need to have a valid license to download and use it.
        Also, please read carefully ecoinvent's `EULA <https://ecoinvent.org/app/uploads/2024/01/EULA_new_branding_08_11_2023.pdf>`__ before using *premise*.

From a brightway2 project
---------------------------

To extract an ecoinvent database from a Brightway project,
indicate the database name in `source_db` and its version in `source_version`:

.. code-block:: python

  import os
  from premise import NewDatabase
  import bw2data as bw

  bw.projects.set_current("my_project")

  ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-NPi", "year":2025}
            ],
        source_db="ecoinvent-3.12-cutoff",
        source_version="3.12",
        system_model="cutoff",
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
        key=os.environ["PREMISE_KEY"],
        keep_imports_uncertainty=True, # True by default, set to False to drop uncertainty in additional inventories
        keep_source_db_uncertainty=False # False by default, set to True if you want to keep ecoinvent's uncertainty data
    )

.. note::

    ``source_db`` must match the **name of the database in your Brightway project** (it is not a fixed string).

EN15804 source databases
~~~~~~~~~~~~~~~~~~~~~~~~

Use ``system_model="EN15804"`` when the source database is the ecoinvent
EN15804 system model, for example::

    ndb = NewDatabase(
        scenarios=[{"model": "remind", "pathway": "SSP2-NPi", "year": 2050}],
        source_db="ecoinvent-3.12-EN15804",
        source_version="3.12",
        system_model="EN15804",
        key=os.environ["PREMISE_KEY"],
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
    )

EN15804 uses attributional IAM market shares and retains secondary-material
technologies. Its supplemental inventories use the attributional source
selection, with explicit native EN15804 scrap suppliers checked before linking.
New aluminium scrap uses the RoW market as a geographic proxy; its positive
reference output credits the existing negative scrap handoff. Post-consumer
aluminium and iron use negative-output treatment markets. Copper scrap uses
the native burden-free provider. Signed quantities and uncertainty are retained.
These adaptations do not add a separate recovery credit or certify an EPD.

EN15804 caches and exported metadata retain their system-model identity.
Brightway, matrix, datapackage and openLCA exports retain this identity.
SimaPro export requires a Brightpath version with EN15804 naming support;
versions supporting only cut-off and consequential naming raise an error
before scenario preparation or file publication.

The native supplier contracts are verified against ecoinvent 3.12 EN15804.
An absent, ambiguous or incompatible native scrap supplier raises an error.

Supplemental inventory proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For consequential imports, the ASSB recycled HDPE purchase uses the native
marginal HDPE market at the same mass: GLO through 3.11, and RER in 3.12 to
retain European supply. Cut-off and EN15804 retain the recycled supplier.

Two chemical proxies apply only to ecoinvent 3.8, where the native markets
are unavailable. The polymer ASSB uses the bundled Japanese lithium iron
phosphate synthesis recipe. The iridium inventory uses the GLO organic
chemical market as a manufacturing-burden proxy for sodium ethyl xanthate.
Both retain the original kg quantities. The latter is a weaker proxy and
does not establish chemical or environmental equivalence. Imported exchange
comments identify these substitutions; later versions use the native markets.

Note that a cache of the database will be created the first time and
stored in the configured user data directory. Compatible subsequent builds
can reuse it; source and inventory fingerprints determine cache validity.
See :doc:`inventories` for cache and checkpoint behaviour.

To clear source and inventory caches, scenario checkpoints and default CLIC
caches, first release all ``NewDatabase`` instances, then run:

.. code-block:: python

    from premise import clear_cache

    clear_cache()



To clear only the *premise* additional inventories, do:

.. code-block:: python

    import os
    from premise import clear_inventory_cache

    clear_inventory_cache()

.. note::

    After a version update, databases and inventories are automatically
    re-extracted and re-imported. This is to ensure that the data is
    consistent with the new version of *premise*.


From ecospold2 files
----------------------

To extract from a set of ecospold2 files, you need to point to the location of
those files in `source_file_path`, as well as indicate the database format in
`source_type`:

.. code-block:: python

    import os
    from premise import NewDatabase

    ndb = NewDatabase(
        scenarios = [
            {"model":"remind", "pathway":"SSP2-NPi", "year":2025}
                    ],
        source_type="ecospold",
        key=os.environ["PREMISE_KEY"],
        source_file_path=r"C:\file\path\to\ecoinvent 3.12_cutoff_ecoSpold02\datasets",
        source_version="3.12",
        system_model="cutoff",
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
    )

.. note::

    When using ``source_type="ecospold"``, no Brightway project or biosphere
    database is needed unless you later export the result back to Brightway.

UVEK 2026 with the ecoinvent 3.10 biosphere
------------------------------------------

This development branch installs Brightpath from a qualified GitHub revision,
including the scoped-approval module and reviewed mappings required by UVEK.
Released Brightpath ``1.0.0a14`` lacks these changes, although the development
revision currently has the same version number. Git must be installed so pip
can fetch this dependency.

From the Premise checkout, run this command in the environment that runs Jupyter:

.. code-block:: console

    python -m pip install -e ".[bw25]"

Premise pins Brightpath revision
``6150ec80bbaf8a4a07650ffbb91231b02a24f98e`` for reproducibility. No adjacent
Brightpath checkout is required. Restart the notebook kernel after upgrading.

When upgrading an existing environment that already has another GitHub
alpha14 revision, force the companion replacement once:

.. code-block:: console

    python -m pip install --force-reinstall --no-deps "brightpath @ git+https://github.com/romainsacchi/brightpath.git@6150ec80bbaf8a4a07650ffbb91231b02a24f98e"

With the native database and biosphere installed in the selected Brightway
project, use the usual scenario workflow:

.. code-block:: python

    ndb = NewDatabase(
        scenarios=[{"model": "remind", "pathway": "SSP2-NPi", "year": 2050}],
        source_db="BAFU:2026-reimported",
        source_family="uvek",
        source_version="2026",
        biosphere_name="ecoinvent-3.10-biosphere",
        key=os.environ["PREMISE_KEY"],
    )
    ndb.update(["biomass", "electricity", "heat", "cement"])
    ndb.write_db_to_brightway()

The database names above are examples; no custom Brightway background labels are
required. UVEK uses the shared sector transformations. Electricity updates create
high-, medium- and low-voltage IAM-region markets, including REMIND EUR and NEU,
and connect consumers directly or through the usual country-market handovers.

This four-sector REMIND workflow has been checked through export and LCI.
Cement uses the shared clinker efficiency, CCS, market and producer transformations,
with native product/filter aliases declared in YAML. Cement grades retain their
clinker doses and other recipe quantities; source geography and technology
approximations remain explicit warnings. The no-argument update default remains
biomass, electricity and heat; request cement explicitly. Other sectors remain
outside the current UVEK qualification. Existing inventory
proxies, explicit omissions and geographic/loss-weighting approximations remain
screening limitations. See the :download:`implementation audit <../development/uvek-standard-workflow.md>`
for the scope, assumptions and verification record.
