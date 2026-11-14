Source databases
==================

.. contents:: On this page
   :local:
   :depth: 1

Use an existing `Brightway <https://brightway.dev/>`__ import for the normal
workflow. Alternatively, Premise can read ecospold2 files obtained separately
from `ecoinvent <https://ecoinvent.org>`__. Choose the source version and system
model that match those data.

.. note::

        The ecoinvent database is not included in *premise*.
        You need to have a valid license to download and use it.
        Also, please read carefully ecoinvent's `EULA <https://ecoinvent.org/app/uploads/2024/01/EULA_new_branding_08_11_2023.pdf>`__ before using *premise*.

From a brightway2 project
---------------------------

To extract an ecoinvent database from a Brightway project,
set ``source_db`` to its local name and ``source_version`` to its version.
The following example uses the names configured in
:doc:`/getting_started/preparation`:

.. code-block:: python

  import os
  from premise import NewDatabase
  import bw2data as bw

  bw.projects.set_current(os.environ["PREMISE_BW_PROJECT"])

  ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-NPi", "year":2025}
            ],
        source_db=os.environ["PREMISE_SOURCE_DB"],
        source_version="3.12",
        system_model="cutoff",
        inventory_backend="compact",
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
        key=os.environ["PREMISE_KEY"],
        keep_imports_uncertainty=True,
        keep_source_db_uncertainty=False,
    )

.. note::

    ``source_db`` must match the **name of the database in your Brightway project** (it is not a fixed string).

The defaults retain uncertainty from additional inventories and drop it from
the source database. Set ``keep_source_db_uncertainty=True`` to retain source
uncertainty, or ``keep_imports_uncertainty=False`` to drop imported uncertainty.
Neither option assigns probabilities to IAM scenarios.

.. raw:: html

   <span id="supplemental-inventory-proxies"></span>

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

The source must already use the EN15804 system model. Premise uses
attributional market shares and keeps this identity in caches and exported
metadata. This does not certify an environmental product declaration (EPD).

Supplier and export restrictions are described under
:ref:`native-en15804-inventory-links`.

Prepared source data and additional inventories can be cached for reuse.
See :doc:`inventories` for inspection and cache cleanup, and
:doc:`/reference/inventory-migration` for version-specific supplier proxies.

From ecospold2 files
----------------------

To extract from a set of ecospold2 files, you need to point to the location of
those files in ``source_file_path`` and set ``source_type="ecospold"``.
Replace the example path with your own dataset directory:

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
        inventory_backend="compact",
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
    )

.. note::

    When using ``source_type="ecospold"``, no Brightway project or biosphere
    database is needed unless you later export the result back to Brightway.
