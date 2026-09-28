Sequential scenario arrays
============================

.. contents:: On this page
   :local:
   :depth: 1

With modern Brightway (``bw2data >= 4``), *premise* can write one union
database and one compressed ``bw_processing`` ZIP for deterministic scenario
enumeration. The package contains joint technosphere and biosphere arrays in
this order: ``original`` first, followed by the generated scenarios in
``ndb.scenarios`` order.

This export is useful when the same functional unit must be evaluated across
many *premise* scenarios without writing one Brightway database per scenario.
It is separate from the Activity Browser superstructure workflow: no scenario
difference CSV is created.

Requirements and output
-------------------------

The export requires all of the following:

* modern Brightway (``bw2data >= 4``) and ``bw_processing >= 1.0``;
* an active Brightway project containing the source and configured biosphere
  databases;
* at least two generated scenarios with unique labels; and
* a completed scenario transformation, normally ``ndb.update()``.

Calling ``write_scenario_array_db_to_brightway`` writes the union database to
the active project before resolving the Brightway IDs used by the array ZIP.
The return value is the absolute path to the ZIP. If ``filepath`` is omitted,
the package is written to
``export/scenario arrays/scenario_array_<sanitized-name>.zip``. When supplied,
``filepath`` is the complete destination filename and must end in ``.zip``;
missing parent directories are created and an existing ZIP is replaced
atomically.

Complete three-scenario IMAGE example
---------------------------------------

The following example uses the ``ecoinvent-3.12-cutoff`` project and applies
all sector updates to three IMAGE pathways for 2050. Set the ``PREMISE_KEY``
environment variable before running it. The source and biosphere database names must
match the names registered in your project.

.. code-block:: python

    import os

    import bw2data as bd

    from premise import NewDatabase

    PROJECT = "ecoinvent-3.12-cutoff"
    SOURCE_DATABASE = "ecoinvent-3.12-cutoff"
    BIOSPHERE_DATABASE = "ecoinvent-3.12-biosphere"
    DATABASE_NAME = "premise-image-2050-three-pathways-array"
    SCENARIOS = [
        {"model": "image", "pathway": "SSP1-L", "year": 2050},
        {"model": "image", "pathway": "SSP2-M", "year": 2050},
        {"model": "image", "pathway": "SSP3-H", "year": 2050},
    ]

    bd.projects.set_current(PROJECT)
    missing = [
        name
        for name in (SOURCE_DATABASE, BIOSPHERE_DATABASE)
        if name not in bd.databases
    ]
    if missing:
        raise ValueError(f"Missing Brightway databases: {missing}")

    ndb = NewDatabase(
        scenarios=SCENARIOS,
        source_db=SOURCE_DATABASE,
        source_version="3.12",
        source_type="brightway",
        system_model="cutoff",
        biosphere_name=BIOSPHERE_DATABASE,
        key=os.environ["PREMISE_KEY"],
    )
    ndb.update()  # apply all sector transformations

    array_path = ndb.write_scenario_array_db_to_brightway(
        name=DATABASE_NAME,
    )
    print(array_path)

The one export call writes both ``DATABASE_NAME`` to the active Brightway
project and the returned array ZIP. Do not call ``write_db_to_brightway`` first
for the same database name.

Calculate and enumerate scores
--------------------------------

Select activities by their name, reference product, and location, and require
exactly one match. This example evaluates one kilowatt-hour of Swiss
low-voltage electricity with the ecoinvent 3.12 EF v3.1 GWP100 method.

.. code-block:: python

    import bw2calc as bc
    import numpy as np

    from premise.utils import create_scenario_list

    method = (
        "ecoinvent-3.12",
        "EF v3.1",
        "climate change",
        "global warming potential (GWP100)",
    )
    if method not in bd.methods:
        raise ValueError(f"Missing LCIA method: {method}")

    database = bd.Database(DATABASE_NAME)
    matches = [
        activity
        for activity in database
        if activity.get("name") == "market for electricity, low voltage"
        and activity.get("reference product") == "electricity, low voltage"
        and activity.get("location") == "CH"
    ]
    if len(matches) != 1:
        raise ValueError(f"Expected one matching activity; found {len(matches)}")
    functional_unit = matches[0]

    demand, data_objs, remapping = bd.prepare_lca_inputs(
        {functional_unit: 1},
        method=method,
    )

    lca = bc.LCA(
        demand,
        data_objs=[*data_objs, array_path],
        remapping_dicts=remapping,
        use_arrays=True,
        use_distributions=False,
    )

    lca.lci()
    lca.lcia()
    scores = [lca.score]  # original

    for _ in ndb.scenarios:
        next(lca)
        scores.append(lca.score)

    labels = ["original", *create_scenario_list(ndb.scenarios)]
    print(dict(zip(labels, scores)))

    # The next selection after the final scenario wraps to original.
    next(lca)
    assert np.isclose(lca.score, scores[0])

The returned ZIP must be appended *after* the database datapackages in
``data_objs`` so its values override the changing coordinates in the base
database. The initial LCA represents ``original``. Each ``next(lca)`` advances
the technosphere and biosphere arrays together to the next complete scenario;
the selection after the final scenario wraps to ``original``.

To confirm that the written database remains at its original values without
the overlay, create a second LCA with the unmodified ``data_objs``:

.. code-block:: python

    base_lca = bc.LCA(
        demand,
        data_objs=data_objs,
        remapping_dicts=remapping,
        use_arrays=False,
        use_distributions=False,
    )
    base_lca.lci()
    base_lca.lcia()
    assert np.isclose(base_lca.score, scores[0])

Only coordinates that vary across ``original`` and the generated scenarios are
stored in the ZIP. Existing exchange uncertainty distributions are not copied
to the scenario arrays, and this version does not combine array selection with
parameter uncertainty, weights, seeds, or random sampling.

.. warning::

    The ZIP is tied to the active Brightway project and to the IDs assigned to
    the written database. Regenerate it after moving, deleting, or rewriting
    that database. These arrays enumerate scenarios deterministically; they are
    not probability-weighted Monte Carlo samples.

The repository's `scenario-array notebook <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/examples/07_sequential_scenario_arrays.ipynb>`__ includes the complete three-activity
workflow and reference scores from an ecoinvent 3.12 validation run.
