Exporting sparse matrices
===========================

Use this export when you need inventory coefficients for your own numerical
workflow. It writes CSV coordinates for the technosphere and biosphere, plus
files identifying their indices. It does not calculate impact scores.

After updating ``ndb``:

.. code-block:: python

   ndb.write_db_to_matrices(filepath="export/matrices")

Premise prints the destination for each scenario. In its directory you will find:

* ``A_matrix.csv``: activity index, supplied-product index and exchange amount.
* ``B_matrix.csv``: activity index, biosphere-flow index and exchange amount.
* ``A_matrix_index.csv``: activity name, product, unit, location and index.
* ``B_matrix_index.csv``: flow name, compartment, subcompartment, unit and index.

Both coefficient files use semicolons and contain the columns ``value``,
``uncertainty type``, ``loc``, ``scale``, ``shape``, ``minimum``, ``maximum``,
``negative`` and ``flip`` after their two indices. Production rows in A have
``flip=0``; purchased technosphere inputs have ``flip=1``. Amounts retain their
signs. Choose and apply your solver's matrix orientation and sign convention
explicitly; the CSV coordinates are not a ready-made LCIA model.

For example, inspect the first coefficients without changing their signs:

.. code-block:: python

   import csv
   from itertools import islice
   from pathlib import Path

   # Replace this with the scenario directory printed by the exporter.
   directory = Path("path/to/scenario/matrices")
   with (directory / "A_matrix.csv").open(encoding="utf-8", newline="") as stream:
       for row in islice(csv.DictReader(stream, delimiter=";"), 5):
           print(row["index of activity"], row["index of product"], float(row["value"]))

An impact calculation also needs a demand, a solved supply chain and a
compatible characterization method. Use :doc:`brightway` or
:doc:`scenario-arrays` for workflows that calculate with Brightway instead of
assembling these parts yourself.
