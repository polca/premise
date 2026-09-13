Superstructure databases
==========================

If several scenario databases are built, *premise* can generate a superstructure database,
as explained in `Steubing <https://link.springer.com/article/10.1007/s11367-021-01974-2>`__ et al, 2021. This allows to explore several scenarios
while writing only one database in a brightway2 project. Besides writing the
database to disk, this also creates a *scenario difference file* that will be read
by `Activity-Browser <https://github.com/LCA-ActivityBrowser/activity-browser>`__.

This is done as follows:

.. code-block:: python

    ndb.write_superstructure_db_to_brightway()

.. note::

    Superstructure export requires at least two scenarios; otherwise ``write_superstructure_db_to_brightway``
    raises an error.

You can also specify a file path for the export of the scenario
difference file:

.. code-block:: python

    ndb.write_superstructure_db_to_brightway(filepath="some_file_path")

Finally, you can also give a name to the superstructure database:

.. code-block:: python

    ndb.write_superstructure_db_to_brightway(filepath="some_file_path", name="my_db")


.. note::

    Superstructure databases can only be used by Activity-Browser at the moment.
