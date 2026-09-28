Exporting a datapackage
=========================

*premise* can export the databases as a data package, which is a standardized way of
packaging data. This is useful when you want to share your databases with others,
without sharing the source database (i.e., ecoinvent), which is under restrictive license.

This is done as follows:

.. code-block:: python

    ndb.write_datapackage()

This creates a zip file that contains the all the data necessary for
other users to replicate the databases, provided they have access
to the source database locally.

See the library <``unfold`` https://github.com/polca/unfold/tree/main>_ for more information on data packages
for sharing LCA databases. ``unfold`` can read these data packages and create
brightway2 databases (or superstructure databases) from them.
``unfold`` can also fold premise databases registered in your brightway2 project
into data packages, to be shared with and recreated by others.
