Exporting to SimaPro
======================

*premise* can export the databases as Simapro-CSV files.

This is done as follows:

.. code-block:: python

    ndb.write_db_to_simapro()

.. note::

    The categorization of activities in the Simapro activity tree looks different
    from that of the original ecoinvent database accessed from Simapro. That is because
    *premise* relies on ISIC v.4 and CCP classifications to categorize activities.
    Also, a number of activities do not have a category and are found under *Meterials/Others*.
