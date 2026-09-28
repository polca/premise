Exporting to openLCA
======================

*premise* can export the databases as a modified version of Simapro-CSV files compatible with `OpenLCA <https://www.openlca.org/>`__.

This is done as follows:

.. code-block:: python

    ndb.write_db_to_olca()

.. note::

    The categorization of imported activities may differ from OpenLCA's
    original classification.


Then, create a database from scratch (in older versions this is the “create empty database” option).

.. image:: /olca_fig0.png
   :width: 500pt
   :align: center
   :alt: OpenLCA create database interface screenshot

.. note::

    In older versions the import steps were different (an empty database had to be used for the import,
    rather than a complete reference database with the openLCA elementary flows), as this complete reference
    version will have flows not required by Premise.

Import the file as a SimaPro CSV (import>other>Other LCA formats>SimaPro CSV).

.. image:: /olca_fig1.png
   :width: 500pt
   :align: center
   :alt: OpenLCA import SimaPro CSV interface screenshot


You will need to select "SimaproCSV_Import.csv" as mapping file to use.

.. image:: /olca_fig2.png
   :width: 500pt
   :align: center
   :alt: OpenLCA mapping file selection screenshot

Then import the ecoinvent impact assessment methods (available for free on OpenLCA's `Nexus <https://nexus.openlca.org/>`__ platform)
into the Premise database as JSON-LD.

.. image:: /olca_fig3.png
   :width: 500pt
   :align: center
   :alt: OpenLCA import impact assessment methods screenshot

Select the option "Overwrite all existing datasets" before importing, because our elementary flows may have
more descriptions or never update existing data set to keep the descriptions from the CSV export of Premise for
the elementary flows.

.. image:: /olca_fig4.png
   :width: 500pt
   :align: center
   :alt: OpenLCA overwrite datasets option screenshot
