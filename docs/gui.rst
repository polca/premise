Graphical user interface
========================

``premise`` ships with a local browser-based graphical user interface for the
main configuration and export workflows.

Overview
--------

The GUI is intended for users who prefer configuring runs interactively rather
than working directly in Python or notebooks. It runs entirely on the local
machine: a small local web service is started, then the interface opens in the
browser.

With the GUI, you can:

* create, save, clone, and reopen ``premise`` configurations;
* choose a Brightway or ecospold source database;
* inspect local IAM scenario files and download the known bundled scenario set;
* store the IAM scenario decryption key locally for the current machine;
* compare IAM scenarios in the Scenario Explorer;
* queue dry runs or full runs and monitor progress, logs, artifacts, and diagnostics;
* export results to Brightway, matrices, datapackages, SimaPro, OpenLCA, and superstructure outputs, depending on workflow and scenario count.

Starting the GUI
----------------

After installing ``premise`` and activating the corresponding Python
environment, start the interface with:

.. code-block:: bash

   premise-ui

This launches the local service and opens the browser automatically when the
application is ready.

If you prefer to open the browser yourself:

.. code-block:: bash

   premise-ui --no-browser

The GUI does not require you to be inside the repository directory. The
``premise-ui`` command works from any location as long as the environment where
``premise`` is installed is active.

Getting started
---------------

1. Install ``premise`` and activate the environment.
2. Run ``premise-ui``.
3. Choose a workflow and a source database in the *Source* tab.
4. Select one or more IAM scenarios in the *Scenarios* tab.
5. Optionally inspect those scenarios in the *Scenario Explorer* tab.
6. Choose transformations and an export target.
7. Queue a run and monitor progress in the *Run Monitor* tab.

If you use encrypted IAM scenarios, request the decryption key from the
developers and enter it in the *IAM Scenario Key* panel of the GUI.

Typical workflow
----------------

The GUI is organized around the same core steps as the Python API:

* **Source**: select a Brightway project and source database, or point to an
  ecospold directory.
* **Scenarios**: define one or more scenario sets, or reuse downloaded local IAM
  scenario files.
* **Transformations**: keep the default sector set or restrict it for a given run.
* **Export**: choose a target such as Brightway, matrices, or datapackage.
* **Run monitor**: inspect queue position, phase logs, artifacts, diagnostics,
  and support bundles.

Configurations are saved as JSON files. These are separate from Brightway
projects and can be copied, versioned, or reopened later.

Scenario Explorer
-----------------

The *Scenario Explorer* is a read-only analysis surface embedded in the GUI. It
lets you:

* load installed IAM scenarios from the local IAM output directory;
* browse sectors from the same summary logic used by ``premise`` reports;
* compare scenarios by region or sub-scenario, variable, and year range;
* view line, stacked-area, grouped-bar, and stacked-bar summaries;
* export plots and inspect values before running a transformation workflow.

Support and diagnostics
-----------------------

When a run fails or needs review, the GUI can display diagnostics, list
artifacts, and generate a redacted support bundle. The troubleshooting tab is
intended to make local debugging easier before contacting the maintainers.

Python API still available
--------------------------

The GUI complements the Python API; it does not replace it. Notebook and script
workflows remain fully supported and are still the right choice for automation,
batch processing, or custom integration in larger pipelines.
