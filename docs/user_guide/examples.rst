Example notebooks and integrations
====================================

Example notebooks
-------------------

The `numbered notebook series <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/examples>`_
covers quickstarts, scenario inputs, exports, and analysis workflows for *premise*.

ScenarioLink plugin
---------------------
ScenarioLink lets you download databases generated with Premise for IAM
scenarios directly from Activity Browser, without running Premise yourself.
It works only with **Activity Browser 2.x**. No version compatible with
**Activity Browser 3.x** is currently planned.
You can find it in the `ScenarioLink repository <https://github.com/polca/ScenarioLink>`_.

Example status and prerequisites
----------------------------------

The :doc:`first-scenario script </getting_started/first-scenario>` needs
licensed ecoinvent data and IAM access. The preflight checks local
configuration without building a database. The small example in
:doc:`inventories` uses illustrative data and needs neither input.

Other snippets assume the setup named on their page. An ellipsis (``...``)
marks configuration you must supply; it is not a runnable constructor argument.
Notebook inputs and results also depend on the Premise version and data they
were written for. Start with the current first-scenario script when adapting
an older notebook.
