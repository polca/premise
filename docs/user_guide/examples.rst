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

The first-scenario script is executable with licensed inputs. The preflight
checks configuration without building a database. Inventory inspection,
validation-findings and sensitivity examples execute in isolated storage with
illustrative data. Download the :download:`execution record </examples/status.json>`
for the date and outcome of the latest documentation checks.

Inline Python snippets elsewhere require the setup identified on their page.
An ellipsis (``...``) denotes omitted user configuration and is not a runnable
constructor argument. These partial snippets are syntax-checked, not counted
as complete scenario executions. API examples in docstrings follow the same
convention.
