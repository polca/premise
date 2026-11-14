Choosing models, scenarios and years
======================================

Choose a model and pathway appropriate to the question, then select the
year to represent. A scenario is a conditional projection, not a forecast.

Use the `Premise IAM Scenario Explorer <https://premisedash-6f5a0259c487.herokuapp.com/scenarios/>`_
to browse the scenarios distributed with different Premise releases and
compare their data by model, scenario, sector, region and year. This can help
you choose a pathway and inspect the projections relevant to your study.

1. Check :doc:`/reference/models` for model scope and :doc:`/reference/coverage`
   for the distinction between model capability, mapped variables and file coverage.
2. Consult :doc:`/reference/scenarios` for pathway descriptions and ensure
   the corresponding scenario file is available.
3. Set the ``model``, ``pathway`` and ``year`` in the scenario dictionary.
4. Record the *premise* version, ecoinvent version/system model, IAM file and
   any custom inputs so the calculation can be reproduced.

Use :doc:`/getting_started/first-scenario` for a complete example. Differences
between models can reflect sector coverage, resolution and assumptions;
matching a scenario label does not guarantee equivalent modelling boundaries.

Battery technology assumptions
-------------------------------

Battery mixes are separate from the IAM pathway. The
:ref:`mobile scenarios <mobile-battery-mix-scenarios>` compare LFP-led,
nickel-based, post-lithium-ion and mixed adoption; the
:ref:`stationary scenarios <stationary-battery-mix-scenarios>` compare
continuity and technology change. ``ndb.update("battery")`` updates all
available mixes. The capacity-market link in the consuming inventory selects
which one is used; there is no constructor-wide battery-scenario selector.
Record that market alongside the IAM pathway when reporting your assumptions.

For custom projections, see :doc:`external-scenarios` and
:doc:`/development/iam-mapping`.
