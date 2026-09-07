Choosing models, scenarios and years
======================================

Choose a model and pathway appropriate to the question, then select the
year to represent. A scenario is a conditional projection, not a forecast.

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

For custom projections, see :doc:`external-scenarios` and
:doc:`/development/iam-mapping`.
