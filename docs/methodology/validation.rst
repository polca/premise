Uncertainty and validation
============================

.. raw:: html

   <span id="validation-approach-and-limitations"></span>


Validation certifies implemented inventory contracts, including production,
finite values, supplier links, market composition and sector-specific bounds.
It does not establish that an IAM scenario is correct or independently verify
all source inventories. Regression scores detect changes relative to a
baseline; they are not external scientific validation.

Read :doc:`/user_guide/validation` for the API and certificate lifecycle,
:doc:`/user_guide/reports` for reviewing changes, and the sector pages for
technology-specific checks and limitations.

Uncertainty
-------------

Source and imported exchange uncertainties can be retained separately through
``keep_source_db_uncertainty`` and ``keep_imports_uncertainty``. These controls
do not represent uncertainty in all IAM assumptions or structural choices.
Scenario comparisons and sensitivity analyses are needed to assess those
choices. Follow :doc:`/user_guide/interpreting-results` for a worked example. See :doc:`/user_guide/source-databases` and the sector methodology.
