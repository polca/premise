Regionalization and supplier selection
========================================

.. raw:: html

   <span id="regionalization-and-supplier-linking"></span>


Regionalization creates location-specific versions of mapped activities and
links their inputs to suitable suppliers. The IAM region of an electricity or
fuel market does not imply that every manufacturing input is produced there.

Matching suppliers
--------------------

Supplier selection begins with the activity and product being requested.
Geography then helps select among matching candidates: a direct location
match, locations within an IAM region, or geographical overlap may be used.
See :doc:`/reference/geography` for the country-to-region correspondence.

If several suppliers qualify, production volumes are used to allocate their
shares where available. Broader geographical fallbacks may be needed when no
representative local supplier exists. The exact filters and fallback policy
are transformation-specific; sector pages document important exceptions.

Relinking and limitations
---------------------------

Relinking redirects affected consumers to the selected or newly generated
suppliers while retaining the meaning of their product and functional unit.
Market shares, input units and any conversion must therefore be read together.
A geographical fallback is a modelling proxy, not evidence of local production.

See :doc:`shared-principles` for market construction and
:doc:`validation` for supplier-link checks.

.. raw:: html

   <span id="decision-is-the-exchange-in-cache"></span>


.. raw:: html

   <span id="final-steps"></span>
