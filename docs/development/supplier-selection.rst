Supplier-selection implementation
===================================

These helpers in ``premise/core/transformation.py`` select suppliers when
relinking an input. For the modelling explanation, read
:doc:`/methodology/regionalization`. These are internal helpers rather than
the public inventory API.

.. raw:: html

   <span id="decision-is-the-exchange-in-cache"></span>

.. _decision-tree:

How a supplier is selected
---------------------------

1. **Reuse a cached match.** ``process_cached_exchange`` copies the saved
   supplier choice into the new exchange list.
2. **Find candidates.** ``process_uncached_exchange`` checks the supplier
   index, then searches the database by activity name and reference product
   if the index has no candidates. A successful search also updates the index.
3. **Handle one candidate.** ``handle_single_possible_dataset`` uses the
   matching supplier and retains the requested product.
4. **Choose among several candidates.** ``handle_multiple_possible_datasets``
   first checks the consuming activity's location. Otherwise,
   ``process_complex_matching_and_allocation`` applies the geographical rules.
5. **Use the appropriate geographical rule.** ``handle_iam_region`` handles
   IAM regions. ``handle_global_and_row_scenarios`` handles broad locations.
   Other locations use geographical matching. Supplier shares depend on the
   matching and allocation options supplied by the caller.

If no candidates are found, the uncached helper prints a diagnostic and
returns an exchange at the consuming activity's location, retaining its
quantity and available uncertainty fields. This does not establish a resolved
supplier; later linking and validation still matter.

Final Steps
-------------

``handle_default_option`` provides the final fallback where the geographical
matching path calls for it. The caller collects the resulting exchanges and
checks the relinked inventory. Do not infer that every input has a suitable
local supplier merely because this helper completed.
