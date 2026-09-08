Interpreting changes and uncertainty
======================================

Trace a change
----------------

A change in an impact score can come from different suppliers, different
input amounts, or changes elsewhere in the supply chain. Use the
:doc:`change report <reports>` to identify inventory changes, then an LCA
contribution analysis to establish how those changes affect the score.
The largest inventory change is not necessarily the largest impact contribution.

.. raw:: html

   <span id="start-with-a-comparable-functional-unit"></span>
   <span id="a-historical-pv-comparison"></span>
   <span id="an-executable-sensitivity-example"></span>

1. **Make the comparison consistent.** Match the reference product, unit,
   geography, ecoinvent system model and LCIA method. Check what each activity
   supplies: electricity at a power plant and electricity delivered at low
   voltage include different processes and losses. Record the scenario, year,
   database version and custom inputs for each calculation. A missing match
   must not be treated as a zero impact.

2. **Check the market composition.** In the **Market Changes** sheet, look
   for suppliers added or removed and changes in their shares. A market score
   can fall because lower-impact suppliers provide more of the product, even
   if each supplier's own inventory is unchanged. Conversely, unchanged shares
   can still give a different market score if supplier inventories change.

3. **Compare amounts per unit of output.** Inspect energy and material inputs,
   direct emissions and the reference production amount. Check the relevant
   efficiency, lifetime and yield assumptions. For example, a lower material
   requirement per kWh of PV electricity may reflect higher lifetime electricity
   production rather than less material per square metre of panel. Consult the
   corresponding :doc:`sector methodology </methodology/index>` to identify
   which quantities Premise adjusts.

4. **Trace the supply-chain contributions.** Calculate contributions to the
   same impact category before and after the change. An unchanged electricity
   input can contribute less if its supplier's generation mix changes. Compare
   contributions at the same level: a supplier's contribution already includes
   its own supply chain, so adding that contribution to its suppliers'
   contributions would count some impacts twice.

5. **Test explanations and account for the remainder.** Where possible, vary
   one assumption at a time while holding the others fixed. Compare the effect
   with the total score difference and report any unexplained remainder.
   Changes can interact, so effects measured separately may not add up to the
   combined effect. A passing validation report helps rule out the issues it
   checks; it does not establish the cause of a score change.

For a reproducible explanation, retain the two total scores, the main changed
inputs or supplier shares, their impact contributions, and the assumptions
used in any sensitivity calculations. State which parts of the difference are
explained by this evidence and which remain uncertain.

Three kinds of uncertainty
----------------------------

These sources of uncertainty answer different questions and require different
analyses. A single uncertainty range rarely captures all three.

.. list-table::
   :header-rows: 1
   :widths: 22 38 40

   * - Kind
     - What it concerns
     - How to assess it
   * - Exchange uncertainty
     - Uncertain quantities within an inventory, such as a material input,
       fuel requirement or emission factor.
     - Retain available exchange distributions and sample them in an LCA
       calculation. Check which exchanges have distributions and whether
       dependencies between quantities are represented.
   * - Scenario uncertainty
     - Different assumptions about future technology, demand and policy,
       represented by alternative pathways or IAMs.
     - Build and compare scenarios using consistent products, units and LCIA
       methods. Use several models where relevant to assess dependence on
       the model's assumptions.
   * - Structural uncertainty
     - Choices about how the system is represented, such as substitute
       datasets, geographical coverage, allocation or system boundaries.
     - Construct alternative representations and compare their results.
       State exactly which modelling choice changed and why each alternative
       is relevant to the study.

**Exchange uncertainty** covers only the distributions actually included in
and used by the calculation. Premise's ``keep_source_db_uncertainty`` and
``keep_imports_uncertainty`` options control retention of source and imported
exchange uncertainties. They do not assign distributions to every IAM
assumption or run a Monte Carlo analysis. Scaling an exchange with a retained
distribution also does not, by itself, represent uncertainty in the scaling
factor. See :doc:`source-databases` for the retention options.

**Scenario uncertainty** describes alternative futures rather than random
samples from a known probability distribution. A range across pathways is
therefore a scenario range, not a statistical confidence interval. Comparing
years within one pathway describes change over time; it does not replace a
comparison of alternative futures for the same year. Check differences in
sector coverage before attributing the spread solely to scenario assumptions.

**Structural uncertainty** can remain even when all input quantities are
known precisely. For example, using a dataset from another region may be
reasonable but still affect the result. Test a different supplier where one
is available. A comparison of cut-off and consequential modelling is a broader
change in the question and modelling approach; explain that distinction when
presenting the results.

Report these analyses separately: the scenario settings, the distributions
sampled, and the alternative modelling choices tested. If you combine them,
explain how. Identify important assumptions that were left fixed so readers
can judge what the reported range does and does not cover.
