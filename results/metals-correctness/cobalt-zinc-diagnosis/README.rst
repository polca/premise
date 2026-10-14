Cobalt and zinc: diagnosis of the RELICS score collapse
=======================================================

This is the archived diagnosis of the ``e54ac3e2`` implementation. The subsequent
fix and validation are documented in ``../resource-fix/README.rst``.

The large decreases are regressions in extraction attribution. They are not
resource savings. The conservative upstream search introduced by the correctness
update interacts badly with broad name matching and circular process inputs.

This investigation uses ecoinvent 3.12, IMAGE SSP2-M 2050, metals-only builds,
in both cutoff and consequential. The functional unit is one kilogram of the
global market product. No transformation code or Brightway database was changed.

Confirmed mechanisms
--------------------

Cobalt
~~~~~~

The economic-allocation electrolysis route in China follows this path:

* cobalt metal production -> cobalt sulfate production;
* cobalt sulfate production -> cobalt hydroxide production in DR Congo;
* cobalt hydroxide production -> electricity, medium voltage, cobalt industry;
* that electricity market consumes itself (0.0046 kWh per kWh), and its
  high-voltage supplier has another self-loop.

``exchange_may_carry_target_resource`` matches the word "cobalt" in the
electricity product/name and does not check the exchange unit. Thus electricity
is mistakenly included in the cobalt material path. Encountering a visited
supplier returns an unresolved result, which propagates to the metal producer.
``get_missing_target_resource_dataset_ids`` only schedules additions when the
result is explicitly False. It therefore skips the target resource addition.

The copper-cobalt ore providers are explicitly classified as carriers whose
metal extraction is attributed downstream. They cannot fill the resulting gap.
The old implementation skipped visited suppliers and returned False when no
resource was found, so the same loop did not prevent its 1 kg addition.

Seventeen regional electrolysis datasets had a 1 kg cobalt addition before the
update and no direct cobalt extraction afterwards. In both fresh pre-correction
graphs, ignoring non-kilogram inputs for the diagnostic search alone changes
the result from None to False. There are no unresolved provider links and no
direct-resource terminals in the traversed pre-correction graph.

Zinc
~~~~

The Chinese and RoW primary-smelting templates have small positive inputs of
zinc and zinc oxide, in addition to the main concentrate feed. In cutoff,
the Chinese route uses 0.00232466 kg zinc, 0.03832861 kg zinc oxide, and
1.49274755 kg concentrate per kg zinc output. In consequential these are
0.00284117, 0.04684475, and 1.82441735 kg respectively.

The zinc input links to the World zinc market, which links back to the primary
producers. In consequential, zinc oxide production also consumes zinc from
that market. These are real supply-chain loops, unlike cobalt's erroneous
electricity classification. The search lacks a distinction between the main
feed and small process inputs. It cannot establish where extraction belongs
inside the loop, returns None, and skips the primary producer's addition.

Zinc concentrate is another explicitly excluded downstream-attribution carrier.
Its principal extraction is cleared by the correction policy, but the skipped
downstream additions leave that extraction unrepresented. Twenty-nine regional
primary-production datasets lose their previous 1 kg zinc addition.

The Canadian template has the concentrate feed without the small zinc and
zinc-oxide inputs. Its search returns False and its addition survives. Canada's
World-market share is 0.04680746. Its direct extraction contributes
0.04693971 kg/kg in the cutoff LCA, 99.83% of the remaining 0.04701744 score.
The difference from its direct market share includes indirect supply-chain
demand.

In both fresh pre-correction graphs, excluding only the positive zinc and
zinc-oxide inputs on primary zinc producers from the diagnostic traversal changes
None to False. No provider is missing or ambiguous. Inspecting the final graph
alone can obscure the cause: the newly added Canadian flow then makes upstream
reachability True, although it does not cover the other producers' extraction.

Quantitative isolation
----------------------

The following calculation holds the corrected technosphere and its solved
supply vector fixed. It restores only the previous direct resource amounts on
the identified primary routes, in memory. All resource flows, suppliers and
registered methods in the actual databases remain unchanged.

.. list-table:: kg extracted element per kg market product
   :header-rows: 1

   * - Market
     - Model
     - Before corrections
     - Current result
     - Restore primary additions only
   * - Cobalt
     - Cutoff
     - 1.00004511
     - 0.00004066
     - 0.99432933
   * - Cobalt
     - Consequential
     - 0.99999909
     - -0.000000006
     - 0.99999904
   * - Zinc
     - Cutoff
     - 1.03586399
     - 0.04701744
     - 1.00290303
   * - Zinc
     - Consequential
     - 1.04427039
     - 0.04888402
     - 1.04431846

This isolates the dominant cause; it is not a proposed final inventory model.
The cutoff cobalt value is consistent with its retained 99.43% primary share.
Other corrections remain active, including the removal of the old 1 kg zinc
resource addition from ``zinc scrap, post-consumer to generic market for zinc``.
Consequently, restoring the primary additions alone need not reproduce every
previous score. No complete decomposition of those residual differences was
attempted.

Required correction
-------------------

* Restrict material-path matching using product identity, compatible units and
  explicit metal-bearing forms. Industry words in energy or service names must
  not establish a material path.
* Declare extraction-attribution boundaries and distinguish main feeds from
  auxiliary/recycled inputs. The existence of any upstream metal resource is
  not proof that the output's primary extraction is fully represented.
* Resolve cycles as connected material networks, while retaining an unknown
  result for genuinely missing or ambiguous links. Blanket reversal to the old
  cycle/market skipping would revive other double-counting problems.
* Add regression cases for the cobalt electricity loop, zinc production loops,
  and mixed regional templates, plus RELICS coverage checks for all primary
  markets. The quantitative acceptance criteria must reflect recycling shares,
  metal content, process losses and allocation conventions.

The 1 kg additions themselves implement a chosen post-allocation attribution
convention. Establishing full physical extraction quantities, including losses
and co-product balances, remains separate from fixing this regression.

Evidence and reproduction
-------------------------

``dev/diagnose_cobalt_zinc.py`` captures fresh inventories immediately before
``Metals.post_allocation_correction`` in private temporary storage. It exports
only relevant graph paths, resource changes, contributions and aggregate scores.
Each system-model directory contains:

* ``graph_traces.json``: relevant paths and the controlled traversal exclusions;
* ``route_changes.csv``: regional resource amounts and weighted score increments;
* ``contributions.csv``: current non-negligible activity contributions;
* ``summary.json``: scores, exact market keys, methods, project, scenario and
  database/code fingerprints.

Both registered resource factors for the simple cobalt/zinc flows are 1.
The four current LCA scores reproduce the all-market audit, and the processed
database fingerprints match that audit. Both pre-correction graph experiments
return None before their exclusions and False afterwards.

With ``PREMISE_KEY`` supplied through the environment and the existing audit
projects available, use a checkout of ``e54ac3e2`` as the implementation to
reproduce these historical graph traces (``/path/to/pre-fix`` below), and run:

.. code-block:: sh

   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
   python dev/diagnose_cobalt_zinc.py --repo /path/to/pre-fix --system cutoff --capture

   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
   python dev/diagnose_cobalt_zinc.py --repo /path/to/pre-fix --system consequential --capture

Omit ``--capture`` to reuse the private pre-correction snapshots. These commands
do not write scenario databases. The diagnostic traversal exclusions are
temporary instrumentation, not changes to the application implementation.
