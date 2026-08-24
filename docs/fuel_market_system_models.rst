Fuel-market system-model rules
==============================

Consequential fuel markets
--------------------------

Consequential markets use marginal production mixes calculated in
``premise/marginal_mixes.py``. Technologies registered as constrained suppliers,
including pathways dependent on residual or waste feedstocks, are set to zero before
the remaining marginal shares are normalized.

The constrained fuel pathways are maintained in
``premise/data/consequential/constrained_suppliers.yaml``. They currently include:

* ``bioethanol, from residues``;
* ``biodiesel, from used cooking oil, with CCS``;
* ``methane, from biomass``;
* ``biomass - residual``;
* ``liquefied petroleum gas, synthetic, from coal``; and
* ``liquefied petroleum gas, synthetic, from coal, with CCS``.

The used-cooking-oil biodiesel pathway was previously labelled
``biodiesel, from oil crops, with CCS`` even though its ecoinvent mapping selected
used-cooking-oil inventories. Its IAM variable, lead time, lifetime, and constrained
supplier entry now use the same explicit used-cooking-oil label.

Cutoff fuel markets
-------------------

Cutoff markets may legitimately use a waste-treatment activity as a fuel supplier.
Such activities are identified by an activity name beginning with ``treatment``.
After positive fuel shares have been normalized, the technosphere exchange to the
treatment supplier is written as a negative amount. This follows the ecoinvent waste
exchange convention and prevents the treatment activity from producing an unintended
negative fuel burden.

The sign rule is enabled for generated liquid-fuel, gas, and hydrogen markets. It is
not applied to consequential markets or to non-treatment suppliers.

Hydrogen logistics failure handling
-----------------------------------

Hydrogen demand-node calculation and its audit log are mandatory prerequisites for
creating hydrogen markets. If demand-node calculation, log writing, or hydrogen
market creation fails, the exception is propagated and the fuel update stops. In
particular, sector-specific hydrogen markets are not created and consumers are not
relinked after a logistics or logging failure. This fail-fast behavior prevents a
partially transformed scenario from appearing successful while omitting its intended
hydrogen transport burdens or required diagnostics.

LCIA regression baselines
-------------------------

The deterministic GWP regression scores are refreshed when either rule intentionally
changes generated supply chains. In particular, excluding used-cooking-oil biodiesel
raises the consequential diesel scores because the remaining marginal suppliers are
renormalized, while treatment-exchange sign changes affect cutoff results in versions
where those suppliers are selected. Changes also propagate to tested electricity,
heat, cement, and steel activities that consume the affected fuels upstream.
