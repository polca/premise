Refined-metal RELICS extraction audit, 19 October 2026
====================================================

Most covered primary producers approach one kg extracted per kg output, but
there are substantial attribution gaps in several routes.

Results
-------

* 128 individual producers across 33 elements, with regional variants retained.
* 104/128 producers (81.25%) are within +/-5% of one kg/kg.
* 107/128 producers (83.59%) are within +/-10%.
* All producers of 23/33 elements are within +/-10%.
* Gold: 0.964396--1.004825 kg/kg across 15 producers.
* Zinc: 1.000095--1.002751 kg/kg across three producers.
* Platinum, palladium, rhodium, iridium and ruthenium: approximately 1 kg/kg.
* Copper electrorefining: 0.904622 kg/kg. Other copper routes range from
  0.013537 for the zinc-refining co-product to 1.027740 for SX-EW copper.

Important departures
--------------------

Zinc-refining co-products
~~~~~~~~~~~~~~~~~~~~~~~~

``primary zinc production from concentrate``, RoW, yields:

* copper cathode: 0.0135374 kg Cu/kg output;
* lead: 0.000161679 kg Pb/kg output;
* silver: 0.000170576 kg Ag/kg output.

The current ``target_resource_is_supplied_upstream`` check detects the presence
of matching metal inputs, even when these cannot explain the final product.
The sum of positive kilogram inputs matching each target is respectively
0.000245137, 0.0131081 and 0.000167071 kg/kg output. These are upper bounds
on contained metal before accounting for composition. The missing-resource
attribution pass therefore needs a quantitative primary-feed boundary rather
than an existence-only decision. The zinc output already has a special
boundary; its other metal co-products do not receive the corresponding
restoration in this audit.

Lithium, arsenic and silicon
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Elemental lithium and the lithium-metal battery product score approximately
  1.8e-11 kg/kg. Their producers are absent from the mining-share attribution
  mapping; target-bearing upstream traversal is unresolved. This is an
  attribution/coverage gap, not a physical extraction result.
* ``7N arsenic production`` scores 2.55439e-7 kg/kg. Its product is recognized
  as elemental arsenic, but this producer has no selected primary-attribution
  route and no matching upstream extraction boundary.
* Metallurgical silicon scores 0.00152508--0.00305024 kg/kg. The correction's
  pure-product qualifier set lacks ``metallurgical``. RELICS also has only the
  elemental Silicon resource mapping, not a conversion of the bulk silica
  or sand extraction feeding silicon production. The low result therefore
  reflects incomplete attribution/characterization coverage.

Copper-concentrate fallback
~~~~~~~~~~~~~~~~~~~~~~~~~~

Copper concentrates without an explicit factor retain their source copper
resource amount. This can retain economic allocation effects: the zinc-mine
copper-concentrate co-product retains about 0.0121079 kg direct Cu resource
per kg concentrate, and cobalt-production concentrate about 0.0956595 kg/kg.
Thus this fallback is not a validated physical grade or recovery correction.
The main copper electrorefining route remains below one at 0.904622 kg/kg.

Above-one results
~~~~~~~~~~~~~~~~~

``7N gallium production`` consumes 1.428571 kg high-grade gallium per kg
output and scores 1.428571 kg/kg. This is consistent with its input/yield
assumption. Aluminium (0.895914--1.138329), primary iron outputs
(1.112912--1.284663), and nickel (1.000761--1.190698) largely inherit their
departures from source supply chains. These results should not automatically
be forced to one: actual losses, source allocation, other inputs and product
purity must be distinguished.

Scope and interpretation
------------------------

The source is ecoinvent 3.12 cut-off in Brightway project
``metals-correctness-3.12-cutoff``. Fresh source cleaning and default inventory
imports were followed by the current ``Metals.post_allocation_correction``
through premise's normal update, validation and export machinery. This changed
160 datasets. The written database is
``metals-refined-relics-20261019-corrected``.

Only the resource correction was applied. A full ``metals`` update failed on
an unrelated missing material-intensity conversion for
``electric motor production, asynchronous, traction``. This audit excludes
material overlays and metal-market remapping; it is not a complete updated
scenario. Encrypted IMAGE SSP2-M input for 2020 was loaded explicitly; no
unencrypted fallback was used. The correction itself has no year parameter.

Functional units are one kg from individual production datasets. Markets,
secondary outputs, concentrates, compounds, alloys, anodes and unrefined gold
are excluded. Primary by-product routes are included. Pig iron and sponge
iron are retained as primary metal outputs without assuming pure Fe content.
Silicon and arsenic are included as refined elemental products. Ordinary
upstream market mixes remain; excluding a market as the functional unit does
not remove every recycled input from upstream supply chains.

The numerator is the full supply-chain RELICS indicator for the target
element, not a sum of all metals. A value of one is a screening benchmark for
the declared resource-attribution convention. Many exchanges are directly
set to one by that convention; agreement is not independent evidence of
correct physical extraction, ore grades or recovery losses.

Verification
------------

* RELICS 0.0.6 mappings were matched to the selected ecoinvent biosphere by
  name and compartment; coverage and the mapping hash are recorded.
* Source comparison covers 111 matching original ecoinvent producers;
  imported foreground producers can lack an original counterpart.
* Export verification confirmed that gold-from-anode-slime retained its
  corrected one-kg direct Gold resource exchange in the written database.
* All 128 reference production exchanges were independently verified as
  one kg; all 128 scores were repeated with ordinary Brightway LCIA.
* Maximum score discrepancy was 1.33e-15 kg/kg. The largest absolute
  technosphere solve residual was 2.33e-10, despite the solver's
  ill-conditioning warning. All results were finite.
* Numerical checks establish calculation consistency, not methodological
  correctness of the attribution rules.

Files and reproduction
----------------------

``report.html`` is a standalone report with plot, element summaries, search,
element filtering and an outlier filter. The plot is also available as PNG
and PDF. ``scores.csv`` records individual own-element results, direct and
upstream contributions and source comparisons. ``all_indicators.csv``
contains all RELICS indicators for each selected producer.
``outlier_contributions.csv`` gives the six largest activity contributions
for departures above 5%. ``attribution_decisions.csv`` traces selected
outlier decisions. ``numerical_verification.csv`` and
``lcia_cross_checks.csv`` record calculation checks. ``metadata.json``
records database fingerprints, code and mapping identities, scope and units.

Run with the premise environment and ``PREMISE_KEY`` set::

    python dev/audit_refined_metals_relics.py \
      --project metals-correctness-3.12-cutoff \
      --database UNIQUE_NEW_AUDIT_NAME --correction-only \
      --output results/NEW_OUTPUT_DIRECTORY
    python dev/diagnose_refined_metals_relics.py results/NEW_OUTPUT_DIRECTORY
    python dev/report_refined_metals_relics.py results/NEW_OUTPUT_DIRECTORY

The build refuses to overwrite an existing database. ``--reuse-build`` can
repeat scoring against an existing audited build with matching metals code.
Source databases and the production transformation code were not edited.
