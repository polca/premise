# Sodium fluoride and its inspected reagent delivery

The two ecoinvent 3.9 cut-off sodium-fluoride identities no longer select sodium
arsenide. They now use an explicit per-kg reaction helper and a delivered-product
market. A third reviewed rule covers the RoW hydrogen-fluoride market needed by
the reaction. Only these three exact profiles gain new permission.

The reaction helper retains inspected HF and active NaOH demand, electricity,
useful natural-gas heat, chemical-plant infrastructure, five direct biosphere
exchanges and their supported uncertainty. The source's assumed 95% yield is
already represented by its coefficients; no second yield or stoichiometric
correction is applied. Separate reagent releases, Water to air and water, and
cooling-water withdrawal retain their signs, units and compartments. The useful
heat uses the existing reviewed global 3.9 CHP/boiler mix, rather than fuel MJ.
The delivered NaF market retains its unit producer input and all four freight
entries. There is no arsenide, new recovery credit or physical omission.

The native NaOH label does not call for a factor of two. [Ecoinvent's methodology
guide, page 60](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Fundamentals/dataqualityguideline_ecoinvent_3_20130506.pdf#page=68)
explains the active-substance basis and the equivalent ambiguous legacy label.
The reaction's internal caustic-soda helper retains all seven source producer
shares, selecting the corresponding native diaphragm, mercury or membrane cell,
and retains all four delivery entries. It does not substitute UVEK's historical
average technology split. Native regional/vintage/allocation differences remain
explicit proxies; no extra chlorine/hydrogen credit or solvent burden is added.
The existing general NaOH and electricity parent rules and approvals are unchanged.

The RoW HF market retains five producer/co-product entries and eight freight
entries. The four allocated HF co-products, about 1.15% of supply, use complete
native fluorspar-route HF as an explicit production-route/allocation proxy. Their
old allocated production burdens are not reconstructed. The previous native-HF
assay/CAS warning remains. Chinese generic rail uses complete generic RER rail,
with a geography/fleet warning, rather than coal freight. Original freight
quantities remain separate; no ship transport or distance correction is invented.

Signed caller quantities, formulas, supported uncertainty and separate foreground
emissions are preserved. Native suppliers replace upstream source inventories,
so technology, geography, allocation, vintage, internal uncertainty and excluded
native biosphere content differ. Score effects remain unquantified. No LCI, LCIA
or stochastic equivalence is claimed.

All 29 Brightpath mapping/storage tests pass. Premise's nine new fresh/cache and
individual-refusal tests pass, along with nine existing review/drift checks.
The installed-wheel check also passes ten fresh/cache/refusal and prior-scope
cases. The unused 43,878,700-byte wheel copy was removed after installation and
verification. Running notebook kernels need restarting to load the changed data.
The transient graph has three consumers and six helpers, 46 native supplier
bindings and six foreground biosphere definitions. Six ordinary signed LCIs are
finite and reverse correctly. Three independently flattened native-demand LCIs
agree after iterative numerical refinement; maximum absolute difference is
4.55e-13 at unchanged rtol=1e-8/atol=1e-11. Initial independent LU factorizations
differed by up to 1.85e-7; the unchanged-matrix residual correction is recorded
rather than presented as bitwise agreement or a mapping change. No Brightway
database, activity or processed file was created, and source/biosphere metadata
and registered database names remain unchanged.

The real battery retry clears exactly these three rules, with no newly unapproved
rule: 188 becomes 185 in this inspected 3.9 cohort. Battery preparation/update/
export remain unfinished; eight of 19 sectors remain qualified. The next reported
case is the RER hydrated-lime market. No runtime Python module changed. The
maintainer replay, mapping resources, tests and approvals await the next batch
publication. See [verification](uvek-battery-fluoride-39-verification.json).
