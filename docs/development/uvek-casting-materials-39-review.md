# Required casting materials, source 3.9

Thirty-four inspected material markets used by the required aluminium-casting
process now retain every original producer entry and final-delivery leg, its
quantity and marginal uncertainty. Complete native production replaces source
production internals. Regional supply shares are retained as quantities;
they do not preserve the former regional technology or allocated burdens.

The maps retain separate steam-reforming/partial-oxidation ammonia, all three
AE7 feedstocks, chloride/sulfate titanium dioxide and virgin/recovered silicon
carbide. The RAF Fischer–Tropsch ammonia coproduct uses native partial-oxidation
ammonia as an explicit production proxy; its coal feedstock and allocation are
replaced. MEK coproduct branches use complete native MEK with explicit route
and allocation warnings. No synthetic-fuel inventory or recovery credit is
added. Recovered silicon carbide uses a nonempty native recycling process.

Source and native nitric acid both explicitly reference one kilogram of pure
HNO3, despite the native 50%-solution label: factor +1, without adding water
or doubling the amount. Native zircon is zirconium-silicate mineral, not
zirconium oxide or contained zirconium; no assay multiplier is introduced.
Native low-alloy steel includes hot rolling, so it is not added again.
Non-metallurgical alumina grade, European primary aluminium substituted for
North-American supply, water-treatment technology, mining grades, freight
fleets and other geography/vintage/allocation differences remain explicit
screening limitations. Their score effects are unquantified.

Sixty Brightpath tests and thirteen Premise tests pass, covering these maps,
municipal waste, older truck mappings, signed uncertainty, caching and approval
withdrawal. Six earlier approved source scopes retain their physical mappings;
all existing recipe contents remain unchanged. The normal generator reproduces
all three resources, with canonical biosphere files unchanged. No production
or generator Python module is modified, and no source exchange is omitted.

Sixty-eight signed finite LCIs and 34 independent original-quantity/native-demand
comparisons pass without database writes. Demand coefficients agree exactly.
An ordinary sparse solve initially missed the 1e-10 comparison tolerance for
one nitric-acid case (1.518e-10 difference). Three residual-correction steps in
the private verifier reduce the maximum matrix residual below 1.725e-15; all
comparisons then pass the original tolerance. Model coefficients, runtime
solver and tolerance are unchanged. This verifies encoded-model consistency,
not source/native impact or stochastic equivalence.

The actual casting-producer retry reduces its unapproved operation rules from
52 to 14: 34 material rules are solved here and four waste rules were solved
in the preceding batch. Wax, sodium nitrite, disposal, heat, recovered-stock
permission and Quebec electricity remain. This is an inspected dependency
group, not a complete count of the remaining project. The whole casting model,
battery update/export and four remaining sector qualifications are unfinished.
