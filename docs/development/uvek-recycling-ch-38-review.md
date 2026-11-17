# Complete Swiss recycling proxies for the required truck inputs

The two remaining ecoinvent 3.8 recycled-plastic inputs now convert through the
normal Brightpath and Premise inventory path. This finishes that input group;
the truck sector itself remains unfinished. The actual truck preparation now
reaches an ecoinvent 3.12 group with 27 unapproved inputs. Fourteen of nineteen
sectors remain qualified.

The new mappings use the complete Swiss mechanical-recycling processes for
recycled HDPE and amorphous PET. The original markets retain their producer
shares and finished-product transport, including all four regional recycled-PET
branches. Swiss operating data replace the other regions' recycling yields,
energy, water, chemicals, emissions and residues. This is an explicit geographic
and operating-data proxy, with unquantified score differences. It preserves
recycled content and paid recycling; it does not substitute virgin resin or an
empty recycling handover.

The closed model retains collection, sorting, washing, container manufacture,
infrastructure, processing and independent emissions. Thirty new helper recipes
reuse existing reviewed complete background models. Twelve utility decisions
apply only inside these two parents. Four new zero-allocation authorities are
needed by Brightpath's existing schema; they do not gain separate Premise import
permissions. Three existing zero-stock recipes are reused unchanged.

The important boundaries are:

- Swiss waste treatment retains incineration, open burning and wet-class open
  dumping separately, together with original collection and freight. Native
  incineration approximates fly-ash extraction, with warnings and no invented
  metal-recovery credit. Negative treatment references change sign once.
- Three heat portfolios retain every producer quantity, including tiny entries.
  Gas cogeneration remains cogeneration. The non-gas industrial mix retains its
  94.295% recovered municipal-waste heat. Solar supply excludes auxiliary heat.
  Native size, allocation, performance and vintage differences remain warnings.
- Five source recycling stocks contain only production exchanges. Two waste-wood
  heat outputs have 41 explicitly zero exchanges each. These verified zero
  allocations remain explicit; nonempty recycling and disposal are not zeroed.
- Historical Swiss wastewater and mixed-plastic helpers retain their original
  source-version audit contexts. A reused HDPE helper also retains four tiny
  recycled-supply omissions previously approved in
  `uvek_packaging_hdpe_review.json`. No new omission or renormalization is added.
- Two container formulas retain parameter values independently recovered from
  their purchased-material amounts. Their signed nominal results are checked.

Twenty Brightpath tests and five Premise tests pass, covering signs and
uncertainty, both native releases, paid recycling, treatment fates, heat sources,
stock authority, formulas, cache warnings and permission withdrawal. The normal
generator reproduces all three resources; both canonical biosphere files remain
unchanged. All six older-PET regressions pass. A broader approval sweep was stopped after
98 passing tests; its unrelated remaining cases were not completed. Eighteen
installed-package checks pass without loading checkout runtime modules. The
verification artifact records that scope explicitly.

Twenty signed LCI solves cover both migrated parents, both recycling producers
and six heat/waste portfolios. Ten comparisons with the independently prepared
source-component graph pass. Flattened native and direct-biosphere coefficients
agree at relative tolerance 1e-12 and absolute tolerance 1e-15. The largest
relative inventory difference is about 1.063e-9; the sparse-solve comparison uses
1e-8 tolerance. This proves finite native-model consistency, not source/native
impact equivalence. No Brightway database, registered activity or processed file
was created; source and biosphere metadata are unchanged.

The next truck group contains 13 material/manufacturing interfaces, six lorry
maintenance/end-of-life services, three wear-emission treatments, three waste
markets, one sea-freight service and one antimony-mining process. These 27 cases
are the next inspected group, not a count of every remaining sector mapping.
The public truck admission gate has not been widened. Batteries, final energy,
emissions and metals also remain unfinished. Production and generator Python
modules are unchanged in this batch.
