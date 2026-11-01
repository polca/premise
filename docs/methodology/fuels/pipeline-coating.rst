:orphan:

Qualified pipeline coating references
=====================================

For ecoinvent **3.12 cutoff only**, the default hydrogen-distribution import
uses two separate, complete hot-dip aluminium-zinc-silicon coating references
from ``lci-pipeline-coating-native-312.xlsx``. These reproduce the approved
CLIC distribution and transmission coating recipes (3652 and 3774), each per
kilogram of retained coating, including the original material losses,
utilities, direct emissions and qualified waste burdens.

The legacy hydrogen-distribution workbook remains authored against **3.7**.
Its distribution and transmission construction activities still purchase
**75.25328256144047 kg** and **876.7114153394484 kg**, respectively. The import
adapter changes only the supplier identity of these two purchases, preserving
their quantities, uncertainty parameters and all other parent exchanges. It
omits the now-unused legacy alloy producer from this modern import view;
the original source workbook is unchanged. The companion is prepared first
against its own 3.12 technosphere and biosphere references. An expanded buyer
graph, changed quantity, incomplete selection or changed guarded recipe fails
before applying the selection.

Older targets and other system models retain the qualified legacy recipe.
The modern coating is not reverse-compiled into a 3.7 recipe: reverse
aggregation loses ingredients and cannot establish equivalent burdens.
No global supplier alias, parent background-version change or native
background modification is introduced. Imported-inventory cache keys include
the parent workbook and, for the selected modern route, the companion and
complete adapter source; native source-database cache keys are unchanged.

These remain **qualified engineering models**, not measured pipe-coating
factory inventories or verified hydrogen-barrier performance. Their original
assumptions include the 55/43.4/1.6 Al/Zn/Si composition, 95% metal retention,
geometry-dependent historical sheet-aluminising utility analogues, cleaning
make-up, protective gas and oxidation scenarios. External nitrogen,
wastewater and contained-residue supplies remain imperfect burden proxies.
The original comments describe gaps in chemistry, emission measurements,
classification, service life and uncertainty. No extra foreground coating
feed, standalone annealing, bath-energy allowance, retirement or recovery
credit is added by this selection. Pipeline geometry, freight, installation
and existing retirement assumptions are unchanged.

The original foreground compilation is CC BY 4.0. The companion retains
attribution, source qualifications, approved CLIC version identifiers and
snapshot digests. Referenced native inventories remain external and retain
their own terms. The default importer does not distribute their LCI bodies.
