Source evidence for pending ecoinvent 3.7 mappings
================================================

The fuel-preparation queue contains 24 pending ecoinvent 3.7 cut-off mappings.
A local licensed 3.7.1 ecoSpold2 archive supplies candidate reference evidence.
All 24 requested name/product/location identities match uniquely; their product
UUIDs agree with the packaged 3.7 catalog, while dataset activity UUIDs differ.
Unit labels agree after spelling normalization, with no numeric conversion.
The inspected patch has twenty positive-one and four negative-one references.
These observations are recorded in ``uvek-37-patch-reference-evidence.json``.

The `official change report, section 2.1
<https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/change%20report_v3%207%201_20201217.pdf>`_
describes corrections to iron sinter, zinc mining and ammonium paratungstate
production. Its discussion of downstream results prevents a blanket claim of
inventory or score equivalence between the releases. Matching product identity
alone is also insufficient evidence for such a claim.

This artifact grants no mapping approval and does not relabel the requested
3.7 source as 3.7.1. Each case still needs a justified use of patch evidence,
source/native functional-boundary review, physical reference conditions,
material grade or service scope, signs, quantity factors and helper inspection.
Normal strict refusals remain in force until the exact approvals, tests and
native calculations pass. Any reliance on patch evidence and its limits must
appear in the eventual decision log.

Raw licensed XML and vectors remain outside the repository. This step changes
no runtime modules, migration rules, source inventories or exported databases.
Full fuel transformation/export and the remaining sector updates are unfinished.
