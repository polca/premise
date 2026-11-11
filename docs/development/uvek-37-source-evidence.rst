Source evidence for pending ecoinvent 3.7 mappings
================================================

The initial evidence snapshot covered 24 pending ecoinvent 3.7 cut-off mappings.
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

Six bounded stock/service decisions now use this evidence through the separate
``uvek_fuels_37_stocks_review.json`` packaged Brightpath review. That review
explicitly accepts functional-reference inference for the six named cases;
it does not treat the unpatched vector as inspected or the releases as equal.
See ``uvek-fuels-37-stocks-verification.json`` for tests and native calculations.
Other candidate witnesses remain subject to their own boundary reviews.

Four further service decisions are recorded in the separate packaged
``uvek_fuels_37_services_review.json``. Compressed air uses complete native
supply-network services, with an explicit patch-market-split assumption and
native leakage-convention differences. Rail, ancillary operation and factory
construction keep their distinct reference units. See
``uvek-fuels-37-services-verification.json`` for component checks; these do not
establish a complete fuel update or export.

Excavation and pipeline-disposal checkpoint
------------------------------------------

The two excavation services retain cubic-metre references and include machinery,
fuel and operating emissions once. Their historical machine/emission assumptions
remain explicit limitations. Pipeline disposal uses factor -1 to convert the
negative waste reference into a positive treatment-service reference. The native
landfill includes waste-specific emissions excluded by the inspected patch source;
this is an explicit screening proxy, not an emissions-equivalent substitution.
Requested 3.7 context and inspected 3.7.1 evidence remain distinct.

The verification receipt ``uvek-fuels-37-earthworks-verification.json`` records
three finite native inventories, 29 focused tests and the complete six-cohort
preflight: 17 mapping cases remain. It does not qualify fuel transformation/export.
