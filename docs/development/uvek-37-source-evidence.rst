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

Hot-rolled steel checkpoint
---------------------------

Three further complete steel supplies use the native kg reference without a
second rolling operation: 18/8 hot-rolled chromium steel, low-alloyed hot-rolled
steel and reinforcing steel. Both native reinforcing-steel production routes
were inspected and already include rolling. Historical route mixes are proxies;
these mappings do not certify a particular mechanical grade or strength class.
The review retains the distinction between requested 3.7 and inspected 3.7.1.
Focused checks cover signed quantities, uncertainty, formulas, fresh/cached
harmonization and refusal when the exact review is withdrawn.

Lubricant and helicopter-cycle checkpoint
----------------------------------------

The exact 3.7 profiles now use one kg of native liquid lubricant and one native
landing/take-off cycle, respectively. No lubricant-yield, aircraft-mass,
fuel-normalisation or cycles-per-hour multiplier is added. Native lubricant
emissions are incomplete; the helicopter has a different fuel requirement and
inconsistent aircraft descriptions. These remain explicit screening limitations.
Four older selected profiles and every helper inventory are preserved.

The verification receipt ``uvek-fuels-37-oils-lto-verification.json`` records
25 focused tests, two finite native inventories, exact resource replay and wheel
checks. Fuel preparation has 12 remaining mapping cases. Fuel transformation and
export are still unfinished; this review does not approve the waste-oil market.

Separate 3.11 source checkpoint
------------------------------

All four remaining 3.11 cases were inspected directly in the installed licensed
3.11 cut-off database, not inferred from 3.7.1. The AlMg3 and hot-rolled 18/8 steel
mappings retain their kg and processing bases. Hydrogen uses an explicit native
SMR/25-bar screening proxy for the source low-pressure supplier mix; nickel uses
an explicit historical 99.5%-labelled production proxy, without claiming purity
certification or adding invented refining. No pressure, purity or rolling factor
is introduced. Earlier hydrogen/nickel helpers and their supplier links remain
unchanged.

See ``uvek-fuels-311-remaining-verification.json`` for 42 focused passing checks,
four finite native inventories, unchanged licensed source stamps and exact
resource/wheel verification. All 3.11 migration-preparation cases now pass, leaving
eight cases in other cohorts; full fuel transformation/export is not qualified.

Cast steel, scrap steel and mixed-plastic markets
------------------------------------------------

The inspected patch cast-steel market has approximately 24.1% converter and
75.9% electric steel. Its new scoped recipe retains those shares, four freight
quantities and their marginal uncertainty; it does not reuse the later releases'
74.2%/25.8% split. Existing cast-steel recipes remain unchanged.

The scrap market retains landfill/incineration shares and freight. A separate
scoped recipe binds its recovered-iron handover to the existing verified 3.7
positive-stock profile. The inspected patch stock has a positive kg reference
and no nonzero exchanges or formulas; this is an allocated handover, not a claim
that physical waste treatment is burden-free. The mixed-plastic market retains
all three fates, including its reviewed open-burning proxy, at the inspected
shares. The patch-evidence restriction applies to each requested 3.7 case.

``uvek-fuels-37-market-profiles-verification.json`` records 35 focused passing
checks, three finite weighted native inventories, exact regeneration and wheel
checks. Five fuel mapping cases remain; full transformation/export is unfinished.

Swiss sand delivery and helicopter hours
---------------------------------------

The 3.7 sand helper retains one kg of allocated Swiss sand and the inspected
0.00372 rail/0.02026 road tonne-km delivery legs with their marginal uncertainty.
The earlier approved 3.9 at-mine supplier and factor remain unchanged. Its prior
physical inspection is pinned; other uninspected sand versions are refused.

The flight-hour case extends the explicitly reviewed native single-engine
without-LTO proxy, retaining one hour rather than scaling by fuel. Source patch
fuel is 26.4 kg/hour and native fuel 182.6 kg/hour; the earlier 3.6 review documents
the same discrepancy. Fleet, purpose and documentation limitations stay explicit.
No LTO, fuel-ratio or year/hour multiplier is added, and the existing helper is
unchanged. Requested 3.7 and inspected 3.7.1 evidence remain distinct.

``uvek-fuels-37-sand-hour-verification.json`` records 24 focused passing checks,
two finite native inventories and exact regeneration/wheel checks. Three fuel
mapping cases remain; full fuel transformation/export is still unfinished.

Allocated waste-oil market
-------------------------

The inspected kiln-receipt branch has 57 zero nominal nonreference exchanges,
all with zero uncertainty bounds. Its inherited formulas also occur in the
nonzero clinker activity; they are not transferred into this allocated waste
receipt. A literal source-input omission now records the 0.204833837097889 kg
branch, its sign, index and inspected patch witness. This is a fixed-allocation
screening boundary, not a claim that physical kiln emissions vanish. The source
context remains 3.7; the witness hash identifies inspected 3.7.1 evidence only.

Both incineration shares and all road/rail freight remain separate and unchanged,
without renormalisation or invented energy credits. Factor -1 converts the
negative waste reference into a positive treatment-service reference. The exact
omission requires its own authorization inside the matched rule review: removing
or altering that authorization is refused even after priming the cache.

``uvek-fuels-37-waste-oil-verification.json`` records 16 focused passing checks,
a finite weighted native inventory and exact resource/wheel checks. The complete
fuel preflight now has only two packaging-market mapping cases. Bare-3.7 source
vectors were not inspected; full fuel transformation/export remains unfinished.
