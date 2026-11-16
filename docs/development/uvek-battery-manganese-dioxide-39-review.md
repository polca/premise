# Manganese-dioxide supply — 16 November 2026

The required sodium-ion battery inventory buys manganese dioxide. Its previous
candidate was lithium manganese oxide, a different material. This decision
retains the source supply structure and uses native UVEK component suppliers.
It applies only to the inspected ecoinvent 3.9 cut-off inputs.

The delivery helper retains both original producer shares and all four freight
entries. The main producer retains its 13 technology inputs and nine elementary
flows. Its manganese(III)-oxide precursor now uses the specifically named native
Mn2O3 supplier, rather than lithium manganese oxide. Native precursor production
uses Chinese carbonate roasting; route, grade, geography, vintage, allocation
and poorly documented emissions remain screening limitations. No extra yield
factor, sulphate credit or invented battery-grade purification is added.

The small recovered share remains a separate positive stock input. The source
stock has only its production exchange; the associated waste-treatment output
has 22 nonproduction exchanges. The empty stock therefore does not receive a
second treatment burden or an avoided-primary-production credit. This does not
mean that physical battery recycling has no impacts.

Three rules and three helper recipes change. Sixteen component-rule scopes and
14 borrowed recipes are checked against their reviewed fingerprints/checksums.
All previous recipes, other rules and both biosphere resources remain unchanged.
There are no new foreground omissions. Caller signs, quantities and marginal
uncertainty are retained; this does not establish cross-exchange correlations.

The complete model passes 25 Brightpath checks and four Premise fresh/cache and
approval-withdrawal checks. Six signed inventory calculations are finite. The
delivered inventory equals an independently converted raw producer plus the
original delivery inputs, with zero observed inventory difference. These checks
use transient matrix additions and create no Brightway databases or activities.

The normal generator reproduces all three resource files byte for byte. A small
maintainer-only loader and six-line generator hook are needed because these
reviews depend on mappings created late in generation. Further cases can extend
the index and review files. Runtime Python, shared Premise logic, notebook usage
and the supported system-model boundary are unchanged.

Actual import retries reduce battery's first source-version cohort from ten to
nine pending rules, and final energy's from 19 to 18. Both updates still stop on
other required correspondences. No additional sector or complete battery export
is qualified by this change; the previously verified count remains 14 of 19.
See the [verification record](uvek-battery-manganese-dioxide-39-verification.json).
