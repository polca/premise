# Dolomite supply — 16 November 2026

The current battery import requires the source 3.9 global-rest-of-world dolomite
market. Its source 3.8 counterpart was already reviewed, but that does not grant
permission to use it for a different source version.

The new helper keeps the source 3.9 one-kg producer entry and all nine delivery
entries separately. It retains their quantities and supported uncertainty.
Five rail-region entries, one barge entry and three lorry entries remain visible;
they are not combined or renormalized. The complete native raw dolomite process
replaces production. Its European, limestone-based mining and milling model is a
screening proxy. Production electricity, geology, site water, allocation,
technology, vintage and emissions coverage differ. Effects on scores are
unquantified. This is uncalcined dolomite; no lime, calcination emission or extra
yield factor is introduced.

The exact source 3.8 profile, factor and helper remain unchanged. Its approval
fingerprint must be refreshed because the fingerprint includes the entire rule,
including the added 3.9 profile. Withdrawing the new 3.9 decision still permits
the separately approved 3.8 conversion. The superseded shared review card is
retained in the existing historical archive. The only maintainer Python change
is that archive's checksum constant. Runtime Python and Premise's shared
transformation logic are unchanged.

The generator reproduces all three resources byte for byte; both biosphere
resources are unchanged. Fifty-five Brightpath checks and eight Premise checks
pass, including existing shipping and manganese regressions. Six signed
inventory calculations are finite, and the delivered inventory equals the
independently converted raw source market with zero observed difference.
No Brightway database or activity is written.

The actual battery preparation retry reduces its first source-version cohort
from nine to eight pending rules. This is not completion of the battery sector;
14 of the 19 sectors remain qualified. See the
[verification record](uvek-dolomite-39-verification.json).

The installable Premise and Brightpath wheels pass another 34 checks covering
both new decisions, permissions, cache replay and resource integrity. Imported
modules come from the isolated installed environment; packaged mapping bytes
and Premise's pinned dependency match the committed checkouts. Superseded test
wheels, temporary generated copies and the isolated build directory were cleaned
after checking their ownership and, where applicable, checksums.
