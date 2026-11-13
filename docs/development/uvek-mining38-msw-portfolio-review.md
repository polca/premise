# Mining disposal mapping and next integration gap

Brightpath `8bd600888372a94e21fab8546b4a887f1d3b32a8` replaces the single-incinerator correspondence for the exact
ecoinvent 3.8 cut-off European municipal-waste market with a reviewed portfolio.
The dependency pin and colleague installation instructions now include it.

All 30 terminal shares, nine distinct uncontrolled treatments, 35 collection
freight entries, unsanitary-landfill operating inputs and recovered iron/copper
cut-off handovers remain. No share normalization or exchange omission is added.
The source waste convention is reversed once at the caller. The public-model
calculation evidence remains separate from whole-native-process screening proxies
and their warnings. See [Brightpath's detailed review](https://github.com/romainsacchi/brightpath/blob/8bd600888372a94e21fab8546b4a887f1d3b32a8/docs/uvek-msw-portfolio.md).

The exact-profile ledger now has 1,244 valid scopes. Focused migration,
manifest/archive and fresh/cached CLIC tests pass. All twelve helpers have finite
positive and negative LCIs against the actual UVEK 2026 background and 3.10
biosphere; the source database was not modified. A built wheel contains the exact
review, migration JSON and manifest. The adjacent receipt gives counts and scope.
The broader existing approval matrix reached 100 passing tests before being
intentionally interrupted; it is not claimed as a complete suite run.

The real encrypted REMIND SSP2-NPi 2050 mining diagnostic now completes inventory
preparation and the shared sector update. It still fails exhaustive graph
validation and has not been exported or added to the public supported-sector gate.

The reported 4,156-node component has the same members as its baseline after
resolving 232 electricity product-label differences. Mining currently defaults to
the ecoinvent family while the baseline uses UVEK's canonical aliases. A private
copy normalized with the existing alias function reproduces all 57 baseline
cycle signatures exactly, without changing amounts, uncertainty or supplier keys.
This proves the label inconsistency; it is not a runtime fix or a suppression.

A separate coverage gap remains: the result has 36 regional clones of the three
imported negative-reference treatment processes, but no sulfidic-tailings market.
Native positive-reference disposal and the impoundment role need an explicit
mapping/filter audit. Merely returning from the update is insufficient.

Next, connect mining to the existing family/biosphere handoff and extend reviewed
YAML aliases and any necessary native market/fate bindings. Preserve the shared
Premise algorithm and handle production-reference signs explicitly. A small
Python handoff adaptation may be necessary because YAML cannot change the family
argument currently omitted by the caller; document and test any such change.
Do not suppress validation, silently skip native waste consumers, or open the
public gate before real workflow/export and coverage checks pass. The remaining
sector-integration goal stays active.
