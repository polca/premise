Declarative sector composition
==============================

The declarative composition project separates sector planning from inventory
mutation. A sector planner reads one immutable latent reference and emits a
versioned ``SectorPatch``. It must never inspect an amount, activity, or link
created or changed by another selected sector.

Contract
--------

Each patch identifies its model, pathway, year, sector, and immutable reference
digest. Sparse declarations cover:

* activity activation or deactivation;
* A and B multiplication factors relative to immutable reference amounts;
* provider identities and metadata values; and
* typed shared operations such as market normalization.

Declaration order is canonicalized before hashing. Generating a sector alone or
as part of a larger selection must therefore produce the same patch digest.

Composition
-----------

The composer sorts patches by sector and applies generic merge rules. Numeric
factors multiply. Repeated identical activation, provider, and metadata
proposals are idempotent. Incompatible proposals fail with ``PatchConflict``;
they must not be hidden behind execution order or pair-specific corrections.

Shared operations retain invariant sector declarations in canonical order for
an operator-specific reducer. Reducers belong to the global composition layer,
not to another sector's planner, and must be associative and commutative.

Migration rule
--------------

The existing sequential update path remains the compatibility oracle during
migration. A sector is migrated only when its patch is context invariant, its
composed inventory matches the canonical sequential output, and the runtime and
memory gates remain satisfied. The production format must not contain
prerequisite-gated pair corrections.

The factorization sidecar publishes a versioned ``sector_migration`` registry
for the 19 sector updates (the ``external`` hook is deliberately excluded).
It distinguishes the native emissions ``SectorPatch`` planner from sectors
currently represented by immutable-singleton ``AtomicExchangePatch`` diffs.
This is capability metadata, not a certification claim: a sector remains
uncertified until its singleton, interaction, graph, matrix, LCIA and
performance gates pass.

Validation strategy
-------------------

Certification uses structural coverage instead of running every unordered
sector pair through the expensive sequential oracle. It must include:

* all 19 singleton sectors against the sequential Premise oracle;
* the all-sector selection and every all-minus-one selection;
* every edge in the measured read/write and coordinate-overlap graph, in both
  caller orders;
* overlap-connected triples and a deterministic strength-two/strength-three
  covering array for larger selections; and
* deterministic randomized larger subsets as a final safeguard.

Disjoint pairs are covered by patch-context invariance plus property tests of
the generic composer. Those tests exhaustively exercise commutativity,
associativity, activation idempotence, duplicate-provider handling,
normalization, and conflict rejection without rerunning Premise. A disjoint
pair is promoted to an oracle case whenever instrumentation discovers a shared
read, write, provider, activation, or reducer declaration. The all-sector case
alone is never considered sufficient because independent errors can cancel.

Emissions pilot
---------------

The first migration pilot plans REMIND GAINS factors against the complete
latent activity universe rather than the activities present when the emissions
step happens to run. The planner freezes IAM arrays, geography mappings, and
GAINS activity classification before emitting its patch. It does not receive a
selected-sector list.

For REMIND SSP2-NPi 2050, composing this invariant emissions patch with the
electricity patch reproduces the canonical direct Premise A and B matrices:
the coordinate sets are identical and no values differ outside the configured
``1e-12`` relative and ``1e-15`` absolute tolerances. This pilot replaces the
electricity/emissions interaction for matrix values; it is not certification of
the other 18 sectors or of the final performance gates.

Vehicle emissions are a genuine shared operation rather than a pair-specific
exception. The emissions planner declares a stable wrapper identity,
representative source identity, purchased-energy denominator, and fuel-derived
flows from the immutable latent reference. After ordinary factors are composed,
the generic ``vehicle-emissions-from-source`` reducer preserves the wrapper's
fuel-derived emissions and refreshes its other emissions from the composed
source column exactly once. The declaration does not name or inspect any
selected transport sector. On the same REMIND case, one unchanged emissions
patch containing 53 declarations reproduces the canonical cars, buses, trucks,
trains, ships, and two-wheelers pair matrices with no coordinate or value
mismatch.

Provider and column ownership
-----------------------------

A sector that removes an activity from the semantic provider index declares
the stable provider identity through ``relink-unavailable-providers``. The
global resolver selects an active provider with the same product semantics for
every composed consumer, including latent consumers activated by other
sectors. The declaring planner never reads those consumers.

A sector that empties an original activity also declares its resulting
technosphere column as authoritative. This prevents latent additions from a
different patch from restoring inputs that the owning sector deliberately
removed. These two generic rules reproduce the canonical REMIND SSP2-NPi 2050
cement-plus-steel graph and A/B matrices without a pair-specific correction.
