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
exception. Vehicle planners declare a stable wrapper identity, representative
source identity, purchased-energy denominator, and fuel-derived flows. After
ordinary factors are composed, the generic ``vehicle-emissions-from-source``
reducer preserves the wrapper's fuel-derived emissions and refreshes its other
emissions from the composed source column exactly once. The declaration does
not name or inspect the emissions sector. On the same REMIND case, 53 such
declarations reproduce the canonical cars-plus-emissions matrices exactly
within tolerance.
