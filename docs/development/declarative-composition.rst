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
