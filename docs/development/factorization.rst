Factorization sidecar
=====================

``NewDatabase(..., factorization_mode=True)`` collects deterministic activity
identities and technosphere supplier links for matrix-factor consumers. The
feature is opt-in and its metadata are kept on the scenario, outside activity
and exchange dictionaries. Standard database writers therefore receive the
same inventory payload as a build with factorization disabled.

After ``update``, consumers can retrieve a deep copy of the sidecar::

    ndb = NewDatabase(..., factorization_mode=True)
    ndb.update(["electricity", "emissions"], persist=False)
    metadata = ndb.factorization_metadata()

``metadata["identities"]`` maps each current activity code to a stable UUID.
Source activities derive their UUID from their fixed database and code. Compact
baseline activities which have not yet received a storage code use their public
semantic role and a deterministic occurrence number; exchange amounts are not
part of identity. Activities created by Premise derive their UUID from the
creating sector, their public semantic role, and a deterministic occurrence
number. The ``provider_identities`` mapping records the stable provider only
for implicit technosphere exchanges whose public metadata match more than one
activity. Explicit Brightway input keys need no sidecar disambiguation.

Compatibility contract
----------------------

Factorization metadata must not change activity order, activity or exchange
fields, amounts, supplier-selection behavior, exports, A or B matrices, or
LCIA results. Differential tests must compare the complete materialized graph
with factorization disabled and enabled. A factorization build is invalid if
the graph differs, even when its LCIA score happens to agree.

The sidecar schema is internal to Premise 2.6.0 and intended for coordinated
use by ``db-assembler``. Callers should reject unknown ``schema_version``
values. ``factorization_identities()`` remains a convenience method when only
the activity-code mapping is needed.

In-memory sparse handoff
------------------------

Consumers that immediately build matrix records should use the compact-store
handoff instead of materializing a nested inventory::

    ndb = NewDatabase(
        ...,
        inventory_backend="compact",
        factorization_mode=True,
    )
    ndb.update(["electricity"], persist=False)
    exported = ndb.export_sparse_inventory()

    for activity in exported.iter_activities():
        consume_activity(activity.storage_id, activity.payload)
    for exchange in exported.iter_exchanges():
        consume_exchange(
            exchange.output_activity_id,
            exchange.ordinal,
            exchange.payload,
        )

``export_sparse_inventory`` is an in-memory, read-only view over the compact
store. It neither calls ``materialize_inventory`` nor writes CSV files. The
activity and exchange streams are re-iterable and retain dense store IDs so a
consumer can construct integer matrix coordinates directly. Payload mappings
are borrowed for the lifetime of the view and must not be mutated.

The handoff includes the finalized factorization sidecar. Schema 17 records
derived exchange lineages separately from provisional exchange UUIDs. A
consumer derives the final UUID after activity identities have been reconciled;
this prevents a pre-reconciliation UUID from depending on update context. The
activity-only reconciliation does not rescan all exchanges.

Performance contract
--------------------

Normal builds retain the default ``factorization_mode=False``. Factorized
builds register source keys in one linear pass and collect additions only at
existing inventory insertion boundaries, including compact-store transactions.
Keyless baseline identities never hash complete exchange payloads. On a warm
cache, the median complete update runtime should remain within 20 percent of
the standard path. Record the source database, scenario, selected sectors,
backend, cache state and at least three runs when checking this threshold.

For REMIND SSP2-NPi 2050 final energy, a warm compact update took 27.7 seconds
and the in-memory sparse handoff took 0.78 seconds for 30,683 activities and
988,796 stored exchange rows. This diagnostic is evidence for the handoff
phase only; the project performance gate still requires alternating medians.
