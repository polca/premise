Inspecting and modifying inventories
======================================

.. contents:: On this page
   :local:
   :depth: 1

The examples below select ``inventory_backend="compact"`` explicitly when
constructing ``NewDatabase``. See :doc:`/development/documentation-audit` for
a legacy-backend transaction limitation found during this review.

Premise 2.5 owns inventories through ``InventoryStore``. Mutable inventory lists
are no longer exposed on ``NewDatabase`` or retained in active scenario
dictionaries.


After creating ``ndb`` as in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   store = ndb.get_inventory_store(scenario=0)
   activities = store.find({"location": "CH"})
   if activities:
       with ndb.get_inventory_store(writable=True).transaction("custom:foreground") as tx:
           tx.patch_activity(activities[0].id, {"comment": "Updated by the foreground integration"})

Read methods return immutable snapshots. Additions, patches, cloning, removals,
and exchange replacement must occur inside a transaction. A transaction commits
data and index changes together and restores its complete prior state if an
exception leaves the context.

Compact transactions keep a rollback-safe structural snapshot and copy only
rows they touch. The emissions transformation uses this path directly; sectors
not yet migrated continue through the private list-compatible bridge.

Integrations that cannot consume the store may explicitly materialize it:


.. code-block:: python

   database = ndb.materialize_inventory(restore_metadata=True)


This duplicates the complete graph as Python dictionaries and can require
several gigabytes for a full ecoinvent scenario. It is therefore an integration
boundary, not the normal inspection API.


Compact storage and checkpoints
---------------------------------


``CompactInventoryStore`` is the production and certification-performance
backend. It provides copy-on-write scenario forks, ordered indexes, and
versioned Arrow IPC checkpoints with a lossless metadata sidecar. The
dictionary-backed ``LegacyInventoryStore`` remains available as a compatibility
and differential-testing oracle. Common exchange strings and numeric values use
typed, batched Arrow columns; arbitrary fields are stored in one sidecar bundle
per activity. Reopening preserves Python and NumPy numeric scalar types exactly.
The bundle contains:


.. code-block:: text

   manifest.json
   strings.arrow
   activities.arrow
   exchanges.arrow
   metadata.bin
   metadata_offsets.arrow
   activity-fingerprints.pkl
   checksums.json


Checkpoint writes use a sibling temporary directory and replacement; every
file is verified before a bundle is opened. Store schema versions are
independent from the historical pickle cache schema. Existing
``inventory_backend="legacy"`` and ``inventory_backend="compact"`` calls remain
accepted; certification has identical semantics on both backends, while all
acceptance and integration runs use ``"compact"`` explicitly.


Automatic scenario cache expiry
---------------------------------


``NewDatabase`` checks for managed scenario checkpoints unused for more than 24
hours at startup. It also removes expired incomplete checkpoint writes. Cleanup
runs only when no other ``NewDatabase`` instance is alive: a process-held shared
lock protects inventories throughout updates, repeated exports, and reports.
The lock is released when the instance is garbage-collected or its process exits.
A long-lived notebook instance therefore defers cleanup until a later startup.

Pass ``cleanup_expired_caches=False`` to skip the startup sweep. This instance still
holds the shared lock. Restart existing premise processes when adopting this
feature so all builds participate in locking. Standalone use of the low-level
``InventoryStore`` API is not registered as an active build; keep checkpoints for
such use outside premise's internal ``cached_files`` directory.

Only newly written, UUID-named checkpoints in ``cached_files`` carry expiry
metadata. Successful checkpoint reopening refreshes their last-use timestamp;
expiry metadata is separate from the checksummed inventory payload. Base
checkpoints needed by retained checkpoints are preserved. Existing unmarked
checkpoints are never automatically adopted or removed; clean them separately
after stopping active builds. Source/import caches in ``cache``, validation
caches, and extracted datapackages are outside this expiry policy.

Cleanup logs a count and estimated reclaimed space at INFO level (unless
``quiet=True``); filesystem errors are warnings and do not abort a build.

Self-contained API example
----------------------------

This small example exercises querying and atomic editing without licensed
source data or IAM files:

.. literalinclude:: /examples/inspect_inventory.py
   :language: python
