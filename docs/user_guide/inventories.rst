Inspecting and modifying inventories
======================================

After building a scenario, use its inventory store to find activities and
inspect their inputs. You can query a few activities without loading a second
copy of the full database.

Read activities
-----------------

After :doc:`/getting_started/first-scenario`:

.. code-block:: python

   store = ndb.get_inventory_store(scenario=0)
   activities = store.find({"location": "CH"})
   for activity in activities[:5]:
       print(activity["name"], activity["reference product"], activity["unit"])

``scenario=0`` selects the first scenario in ``ndb.scenarios``; use ``1`` for
the second, and so on. Query results are read-only snapshots. See
:doc:`/reference/api` for the available search methods.

Edit an activity
------------------

Use a writable store and a transaction, which saves a group of edits together.
This example uses the compact storage selected by the first-scenario script:

.. code-block:: python

   store = ndb.get_inventory_store(scenario=0, writable=True)
   activities = store.find({"location": "CH"})
   if activities:
       with store.transaction("custom:comment") as transaction:
           transaction.patch_activity(activities[0].id, {"comment": "My study note"})

If an exception leaves the transaction, its edits are undone. Adding,
cloning or removing activities and replacing exchanges also require a
transaction. Previously returned snapshots do not change when you edit the
store; query again to see the updated activity.

These edits change the in-memory scenario inventory. To use them in Brightway
or another program, export it again. Inventory changes also require validation
to run again before export.

Obtain a complete copy
-----------------------

If another library needs a list of dictionaries, use:

.. code-block:: python

   database = ndb.materialize_inventory(scenario=0, restore_metadata=True)

This copies the full inventory and can require several gigabytes. Editing
that independent copy does not edit ``ndb``. Use the store when you only need
to inspect or change a few activities; the old mutable ``ndb.database``
attribute is no longer available.

.. raw:: html

   <span id="automatic-scenario-cache-expiry"></span>
   <span id="compact-storage-and-checkpoints"></span>
   <span id="id1"></span>
   <span id="id2"></span>
   <span id="id3"></span>
   <span id="on-this-page"></span>

Saved inventories and cache cleanup
------------------------------------

Premise saves intermediate inventories so later operations can reuse them.
Managed scenario checkpoints unused for more than 24 hours can be removed
when a new build starts and no other ``NewDatabase`` instance is alive.
Long-lived notebook instances can therefore delay cleanup.

Set ``cleanup_expired_caches=False`` to skip that startup cleanup. Source and
import caches have a separate lifetime; see :doc:`source-databases` for the
cache-clearing commands. Storage formats, locking and detailed cleanup rules
are described in :doc:`/development/architecture`.

To clear prepared source caches or only cached additional inventories, use:

.. code-block:: python

   from premise import clear_cache, clear_inventory_cache

   clear_inventory_cache()  # Reimport additional inventories on a later build.
   # Alternative: clear_cache() removes all prepared source/import cache files.

These commands clear reusable input caches, not your written Brightway
databases. The managed scenario-checkpoint cleanup described above is a
separate operation.

Self-contained API example
----------------------------

This small example exercises queries and edits that are saved or undone together without licensed
source data or IAM files:

.. literalinclude:: /examples/inspect_inventory.py
   :language: python
