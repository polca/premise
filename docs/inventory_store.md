# InventoryStore API (premise 3.0)

Premise 3.0 owns inventories through `InventoryStore`. Mutable inventory lists
are no longer exposed on `NewDatabase` or retained in active scenario
dictionaries.

```python
ndb = premise.NewDatabase(
    scenarios=[{"model": "image", "pathway": "SSP2-M", "year": 2050}],
    source_db="ecoinvent-3.12-cutoff",
    inventory_backend="compact",
)

store = ndb.get_inventory_store()
activities = store.find(
    ActivityQuery((FilterExpression("location", "CH"),))
)

with ndb.get_inventory_store(writable=True).transaction("custom:foreground") as tx:
    tx.patch_activity(activities[0].id, {"comment": "updated"})
```

Read methods return immutable snapshots. Additions, patches, cloning, removals,
and exchange replacement must occur inside a transaction. A transaction commits
data and index changes together and restores its complete prior state if an
exception leaves the context.

Compact transactions keep a rollback-safe structural snapshot and copy only
rows they touch. The emissions transformation uses this path directly; sectors
not yet migrated continue through the private list-compatible bridge.

Integrations that cannot consume the store may explicitly materialize it:

```python
database = ndb.materialize_inventory(restore_metadata=True)
```

This duplicates the complete graph as Python dictionaries and can require
several gigabytes for a full ecoinvent scenario. It is therefore an integration
boundary, not the normal inspection API.

## Compact storage and checkpoints

`CompactInventoryStore` is the production and certification-performance
backend. It provides copy-on-write scenario forks, ordered indexes, and
versioned Arrow IPC checkpoints with a lossless metadata sidecar. The
dictionary-backed `LegacyInventoryStore` remains available as a compatibility
and differential-testing oracle. Common exchange strings and numeric values use
typed, batched Arrow columns; arbitrary fields are stored in one sidecar bundle
per activity. Reopening preserves Python and NumPy numeric scalar types exactly.
The bundle contains:

```text
manifest.json
strings.arrow
activities.arrow
exchanges.arrow
metadata.bin
metadata_offsets.arrow
activity-fingerprints.pkl
checksums.json
```

Checkpoint writes use a sibling temporary directory and replacement; every
file is verified before a bundle is opened. Store schema versions are
independent from the historical pickle cache schema. Existing
`inventory_backend="legacy"` and `inventory_backend="compact"` calls remain
accepted; certification has identical semantics on both backends, while all
acceptance and integration runs use `"compact"` explicitly.

## Validation reports

Every completed scenario update receives one cached semantic certificate.
Incremental sector phases and their production coverage phase are persisted
with the scenario checkpoint; exporter schema phases are held only in memory so
exports cannot rewrite a certified checkpoint.

```python
report = ndb.get_validation_report(scenario=0)
report.raise_for_errors()

# Run the complete graph diagnostic explicitly when needed.
exhaustive = ndb.get_validation_report(scenario=0, exhaustive=True)

for phase in report.phase_results:
    checked = sum(result.checked_object_count for result in phase.rule_results)
    print(phase.phase_id, checked, phase.elapsed_seconds)
```

Validation is read-only and cannot be disabled through the public API.
Unsuppressed errors stop an update before checkpointing or export; warnings and
narrow, versioned suppressions remain visible in the immutable report.

The production certificate combines targeted sector contracts evaluated while
the transformation still owns direct references to its changed activities.
They check coverage, finite physical values, reference production, market
composition, and sector-specific methodological expectations. Electricity and
fuel contracts independently recompute consequential marginal mixes; heat,
steel, cement, biomass, metals, transport, batteries, renewables, mining,
carbon removal, final energy, emissions, and external scenarios add their
specific coverage, composition, linking, and physical-bound checks.

The exhaustive diagnostic additionally covers every activity and exchange:
required fields and exchange types, uncertainty, provider identity and
product/unit agreement, geographic fallback, exact duplicate suppliers, stale
links, declared transformation scope, target cardinality, and newly introduced
cycles. It runs automatically when sector coverage is unavailable or a
certified store generation changes, and explicitly when
``get_validation_report(exhaustive=True)`` is requested.

Source normalization happens once when its compact checkpoint is created.
Fast Brightway export assigns only required database and input identifiers and
lets the bounded writer serialize compatible NumPy scalars. Other exporters
retain their format-specific normalization. Validation itself is read-only. A
certificate key includes the store generation, scenario, source and IAM
identities, system model, ecoinvent version, and validation ruleset version.
Changing the store or ruleset therefore forces recertification. Brightway,
SimaPro, openLCA,
datapackage, and superstructure exports reuse the semantic certificate and add
only their streaming schema phase.

The public surface is additive: `NewDatabase.get_validation_report()` and the
immutable `ValidationIssue`, `ValidationRuleResult`, `ValidationPhaseResult`,
`ValidationReport`, and `PremiseValidationError` types are exported from
`premise`. `ChangeReportArtifacts` is likewise exported for the structured V2
Excel/Parquet report returned by `NewDatabase.generate_change_report()`.
Existing constructor and export signatures remain accepted.
