# Experimental annual profile resource, version 1

Implemented on `feat/stock-vintage-pilots` in premise and TRAILS. This is a
software transport contract; validation does not approve the scientific content
of a profile. Legacy exports remain the default.

Pass a validated dictionary as `stock_vintage_profiles` to `TrailsDataPackage`.
The dictionary has `schema_version: 1` and the following members:

| Member | Content |
|---|---|
| `inventory_context` | Nonempty strings `source_database`, `source_version`, `system_model`, `model`, `pathway` |
| `profiles` | Nonempty list of profile records described below |
| `bindings` | Nonempty list of exact caller/supplier bindings |

Each profile has a unique `id`, `event_role`, `asset_unit`, `service_unit`,
`allocation_basis: "common_amortisation"`, a nonempty `provenance` object and a
nonempty `years` list. Each annual record contains integer `service_year`,
integer calendar `event_years`, and finite nonnegative `weights` summing to one
within absolute tolerance `1e-10`. Duplicate event years must be aggregated
upstream. The producer requires consecutive years between each profile's first
and last records; a request outside that range is an error.

The supported roles are `existing_asset_service`, `new_asset_component`,
`replacement_component` and `lifecycle_service`. Only lifecycle profiles may
place events after the caller year. Roles express reviewed rules; the exporter
does not infer them from names. A zero-offset new-component profile can prevent
an inappropriate legacy stock shift, but genuine manufacturing capital is a
separate asset role.

A binding contains `profile_id`, `caller` and `supplier`. Both identities use
exact `name`, `reference product`, `unit` and `location` fields. They must be
different activities and their units must match the profile. Every profile
requires a binding; each binding must match an actual inventory exchange in
each exported scenario. Matrix indices are generated, not maintained identities.

The producer writes type 6 anchor metadata with offsets `event_year-service_year`
and `temporal_amount_source="port"`. It never changes exchange amounts. It also
writes `stock_vintage.json` as a `data-resource` named `stock-vintage`. Package
metadata under `stock_vintage` includes schema version, resource name, SHA-256,
generator identity, allocation convention, inventory context and required
capability `annual-stock-vintage-v1`. Global matrix reordering does not affect
identity-based bindings.

TRAILS resolves the sidecar at the requested service year, including years
between matrix anchors. Enable `stock_vintage=True`. Legacy packages retain
legacy behaviour; new readers reject corrected packages without this explicit
opt-in. Older readers may ignore the new resource and are unsupported. Use the
companion TRAILS branch; a released minimum version has not been assigned.

The JSON hash is verified even with warm matrix caches. Foreground replacements
override package bindings in that model instance. Such modified matrices are
not persisted over the base package cache, because removed bindings are instance
state. See the companion TRAILS `docs/stock-vintage-development.md` for examples,
cache semantics and integration tests.

The full scientific [data contract](data-contract.md) still governs provenance,
unknown stock, service weighting, source rights, boundary review, stock balance
and lifecycle chronology. Those requirements extend beyond this parser. No
blanket CC0 claim is attached to a corrected package: inventory and observation
source rights remain applicable.


## Verified reader compatibility, 10 October 2026

The preserved TRAILS `b7d1bb78` reader was run against the actual premise-generated
synthetic annual package. It loads the file but ignores the annual resource:
for service year 2021 it emits `{2009: 0.05, 2020: 0.20}` instead of
`{2010: 0.10, 2021: 0.15}`. The corrected opt-in reader produces the latter.
Use the paired premise/TRAILS implementation branches, including premise
`edc89b46` and TRAILS `a3ef279` or their reviewed descendants. The evidence archive
pins exact code. No claim is made that old readers reject unknown metadata or
that a public minimum release version has already been published.
