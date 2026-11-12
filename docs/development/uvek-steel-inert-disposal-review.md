# Steel: Swiss inert-waste disposal

The source market has a negative one-kg waste reference, a deterministic -1 kg
input to its treatment provider, and two collection-freight inputs. The treatment
provider contains exactly generic inert-landfill operation (1 kg) and landfill
infrastructure (1.48e-9 facility units), with no direct biosphere exchanges.
The suggested construction-waste type-B disposal is not accepted as equivalent.

For the exact ecoinvent 3.9 cut-off profile, one positive physical-service recipe
retains these two treatment components plus 0.0036 rail and 0.0258 road tonne-km.
It uses the already inspected complete native Swiss generic landfill suppliers
and native RER rail/fleet-average road services. Original freight uncertainty is
retained. The deterministic treatment share permits flattening the two-component
treatment into this one recipe without multiplying uncertain distributions.
The source-to-service factor is -1, preserving physical treatment demand when
the caller has a negative waste exchange. No moisture correction, recovery
credit, invented direct leachate emission or second disposal burden is added.

The complete native operation and infrastructure retain their own internal
inventories. Geography, transport fleet, landfill technology, land use,
aftercare, vintage, allocation, internal uncertainty and native biosphere
exclusions can differ; score effects remain unquantified.

The same rule had an earlier independently approved ecoinvent 3.6 scope. Its
source, target supplier, conversion factor and reference-sign decision remain
unchanged. The shared rule now carries explicit 3.6 and 3.9 selections. Because
the approval fingerprint includes the full shared rule and recipe closure,
the existing 3.6 fingerprint was refreshed with an explicit retention receipt;
this does not claim a new scientific review or improvement of that older proxy.
Other releases remain unapproved.

Frozen review: `export/uvek_steel_39_inert_review.json`, SHA256
`3a48cfa716b1576b793acab9810e0690987a3e6e58d580aa237bf0fc51cd4d27`.
Brightpath commit: `9ab2c08335e864d6f7e5dacd1f489fdbebb3bb99`.
Runtime Python modules remain unchanged. Maintainer replay scripts, generator
hook and tests support reproducibility and reject parent/recipe drift atomically.
Licensed source vectors remain private.

Sixteen Brightpath inert-disposal/air checks pass together (21.97 seconds),
including signed amounts, normal/lognormal uncertainty, formulas, input
immutability, exact release scope, replay idempotence, drift refusal and unchanged
historical 3.6 selection. The Premise strict fresh/cache/approval-withdrawal test
passes (18.65 seconds). Black and Ruff pass. The weighted four-input native
service passes two finite signed composite LCIs with exact sign reversal and
unchanged source database. See `uvek-steel-inert-composite-lci-verification.json`.

The actual encrypted REMIND SSP2-NPi 2050 diagnostic preparation now reports
**36 unapproved correspondences, down from 37**. Only the closed steel support
guard was bypassed; strict migration validation remained enabled. The first
remaining refusal is iron-ore concentrate (`bp-23f293b98c9d5570`), whose current
chromite suggestion requires correction. No steel transformation or export is
claimed. Private audit receipt:
`25f5cb874755dc0a9d8ecf279b06986dded87739e37d419974357df814c892f9.audit.json.gz`.
