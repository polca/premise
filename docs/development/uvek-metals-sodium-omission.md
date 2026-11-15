# Metals: scoped sodium-emission omission

The metals inventory preparation failed on the CLIC **disodium disulphite**
dataset (same reference product, GLO, kilogram), using ecoinvent 3.9 cut-off.
Its generic Sodium emission to unspecified water has no identical publisher UUID
or reviewed exact synonym in the selected ecoinvent 3.10 biosphere. The nominal
source exchange is **0.00134036749331893 kg per kg of chemical**. No claim is made
that this is negligible or that its impact is zero.

The source record is CLIC `disodium-disulphite`, version 1.0.0 (1819), SHA256
`78ea029727815efc3d71e18a0ca811296aba7f008a26a92ee1f3cdfa5f7b8973`.
These pins describe the inspected evidence; the existing omission mechanism
matches dataset identities rather than freezing archive hashes.

The generic source flow UUID `723f56f9-c68e-5692-a969-0fb2a70bce20` must not be
asserted identical to Sodium ion UUID `5ac961f0-8e53-42ff-b13f-1a7fb9133868`.
The generic flow is retired in the target catalog. Native UVEK's **Sulphite,
at plant** is not a replacement for the complete foreground chemical: its
metadata identifies the sulfite anion SO3(2−), CAS 14265-45-3, rather than
sodium metabisulfite Na2S2O5. No chemical-basis factor is invented.

Following the user's standing instruction to omit unsupported exchanges with
warnings, and standing auto-approval for subsequent documented decisions,
`approved_omissions.json` now authorizes only this biosphere exchange for
**UVEK 2026 cut-off with ecoinvent 3.10 biosphere**. The source context must remain
ecoinvent 3.9 cut-off with its 3.9 biosphere. All other chemical exchanges and
caller quantities remain intact. There is no avoided-emission credit or added
zero-burden chemical supplier. The omitted impact remains unquantified.

A small generic Python adjustment is necessary because the existing omission
matcher only compared technosphere identity fields and labelled every audit a
technosphere omission. Biosphere identity requires name, compartment and unit.
The adjustment matches these fields for biosphere exchanges and emits
`migration.biosphere_omission`. Existing technosphere matching is unchanged.
Original signed quantities, uncertainty, source version evidence and exchange
indices are retained in owned audit copies. Ambiguous duplicates still fail.
The canonical biosphere migration rules and strict UUID/synonym checks are
unchanged.

Verification: `tests/test_clic_omissions.py` passes **24 tests**, including signed
quantity/uncertainty audit ownership, retention of air and surface-water flows,
wrong-unit and Sodium-ion exclusions, owner/target exclusions, ambiguity/source
refusals, and the existing harmonizer cache/provenance checks. This is a scoped
screening decision, not qualification of the metals sector. Actual metals
preparation and transformation must still be tested separately.

The actual ten-record source 3.9 cohort also passes the normal CLIC harmonizer:
ten inputs yield 32 records including reviewed helpers, with one
`migration.biosphere_omission` audit, a visible warning, and unchanged source
records. No Brightway database is written. The full metals trial advances to
16 refused source 3.8 correspondences; metals remains unfinished. See
`uvek-metals-sodium-verification.json`.
