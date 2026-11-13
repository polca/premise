# Spent-solvent treatment: version-scoped correspondence review

Carbon-removal inventory preparation requires the Swiss hazardous-incineration
service for spent solvent mixture from ecoinvent 3.9 cut-off. Its existing
correspondence was independently registered only for 3.6. The reviewed definition
matches native UVEK 2026 in all 42 documented wet-composition coordinates,
including 16.5% water, and the hazardous-incineration route with wet scrubbing
and SCR. The complete native process is used per wet kilogram.

The source reference is −1 kg of waste; the native reference is +1 kg of
treatment. The existing factor −1 applies once: −2.5 kg becomes +2.5 kg of native
treatment, while a genuine positive waste credit becomes negative native treatment.
No moisture, heating-value or density multiplier, extra energy credit, omission,
or invented treatment inventory is introduced. Caller signs, formulas and
uncertainty transform together. The rule and compiled migration resource are
unchanged; no runtime Python modules change.

Requested cut-off profiles 3.6–3.12 are enumerated independently. The requested
3.7 scope uses inspected 3.7.1 functional-reference evidence and matching adjacent
3.6/3.8 treatment definitions; unpatched 3.7 coefficients were not inspected.
Neither patch profiles nor cross-system-model conversions inherit approval.
Native 2025's shorter comment lacks the full composition; its named wet-waste
class, positive reference and all 20 direct technosphere coordinates match the
native 2026 bridge destination. The earlier 3.6 inspection and approval are
retained inside the dedicated packaged review for traceability.

Retain native auxiliary fuels, flue-gas cleaning, infrastructure, residues,
solidification and landfill treatment. Some source purchases are useful heat,
where native inputs are fuel-combustion quantities: displayed MJ differences
must not be used to scale the complete disposal service. Older descriptions
swap thermal/electric recovery labels; stored inventories remain authoritative.
Both documented models allocate all burdens to disposal without substitution or
system expansion. Emissions, background, vintage and coverage differ; score
equivalence is not established.

The review is packaged in Brightpath revision
`7d9ba25f672dfcb7b606346ee6f3df2f8cbf9b79`. Premise registers six additional exact
scopes, retains all 1,250 previous fingerprints and changes no other operation.
The [verification receipt](uvek-spent-solvent-profiles-verification.json) records
completed tests and the next real-run result separately. This correspondence
alone does not qualify carbon removal or the other unfinished sectors.
