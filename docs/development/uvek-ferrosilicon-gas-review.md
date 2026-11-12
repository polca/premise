# Ferrosilicon delivered-gas review

Thirteen ecoinvent 3.9 cut-off low-pressure gas markets now use complete native same-location low-pressure gas at-consumer inventories. Source production properties explicitly state 36 MJ net heating value for each positive 1 m³ reference. Native positive 1 MJ references document 36 MJ/Nm³. The conversion factor is therefore +36 MJ per signed source m³, with caller uncertainty and formulas scaled consistently.

The locations are CA, TR, RoE, BE, RoW, IT, ES, US, DE, FR, NL, MX and GB. Matching labels do not establish identical regional membership or gas origin. Swiss-based native distribution assumptions, leakage, supply mix, vintage, allocation, biosphere exclusions, conventional volume definitions and internal uncertainty remain screening limitations. Carbon composition and score equivalence are not established. Separate caller emissions remain unchanged.

The complete native delivered-gas process replaces the source supply model. No extra leakage, combustion, heat production, credit or loss multiplier is added. No helpers or runtime Python changes were introduced. Only the inspected ecoinvent 3.9 cut-off profiles qualify; other versions and consequential/reverse routes remain unapproved by this review.

The Swiss case is excluded from this batch because an existing ecoinvent 3.12 approval binds a different reconstruction. That approval remains unchanged; its 3.9 mapping is still unfinished.

Validation: 11 Brightpath tests, one Premise fresh/cache/withdrawn-approval test, and 26 finite signed LCIs with exact sign reversal passed. Source database modification metadata remained unchanged. The real ferrosilicon probe fell from 31 to 18 unapproved supply correspondences. The global ferrosilicon reconstruction, industrial-heat preparation root and full steel transformation/export remain unfinished.

Frozen review SHA256: f2918c3bd4da3ab9a6011103f9da081464e6a63affe897853399d5c8b5482f7c. Brightpath commit: 522fb2d416f7769bfbe977d42b9d198aed72f239.
