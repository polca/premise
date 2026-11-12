# Ferrosilicon coal and fuel-oil supply review

Fifteen ecoinvent 3.9 cut-off fuel markets use complete native regional-storage supplies per signed kg: eight hard-coal markets and seven heavy-fuel-oil markets. Source and native references are positive 1 kg, nonempty, and the native inventories include actual transport exchanges. Factor +1 preserves caller amounts, uncertainty, formulas and separate emissions. No helpers or runtime Python changes are introduced.

Coal locations AU, RLA, ZA, IN and RU retain their location codes. Explicit coal geographic proxies are ID → AU, CN → CPA and RNA → US. The seven heavy-fuel-oil locations (RoW, IN, CO, RER, ZA, PE, BR) use complete native RER regional storage; six are geographic proxies. Equal regional labels do not establish equal membership.

These are screening replacements of complete internal supply models, not exchange-by-exchange reconstructions. Source mining/refining, freight distances/modes, storage, self-consumption, dust and aqueous discharges are replaced by the native model. Coal rank, moisture, calorific/carbon composition, petroleum specifications, allocation, geography, vintage, biosphere exclusions and internal uncertainty can differ. Score and carbon-balance effects are unquantified. No source freight, losses, combustion or recovery credit is added on top of the complete native provider.

Only independently inspected ecoinvent 3.9 cut-off profiles qualify. Other releases, consequential and reverse routes remain unapproved by this review.

Validation: 11 Brightpath tests, one Premise fresh/cache/withdrawal test and 16 signed finite native LCIs passed, with exact sign reversal and unchanged source modification metadata. Frozen review SHA256: 49d04d53874ead3b453cea1fe580a06760dba9ba46a064f23cea92cd0370d47c. Brightpath commit: 0c7ae250078d83aedd5ecbe911e1dcf491cabc25.

The real ferrosilicon dependency probe is reduced from 18 to three unapproved supplies: coal tar, petroleum coke and RoW low-pressure gas. The complete ferrosilicon reconstruction, industrial-heat preparation root and full steel transformation/export remain unfinished.
