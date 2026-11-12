# Quebec bark market co-products

The exact ecoinvent 3.9 cut-off Quebec bark market now uses three corresponding native RER bark suppliers: hardwood sawmill bark, softwood sawmill bark and allocated bark from soft fibreboard production using wet/dry processes. A market helper is needed to preserve its three original shares and final-product lorry freight; a complete generic bark mix would replace those shares.

All four original market rows remain separate, with original signed quantities, uncertainty and formulas. The fibreboard share (about 0.00651%) is retained as allocated bark, not omitted or replaced with full board production. All source/native outputs explicitly reference one kg measured as dry mass. The factor is +1; source wet mass of 2.4 kg per dry kg does not justify a wet-mass multiplier.

RER is a geographic screening proxy for Quebec. Within-class species, allocation, mill yields, wet/dry process mix, moisture, carbon, energy, infrastructure, vintage, internal uncertainty and native biosphere exclusions differ, with unquantified score effects. Existing native allocated inventories retain their own resource/energy/emission burdens; no allocation adjustment, main-board burden, avoided-product credit or carbon/energy correction is added. Original final delivery is added once, beyond production's internal/precursor transport, with an explicit European fleet/geography/fuel/load warning. Separate caller emissions remain unchanged.

Only this inspected 3.9 cut-off market is newly approved. RoW pulp/paper bark, window-frame pellet co-products and residual-wood conversion branches remain outside the approval: similarly named generic suppliers would not preserve their specific production or allocation boundaries. No source omission, other-version or consequential/reverse approval is added.

Twelve migration tests and one Premise fresh/cache/approval-withdrawal test passed. They check signed quantities, normal/lognormal uncertainty, exact component rows, allocated-bark supplier identity, nonempty inventories, rejection of unapproved profiles and atomic replay. All four original rows were independently re-read and checked against source hashes/statistics. Four native suppliers and the composite market demand passed ten signed finite-LCI checks, with exact sign reversal and unchanged database modification metadata. Frozen review SHA-256: f575ef52e8544c0467c903c4029ca8490dc375fe358497848c933a5865675129.

No runtime Python changes were needed. Full pulp heat, industrial heat and steel transformation/export remain unfinished. Licensed source inventories stay private; published bindings contain source hashes and derived native recipes.

The strict five-inventory pulp-heat probe decreased from 54 to 53 unresolved nested inputs. One known industrial-heat preparation root still blocks steel.
