# Pulp-heat freight inputs

Two exact ecoinvent 3.9 cut-off freight markets are newly approved per signed tonne-kilometre. Both source and native suppliers have positive unit references, so the factor is +1. Original caller quantities, uncertainty, formulas and separate emissions remain unchanged. No new helper or source omission is added.

The RoW inland-waterway barge inventory is itself extrapolated from European Rhine-fleet data. Native European barge transport includes vessel, port/canal infrastructure, fuel and operational emissions. It is an explicit screening proxy: vessel lifetime/performance, load, port/canal allocation, geography, emissions and vintage differ. The existing barge parent, target and earlier 3.8 approval are unchanged.

The Brazilian >32 tonne EURO5 lorry match previously used an all-size Swiss fleet. It now uses native RER-labelled 32–40 tonne fleet-average transport. Native documentation explicitly says Swiss 2020 fleet shares and HBEFA 5.1, despite the RER label. It does not reproduce Brazilian Proconve P7/EURO5, 12% biodiesel/10 ppm sulphur diesel, utilization or local emissions. The class upper bound, payload, fleet standards, fuel carbon, infrastructure, geography and vintage differences have unquantified carbon and score effects. No payload or efficiency multiplier is inferred from gross-weight classes.

Use each complete native transport service once, without adding diesel, infrastructure, tailpipe emissions or another empty-return factor. Only the independently inspected 3.9 cut-off contexts are newly approved; no consequential/reverse or other-version approval is inferred.

Eleven migration tests passed, including positive/negative callers, normal/lognormal uncertainty, rejection of unapproved contexts, atomic review replay and preservation of the earlier 3.8 barge behavior. One Premise fresh/cache/approval-withdrawal test passed. Both native services pass four signed finite-LCI checks with exact sign reversal and unchanged database modification metadata. Frozen review SHA-256: cd0e60948d5ed82ceadeb284a35b8059b5acc3ad3664a7a9742c7a5cfc9c3ed9.

The strict five-inventory pulp-heat probe decreased from 61 to 59 unresolved nested inputs. One known industrial-heat preparation root still blocks steel. Full pulp heat, steel transformation/export and the remaining sectors are unfinished. No runtime Python changes were needed.
