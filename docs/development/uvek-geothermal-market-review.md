# Geothermal dependency preparation

The retained ecoinvent 3.6 cut-off geothermal workbook now prepares against UVEK 2026 and the ecoinvent 3.10 biosphere. Both the checkout test environment and the regular conda environment using the pushed Git installation prepare 48 selected records into 53 records, including five helpers and 12 heat roots. No unapproved correspondence remains in this preparation. Regional heat creation, relinking, export and finite LCI qualification remain unfinished. No heat alias is admitted by this change.

The final five decisions are:

- Inert waste uses complete native Swiss type-B disposal at factor −1, converting negative waste references into positive treatment services once. Moisture, sealing, leachate, closure and delivery differences remain explicit screening limitations.
- Diesel uses the complete native 10 MW service per signed MJ at factor +1. Both references describe fuel use, not kWh output. No electrical-efficiency multiplier, extra generator or heat credit is added.
- Wastewater retains the five original shares and corresponding complete native capacity classes. Documented COD concentration and class-specific sewer quantities match. Collection is already included; no extra sewer inventory or different 3.9 untreated-discharge model is added.
- Drilling waste retains 60% landfarming and 40% residual-material landfill and every separate freight term, including tiny branches. Native Swiss 71.5%-water services substitute RoW practices with composition, technology, allocation, vintage and emission warnings.
- Cement replaces the erroneous slag-cement match with complete Swiss CEM I 42.5 and 52.5 processes, using their historical 40:6 production split as an explicit screening strength split. Both original freight terms remain. This is not an observed source strength split or a claim of equal clinker/addition composition.

Freight uses complete native RER road/general-rail proxies, US diesel rail and the previously reviewed complete Swiss rail mix. All original tkm remain. Geography, fleets, load factors, energy and allocation differ. The bad general-rail → aircraft heuristic is not approved; coal-only rail is not used as general freight.

Signed caller quantities, formulas and uncertainty transform consistently. Source market uncertainty is retained where available in recipes; complete native suppliers replace internal producer uncertainty. Score effects are unquantified. Only exact 3.6 cut-off scopes are added; no cross-system-model conversion is introduced.

Brightpath `e1e0e48b5c079da78eb4b9f4d489676ad939306a` packages the two service reviews and three recipes. Runtime Python remains unchanged. A maintainer replay script and two tests preserve the mapping changes during regeneration. One existing 3.9 wastewater fingerprint is rebound to expanded profile/provenance metadata, without changing its selected scientific fields or existing recipe terms. Premise has 749 exact scopes.

Verification: 102 focused Brightpath tests; four signed service and six signed recipe checks; built-wheel evidence/resource identity; successful full retained preparation from the regular Git installation. The broader approval suite initially reported 829 passes and three failures. Two cache expectations failed during resource updates; a third assertion incorrectly excluded an already-approved 3.11 dependency. All three cases pass on recheck after settling resources and correcting that test expectation. A full rerun against frozen committed Premise/Brightpath sources now passes all 832 tests; the initial run itself is not reported as green.

See [verification receipt](uvek-geothermal-market-verification.json). The standing autonomous authorization covers the recorded native proxies and composition-preserving helpers.
