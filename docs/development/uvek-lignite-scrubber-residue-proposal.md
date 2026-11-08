# Lignite scrubber: residue-boundary decision

Status: pending explicit approval. This proposal enables no correspondence,
helper or omission. The exact ecoinvent 3.6 cut-off rule is
`bp-7b9f2134fcb65b6d`, `desulfurisation of lignite flue gas`, mapped to native
`SOx retained, in lignite flue gas desulphurisation`.

Both references are positive kg of retained pollutant and both descriptions
state 90% removal. Actual source suppliers named “lime” were inspected: their
milled product uses crushed limestone. The service purchases approximately
2 kg of that reagent and 10 kg of decarbonised water, matching native quantities.
The legacy name alone does not establish that the reagent is quicklime.

The remaining difference is substantive:

| Boundary per kg SOx retained | Ecoinvent 3.6 | Native UVEK |
| --- | --- | --- |
| Gypsum disposal | 4.1 kg to landfills: about 2.87 kg inert and 1.23 kg sanitary | Gypsum described as burden free; no explicit gypsum output or disposal exchange |
| Wastewater | 0.0024 m³ through treatment markets with active treatment burdens | Description assigns water to wetting power-plant ash; no separate wastewater treatment exchange |

The source routes were followed to actual landfill and wastewater-treatment
providers, rather than inferred from market names. Native gypsum quantity is
not declared; this does not establish a 4.1 kg native coproduct yield.

The proposed screening proxy uses the complete existing native wet scrubber
once per signed kg retained, with factor +1. It retains native limestone,
decarbonised water and internal freight, while warning explicitly that the
source's gypsum disposal and wastewater treatment are not represented by that
native boundary. Separate caller exchanges and stack emissions remain unchanged.
No gypsum recovery credit, avoided-SOx emission, reconstructed residue stream,
efficiency scaling or additional wastewater process would be introduced.

This requires a new decision about process and residue boundaries; matching
removal efficiency and reagent quantities is insufficient. Source allocation,
geography, vintage, native biosphere exclusions and unquantified score effects
remain limitations. Approval would cover only this inspected source profile;
other releases, inverse routes and cross-system-model conversions stay closed.

The native supplier has a finite baseline LCI and unchanged scientific-content
hashes. This validates the existing supplier numerically, not the proposed
scientific equivalence or a prospective scenario. Private evidence is retained
in `lignite_scrubber_residue_route_evidence.json`,
`lignite_scrubber_proxy_candidate_review.json` and
`native_lignite_scrubber_proxy_candidate_lci_report.json` in the pilot work area.
