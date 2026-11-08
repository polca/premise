# Concrete sorting residue: pending screening decision

The pilot uses ecoinvent 3.6 cut-off's Swiss
`treatment of waste concrete, not reinforced, sorting plant`
(`bp-e6c831a29eb4d1fa`). The source and proposed native service have matching
dismantling diesel, sorting electricity, digger and facility requirements.
Both reference one kg of waste, with opposite waste/service reference signs.

Tracing the source residue market exposes a different fate: 0.4 kg of inert
residue per kg sorted goes to `clinker production`, whose reference product is
`inert waste`. This is a cut-off allocation node: the inspected stored inventory
has no technosphere inputs and 28 numerically zero biosphere exchanges, each
with uncertainty type 0. No avoided-clinker credit is inferred from that node.

UVEK's existing complete
`Disposal, building, concrete, not reinforced, to sorting plant` instead includes
0.4 kg of `Disposal, inert material, 0% water, to sanitary landfill`, plus its
native internal freight. Matching the sorting operations alone does not
authorize replacing the source co-processing fate with landfill.

Proposed screening choice, **not enabled**: use the complete existing native
sorting service per signed waste kg, with reference-sign factor -1. Preserve
its landfill, machinery, electricity, freight and emissions once. Warn that the
proxy changes residue fate and adds landfill burdens absent from the inspected
source allocation, with an unquantified impact-score effect. Retain geographic,
landfill-model, background and vintage limitations. Add no recycling credit,
second transport, invented clinker treatment or density/yield adjustment.

Alternatively, preserve the source co-processing fate through a separately
reviewed helper; that requires further implementation and uncertainty checks.
Neither alternative approves the PE/PP collection case, whose source has a
global mix of incineration, landfill, open dumping and open burning.

The source zero-allocation node's exchange-vector SHA-256 is
`5783c74eb255b757ec3c4dc41de3a06480b33971abf9b7e384e09326bccdb0d0`.
The proposal covers only this inspected 3.6 cut-off correspondence. No mapping
or helper is admitted by this document alone.
