# UVEK grid-loss representation

The source-specific loss mapping uses Premise's unchanged electricity-market
equations. It does not extract two separately measured medium/low loss terms
from the inspected native UVEK grid records. Those records directly purchase
more than one upstream kWh per delivered kWh. The crosswalk puts that factor in
the upstream input and sets the separate market self-consumption coefficient
to zero. The zero is not a claim of physically lossless transformers or grids.

For a voltage step supplied entirely from upstream, let `s` be the market
self-input coefficient and `1+d` the upstream multiplier. Solving the self-loop
gives `(1+d)/(1-s)` upstream kWh per delivered kWh. The ecoinvent route can
extract `s` from the market and `d` from its voltage-transformation supplier.
The UVEK medium/low crosswalk uses `s=0` and `d=f-1`, where `f` is the native
upstream input factor. High voltage uses the separately documented self-input
conversion in `uvek2026_grid_losses.yaml`.

This is a mapping choice motivated by the inspected source structure. It
preserves those native electricity-input factors without inventing a loss
breakdown. It is not a claim of identical infrastructure or emission inventories:
a self-loop increases every nonproduction burden, whereas an upstream multiplier
only increases the electricity input. Standard Premise network and SF6 assumptions
remain standard assumptions rather than a full reproduction of native UVEK grids.
Regional country weights also retain the limitations declared in the YAML.

The inspected EUR medium-voltage market in the 11-November four-sector review
export produces one kWh, purchases about 1.010131 upstream kWh, and has a zero
self-input. Additional SF6 and network exchanges remain present. Native national
grids' one-for-one redirects to regional IAM markets are separate from these
regional market loss equations. Zero rows, numerical loss coefficients and
market-generation logic are unchanged by this clarification.
