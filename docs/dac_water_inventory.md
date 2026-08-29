# Water accounting in the direct-air-capture inventories

The packaged DAC inventories distinguish gross water supply, water consumption,
and one-time plant water stock. This avoids treating every water input as consumed
and avoids annualizing construction water inside the operating exchange.

## Solvent-based DAC

The reference plant captures 1 Mt CO2 per year for 20 years. Qiu et al. report a
744,000-tonne initial circulating-water charge and 3.4 Mt/year of make-up water at
20 °C, 60% relative humidity and 90% operation. The source describes the latter as
replacement of evaporation from the recirculating cooling system.

The source inventory therefore contains:

- `3.4 kg` tap-water make-up per kg CO2 in every operating variant;
- `0.0034 m3` Water to air per kg CO2, closing the operating mass balance at a
  density of 1,000 kg/m3; and
- `744,000,000 kg` tap water per solvent-DAC plant as construction stock. With a
  plant exchange of `5e-11 unit/kg CO2`, this adds `0.0372 kg/kg CO2` over the
  assumed plant output.

The source does not state the residual alkaline-solution inventory or treatment at
decommissioning. The workbook identifies this explicitly but does not invent an
emission or waste-treatment exchange. A future revision should add a chemically
appropriate treatment only when a defensible source is available.

The climate values in
[`dac_water.yaml`](../premise/data/cdr/dac_water.yaml) are sensitivity cases, not a
probability distribution. Water scaling is separate from IAM energy-efficiency
scaling. `CarbonDioxideRemoval.adjust_dac_water` always scales make-up and
evaporation together, enforces the documented lower bound, and is idempotent.
Technosphere relinking during normal premise regionalization selects the most
appropriate available tap-water provider for each IAM-region proxy.

## Sorbent-based DAC

The Deutz and Bardow foreground reports no external operating-water input for the
temperature-vacuum swing adsorption plant. The base inventory consequently has no
direct water input or release. Atmospheric water can co-adsorb and be recovered,
but no credit is given because recovery depends on site conditions and the source
does not specify a product quantity or quality. The `0.8-2.0 kg/kg CO2` values in
the configuration are optional sensitivity bounds only.

The sorbent-manufacturing chain is modelled as follows:

- one kg sorbent contains `0.36 kg` PEI and `0.64 kg` silica support;
- 95% silica-support recycling is represented by `0.032 kg` virgin silica per kg
  sorbent (`0.64 * 0.05`);
- only the `0.36 kg` PEI fraction uses the spent-resin incineration proxy; and
- sodium sulfate is restored as a PEI co-product credit at the midpoint of the
  reported `3.30-5.89 kg/kg PEI` range (`4.595 kg/kg PEI`).

For virgin silica, `40 kg` process water and `35 kg` wastewater are retained per kg
silica. The wastewater is `0.035 m3`, not `35 m3`. Applied to one kg sorbent after
95% silica recycling, the intended wastewater is `35 * 0.64 * 0.05 = 1.12 kg`, or
`0.00112 m3`. The fate of the remaining `5 kg/kg silica`, and the complete PEI
output-water split, are not reported and remain explicitly unresolved.

## Reproduction and validation

The workbook correction can be reapplied safely:

```console
python dev/update_dac_water_inventories.py
python dev/audit_dac_water_inventories.py
pytest -q tests/test_cdr_water_inventories.py
```

These checks operate on the source workbook and transformation logic only. They do
not rebuild a Brightway database or calculate LCIA. Also note that adding Water to
air closes the inventory mass balance but does not reduce an LCIA indicator defined
solely as gross freshwater extraction.

## Sources

- Qiu, Y. et al. (2022), *Nature Communications* 13, 3635,
  <https://doi.org/10.1038/s41467-022-31146-1>.
- Deutz, S. and Bardow, A. (2021), *Nature Energy* 6, 203-213,
  <https://doi.org/10.1038/s41560-020-00771-9>.
- National Academies (2019), *Negative Emissions Technologies and Reliable
  Sequestration*, Chapter 5, <https://doi.org/10.17226/25259>.
