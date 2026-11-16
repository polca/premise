# Completing the first car-import source cohort

The five remaining ecoinvent 3.12 correspondences now pass. The same nine-root,
48-foreground import has reached its ecoinvent 3.9 cohort, where eight further
correspondences require review. The car update itself is not yet qualified.

Four supplies reuse existing reviewed inventories: average wastewater treatment,
natural-gas district/industrial heat, pure hydrogen fluoride and packed basic
refractory. Their source-3.9 and source-3.12 product definitions are identical,
with matching signed reference quantities and units. That establishes their
documented function, not identical inventories. Each new wrapper explicitly
delegates to the older source-3.9 model. Model and caller versions remain separate
in provenance, including every nested helper. Current internal production,
treatment and delivery mixes are replaced by the historical models, with
warnings and unquantified score effects. No zero-stock or source-omission helper
occurs in these reused closures. All previously approved effective profile rules
remain unchanged.

Hydrogen needs a different solution. The source market's producer delivers
gaseous hydrogen at 200 bar; the available complete native SMR process delivers
25 bar. The previous liquid-hydrogen/mercury-cell candidate is incorrect. A new
screening helper keeps one kg of native 25-bar gas and adds calculated electricity
for compression to 200 bar. Its current callers are chemical manufacturing
processes, rather than vehicle fuelling services.

The added stage assumes two equal pressure ratios, ideal gas, perfect
intercooling to 25°C, heat-capacity ratio 1.4, molar mass 0.002016 kg/mol,
80% isentropic efficiency and 95% motor efficiency. Both pressures are treated as
absolute. The resulting electricity
purchase is 1.0882062648 kWh/kg from the complete native Swiss medium-voltage
grid. These are deterministic modelling assumptions, not measured compressor
performance. The calculation uses the published
[compressor-work equation](https://www.grc.nasa.gov/www/k-12/airplane/compth.html)
and [equal-stage compression framework](https://pmc.ncbi.nlm.nih.gov/articles/PMC7517211/).
The hydrogen compression analysis by
[Franco and Giovannini](https://doi.org/10.3390/hydrogen5020017) also examines
isentropic efficiency and intercooling assumptions.

The extra stage does not model real-gas corrections, compressor manufacture,
cooling auxiliaries/water, leakage, storage, variable load or merchant delivery.
Those missing details are not physically zero. Native SMR production and its
upstream fuel, emissions and infrastructure remain complete. The pressure-work
model is explicitly a screening approximation, with an unquantified score effect;
it is not described as conservative or equivalent to the original source model.
No liquefaction, combustion, extra vehicle-tank compressor or heat credit is added.

Changes stay in mapping resources, approval data and the maintainer replay. No
runtime Python or new source-exchange omission is introduced. Clean generation
reproduces all three packaged forward resources byte for byte. Tests check signed
demands and uncertainty, distinct model provenance, strict refusal, unchanged old
profiles, atomic replay and thermodynamic work bounds. The complete real car run,
physical checks, combined scenario and finite LCI checks remain unfinished.
