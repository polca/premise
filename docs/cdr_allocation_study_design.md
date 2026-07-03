# Study Design: Allocating CDR Burdens to Everyday Products

## Working title

Hidden resource burdens of residual-emission compensation in climate-ambitious
scenarios.

## Central question

How much do the water, energy, land, and metals footprints of common end-user
products increase when the carbon dioxide removal (CDR) deployed in a
climate-ambitious scenario is reallocated to the products and services whose
supply chains still emit greenhouse gases?

## Motivation

Integrated assessment model scenarios usually show CDR as a separate sector.
This framing can make CDR look detached from the rest of the economy. In
climate-ambitious pathways, however, CDR deployment exists because other
sectors do not fully decarbonize. The study reallocates the physical CDR burden
back to residual greenhouse gas emissions in product supply chains and quantifies
the resulting increase in non-climate environmental footprints.

The commentary paper can make three points:

1. Everyday products in climate-ambitious futures may appear low-carbon because
   other sectors deploy CDR.
2. The resource burden of that CDR is not evenly distributed across products,
   because residual emissions are not evenly distributed across supply chains.
3. Net-zero product claims should report the land, water, energy, and metals
   requirements of compensation, not only the reduced climate score.

## Main study region and scenario

Focus the main analysis on Western Europe (WEU).

Candidate scenario:

- IAM model: IMAGE
- Pathway: SSP2-VLHO
- Target year: 2055
- Region: WEU

Selection rule:

- Use 2055 if WEU has a CDR mitigation ratio of 100 percent, or practically
  100 percent after rounding.
- If 2055 is not a native IAM output year, either interpolate to 2055 and report
  this explicitly, or use the closest native year only as a sensitivity case.
- If mapped CDR variables are missing or renamed in the IMAGE file, update the
  `carbon_dioxide_removal.yaml` mappings before selecting the year.

Preliminary local check:

- The local repository checkout contains `image_SSP2-M.csv`,
  `remind_SSP2-PkBudg1000.csv`, and `remind-eu_SSP2-NPi.csv`, but not
  `image_SSP2-VLHO.csv`.
- The Zenodo `image_SSP2-VLHO.csv` file is reachable.
- A quick inspection found a WEU region but no native 2055 column in the file.
- The downloaded file uses newer CDR labels such as
  `Carbon Capture|Geological Storage|Biomass` and
  `Carbon Capture|Geological Storage|Direct Air Capture`; the premise mapping
  should accept these labels in addition to the older `Carbon Removal|...`
  labels.
- With biomass geological storage and direct air capture counted as CDR, WEU is
  not at 100 percent in interpolated 2055 on a CO2-basis. The preliminary values
  are about 66 percent in 2055 and 100 percent in 2060.

## CDR allocation logic

The `cdr_allocation=True` logic should compensate all relevant greenhouse gases
on a CO2-equivalent basis.

Recommended accounting mode for the paper:

- Record gross greenhouse gas biosphere emissions before compensation.
- Reduce covered positive greenhouse gas biosphere exchanges by the regional CDR
  allocation share.
- Add a same-region input from `market for carbon dioxide removal` sized from
  the original gross greenhouse gas CO2e value.
- For datasets mapped to the global IAM region, including ecoinvent `GLO` and
  `RoW`, use the `World` CDR allocation share and global CDR market.
- Size the CDR input in kg CO2 removed, using the CO2-equivalent value of the
  residual greenhouse gas emissions.
- Report both the recorded gross climate footprint and the net climate footprint
  after emission reduction and CDR input.
- Avoid double counting CDR co-products: when CDR allocation is enabled,
  co-product datasets such as BECCS electricity and fuel production with CCS
  should not also carry their own atmospheric CO2 uptake credit or separate CO2
  storage-service input. The implementation zeroes `Carbon dioxide, in air`,
  negative `Carbon dioxide, non-fossil` emissions in CCS electricity datasets
  and CO2 compression, transport and storage technosphere inputs in mapped CCS
  fuel variables and fuel co-product datasets whose activity names explicitly
  indicate CCS, while leaving the CDR market uptake and storage chain intact.

This is a modelling allocation, not a claim that methane and nitrous oxide are
physically removed from the emitting process. The original gross emissions
should remain auditable through log parameters, while the transformed product
system represents the compensated residual emissions as reduced biosphere flows.

For each eligible dataset:

```
gross_GHG_CO2e = sum(exchange_amount_i * GWP_i)
allocated_CDR = gross_GHG_CO2e * regional_CDR_share
new_exchange_amount_i = exchange_amount_i * (1 - regional_CDR_share)
```

where:

- `exchange_amount_i` is the mass of greenhouse gas emission `i`.
- `GWP_i` is the IPCC 2021 GWP100 factor currently covered by the
  implementation. The covered gases are fossil CO2, CO2 from soil or biomass
  stock, methane, nitrous oxide, sulfur hexafluoride, tetrafluoromethane,
  hexafluoroethane and 1,1,1,2-tetrafluoroethane.
- `regional_CDR_share` is the scenario share of residual GHG emissions mitigated
  by CDR in the IAM region and year.
- `allocated_CDR` is added as kg input from the regional CDR market.
- `new_exchange_amount_i` is the compensated biosphere emission amount written
  back to the transformed dataset for covered positive GHG flows.

If the regional CDR share is 100 percent, the product receives enough CDR input
to compensate the full gross GHG footprint covered by the implementation and
the covered positive GHG biosphere exchanges are set to zero. For example, with
the IPCC 2021 GWP100 factor for fossil methane, 1 kg fossil CH4 receives
29.8 kg of CDR market input and the methane biosphere exchange is reduced to
0 kg when the regional share is 100 percent.

## Regional scenario share

For all-GHG compensation, calculate the regional scenario share on a GHG
CO2-equivalent basis rather than CO2-only:

```
regional_CDR_share = CDR / (gross_GHG_CO2e + CDR)
```

where:

- `CDR` is the absolute physical CDR deployment in the IAM region and year.
- `gross_GHG_CO2e` is the gross residual greenhouse gas emissions in CO2e.
- The share is capped at 1.
- Missing CDR data imply a share of 0.

If the IAM file only provides CO2 and not total GHG CO2e, document this and use
one of two approaches:

1. Extend the IAM mapping to include total GHG or Kyoto-gas variables.
2. Use CO2-only scenario shares but compensate product-level non-CO2 gases with
   CDR inputs, clearly separating the scenario-share basis from the product-level
   compensation basis.

The first option is methodologically cleaner.

## Product basket

Use a small, recognizable product basket that spans household consumption and
major hard-to-abate supply chains. Candidate functional units:

| Product or service | Functional unit | Rationale |
| --- | --- | --- |
| Passenger car mobility | 1 passenger-km or 1 vehicle-km | Mobility and residual fuel/material burdens |
| Passenger car | 1 vehicle | Durable product with metals and battery relevance |
| Tomato | 1 kg tomato at market | Food product familiar to readers |
| Beef | 1 kg beef at market | High methane intensity and land relevance |
| Milk | 1 kg milk at market | Agricultural methane and land/water relevance |
| Bread or wheat flour | 1 kg product | Staple food with lower GHG intensity |
| Concrete | 1 kg concrete | Construction material with broad use |
| Cement | 1 kg cement | Process emissions and CCS relevance |
| Low-voltage electricity | 1 kWh | Energy service and scenario-sensitive mix |
| Residential heat | 1 MJ useful heat | Direct fuel and infrastructure residuals |
| Freight transport | 1 tonne-km | Logistics and fuel transition burden |
| Smartphone or laptop proxy | 1 product | Metals and electronics relevance |

For the main WEU case, prefer activities located in WEU or countries mapped to
WEU. Where ecoinvent has no WEU market, use a defensible European or country
market and report the exact activity name, reference product, location, and unit.

## Databases to build

Build paired databases with identical settings except for the allocation mode:

1. `baseline`: climate-ambitious premise database without CDR allocation.
2. `cdr_compensated`: same database with all-GHG CDR allocation enabled.

Both databases should use:

- Same source ecoinvent version and system model.
- Same IAM model, pathway, region definitions, and year.
- Same premise transformations.
- Same imported inventories and caches.
- Same LCIA methods.

The difference between the two databases should isolate the effect of assigning
scenario CDR to residual product-system GHG emissions.

## Impact indicators

Primary indicators:

- Climate change, GWP100: used to calculate gross and net climate scores.
- Water use.
- Land use.
- Energy resource use or cumulative energy demand.
- Mineral and metal resource use.

Use EF v3.1 or another consistent method set available in the Brightway project.
For GHG conversion factors, use the same climate method used for scoring, or a
fixed IPCC GWP100 factor set stated in the methods section.

## Main calculations

For each product and each database:

1. Calculate LCIA scores for all selected indicators.
2. Record gross GHG footprint before CDR compensation.
3. Record CDR market input added per functional unit.
4. Calculate net climate score after CDR compensation.
5. Calculate absolute and relative increases in water, land, energy, and metals.

Report:

```
absolute_increase = score_cdr_compensated - score_baseline
relative_increase_percent = absolute_increase / score_baseline * 100
```

When baseline scores are close to zero, report absolute increases and avoid
unstable percent changes.

## Contribution analysis

For each product, decompose the added non-climate burden into:

- Direct CDR market input.
- CDR technology mix components.
- Energy inputs to CDR.
- Materials and infrastructure for CDR.
- CO2 transport and storage.

This supports the commentary claim that compensation can shift burdens from
climate to other resource domains.

## Expected figures

1. Product-by-indicator heatmap of percent increases in water, land, energy, and
   metals after CDR allocation.
2. Bar chart of gross climate footprint, CDR compensation, and net climate
   footprint by product.
3. Stacked contribution chart showing which CDR supply-chain components drive
   added burdens.
4. Scatter plot of gross product GHG intensity versus added non-climate burden.
5. Scenario-share panel showing WEU CDR share over time for IMAGE SSP2-VLHO once
   variable mappings are confirmed.

## Sensitivity analysis

Minimum sensitivities:

- Year choice: 2050, 2055 if interpolated, and 2060.
- CDR share basis: CO2-only versus all-GHG CO2e.
- GWP factors: IPCC AR6 GWP100 versus the selected EF climate method.
- CDR technology mix: default IMAGE mix versus DAC-heavy and BECCS-heavy
  alternatives if feasible.
- Product location: WEU aggregate versus selected countries such as CH, DE, FR,
  or NL.

## Key implementation tasks

1. Extend `cdr_allocation` from fossil CO2-only to all relevant GHG flows.
2. Reduce covered positive GHG biosphere exchanges by the regional CDR
   allocation share while recording the original gross GHG CO2e value.
3. Add a mapping from biosphere greenhouse gas flows to GWP factors.
4. Compute regional all-GHG CDR shares from IAM data, preferably using mapped
   total GHG CO2e variables.
5. Verify IMAGE SSP2-VLHO CDR variable names and update
   `carbon_dioxide_removal.yaml` if needed.
6. Confirm whether WEU reaches 100 percent mitigation by CDR at or near 2055.
7. Build paired databases and audit that CDR inputs are same-region.
8. Remove embedded atmospheric CO2 uptake credits and CO2 storage-service inputs
   from BECCS and CCS fuel co-products when `cdr_allocation=True`, so the
   removal credit and storage burden are represented only by the CDR market
   input.
9. Run LCIA and contribution analysis with reproducible scripts.

## Claims to avoid

- Do not claim CDR physically removes methane or nitrous oxide from the emitting
  process. It compensates their CO2-equivalent climate impact.
- Do not claim all products become impact-free. Only the selected climate metric
  can be netted to zero; other burdens usually increase.
- Do not hide gross emissions. Report gross and net climate footprints together.
- Do not generalize one WEU scenario to all regions without sensitivity analysis.

## Short methods paragraph draft

We generated paired premise databases for a climate-ambitious IAM scenario for
Western Europe. The first database applies the standard premise transformations,
whereas the second additionally assigns regional CDR deployment to product
systems in proportion to their residual greenhouse gas emissions. For each
dataset with positive greenhouse gas emissions, emissions were converted to
CO2-equivalents using a consistent GWP100 characterization set. The corresponding
amount of the same-region `market for carbon dioxide removal` was then added as
a technosphere input, scaled by the IAM regional CDR mitigation share, and the
covered GHG biosphere exchanges were reduced by the same regional share. We
calculated life-cycle climate, water, land, energy, and mineral-resource
indicators for a basket of everyday products and compared results with and
without CDR allocation. This isolates the additional resource burden and changed
residual-emission profile associated with compensating product-system greenhouse
gas emissions.
