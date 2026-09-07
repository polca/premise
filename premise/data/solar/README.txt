IEA PVPS 2026 integration
========================

The default PV core for ecoinvent 3.12 cut-off is lci-PV-2026.xlsx. It replaces,
rather than accompanies, the old lci-PV.xlsx core. Other versions and the
consequential model retain lci-PV.xlsx pending separate migration validation.
The inventory cache schema changed from 5 to 6 to prevent reuse of the old core.
The 3.12 cut-off inventory cache key also fingerprints the three PV workbooks,
so workbook edits invalidate the inventory cache without re-extracting ecoinvent.

Files
-----
* additional_inventories/lci-PV-2026.xlsx: 324 reviewed activities. The 45
  original electricity functional units retain their exchange quantities.
  REF is a reference-system label, not a geography: its 12 activities use GLO
  in the packaged file and retain REF in the "pv reference location" field.
  Their names retain the 1000 or 1300 kWh/kWp/year yield assumption. They are
  not selected as country suppliers by the residential/commercial IAM aliases.
* additional_inventories/lci-PV-2026-electricity.xlsx: residential, commercial
  and combined electricity for 250 locations (249 ISO countries/territories
  and Kosovo), plus one compatibility activity for the legacy hydrogen model.
* additional_inventories/lci-PV-CIGS.xlsx: four distinct legacy CIS/CIGS
  manufacturing/storage activities and five foreground dependencies. Its
  input ecoinvent version remains 3.7; the normal importer migrates it.
* lci-PV-perovskite.xlsx and lci-PV-GaAs.xlsx remain separate default supplements.
* pv_2026_parameters.json: report-country yields, inherited Global Solar Atlas
  ground yields, geographic coverage and lifetime assumptions.
* pv_generation_2023.csv: IRENA 2025 solar-PV GENERATION in 2023, in rounded GWh.
  There are 219 country/territory observations. Their sum is 1,610,739 GWh;
  the published world total is 1,610,740 GWh (independent rounding).
  Flags e/o/u are retained as printed; a blank flag is not an imputed value.
* pv_2026_gwp100_reference.csv: 45 calculated ecoinvent 3.12 cut-off regression
  benchmarks, in kg CO2-eq/kWh. These are not characterized results published
  in the report. Method: ecoinvent-3.12 / IPCC 2021 / climate change: total
  (excl. biogenic CO2) / global warming potential (GWP100).

Country extension
-----------------
For each of the 33 report countries, installation amounts are converted back
to electricity shares as amount x capacity (kWp) x annual yield x 30 years.
Minor source rounding is removed by normalising these shares. Residential
contains 10 kWp rooftop/facade systems; commercial contains 250 kWp rooftop
and 10 MW ground systems. Shares are separately normalised within each segment.
The two reference yields are alternative site assumptions, never mix weights.

Other countries use the generation-weighted mean of the 33 national recipes,
with corrected IRENA 2023 generation weights. The suppliers' manufacturing
geographies are retained. This is a proxy mix, not a measured national mix.
The obsolete workbook's inconsistent generation values and 2024 label are
not used. For example: China 583,854; France 22,126; India 108,134; Chile 18,310
GWh, all for 2023.

Report-country yields take precedence. Elsewhere, retained Atlas ground yield
is multiplied by 0.94 for roof and 0.66 for facade. An absent or zero Atlas value
uses the generation-weighted mean report yield; the activity explicitly records
this fallback. The old Atlas CN-TW code is read as TW, as used by current
Premise geography. The unidentified Atlas -99 row is not used.

The source workbook's coefficients use 30 x annual yield. Although its comments
mention 0.7% annual degradation, this integration does not apply an additional
0.895 multiplier to those coefficients or to extensions. This preserves the
workbook convention; the yield definition should be revisited if the authors
clarify whether their reported yield already includes lifetime degradation.

Cleaning consumes 20 litres per square metre of module over its life: 90% is
wastewater, 10% evaporates. New country extensions use the Swiss cleaning-water
and wastewater suppliers as explicit proxies. Converted solar energy and waste
heat retain the common per-kWh amounts in the reference inventories.

Production volumes are generation_GWh x 1e6 x segment_share, in kWh. Missing
and rounded-zero observations remain zero. They do not contribute donor weights.
A segment with zero historical share uses the mean segment composition to
provide a prospective supplier, while retaining a zero historical volume.

Residential output is assigned to electricity, low voltage; commercial/utility
output to electricity, medium voltage. This defines the generation boundary;
no extra transformer inventory or unreported distribution loss is introduced.
Combined adapters use electricity, photovoltaic, at plant. Premise's existing
IAM aliases select the residential and commercial names.

Modelling adaptations and scope
------------------------------
The empty US multicrystalline-wafer market is supplied by the report's domestic
US wafer production, with transport retained. Removed from the reviewed core:
unused historical batteries, avoided-burden processes, the duplicate CIGS branch,
and the perovskite-silicon tandem branch with its exclusive inputs. Cut-off
allocated recycling inventories remain. Linking/proxy assumptions remain in
individual comments; publication references are in the source field.

The legacy electrolysis activity labelled "at 280 kWp flat-roof, single-Si"
now delegates 1 kWh to the new 250 kWp single-crystalline reference at
1000 kWh/kWp/year. The label remains solely for backward-compatible linking;
there is no 280/250 scaling of a kWh functional unit.

Future module efficiency uses explicit technology and capacity metadata,
including 10 MW = 10,000 kWp. It uses the total module area across all module
inputs. Module area, associated mounting and module recycling scale together;
fixed capacity-dependent equipment remains unchanged. Lifetime cleaning water
is scaled by the relative change in module area, preserving the original core
cleaning coefficients when efficiency is unchanged. Existing higher module
efficiencies are not reduced.
Operational PV metadata survives inventory and scenario caches.

Reproduction and validation
---------------------------
Run `python scripts/build_pv_2026.py` from the repository root to rebuild the
country adapters and CIGS supplement from the versioned inputs. The reviewed
core is the numerical source; this script adds integration metadata and the
REF-to-GLO geographic adaptation. All exported exchange amounts are numeric;
calculation expressions are described in exchange comments. No Excel formula
recalculation or external workbook links are required.

Run `python scripts/validate_pv_2026.py` in a configured Brightway environment
with project ecoinvent-3.12-cutoff and PREMISE_KEY in the environment. This checks
the complete default graph, recalculates the 45 GWP benchmarks, and builds an
IMAGE SSP2-M 2050 electricity scenario without writing a Brightway database.
Unit regressions are in tests/test_photovoltaic.py.

Sources and attribution
-----------------------
IEA PVPS Task 12, report T12-33:2026 and accompanying LCI workbook:
https://doi.org/10.69766/WJTE1771
The report states CC BY-NC-SA 4.0, with exceptions/restrictions in its licence
notice. This source material has separate terms from Premise's software licence;
retain its attribution and source notice with the adapted inventories.

IRENA, Renewable Energy Statistics 2025, solar photovoltaic production tables:
https://www.irena.org/-/media/Files/IRENA/Agency/Publication/2025/Jul/IRENA_DAT_RE_Statistics_2025.pdf

Global Solar Atlas:
https://globalsolaratlas.info/download/world
The inherited workbook did not record the Atlas retrieval date. These are
retained numeric values, not a newly downloaded Atlas release.
