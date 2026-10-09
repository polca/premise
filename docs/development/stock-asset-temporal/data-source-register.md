# Stock-asset data source register

Screened: 9 October 2026. This is a search register, not a list of approved
defaults. Difficulty assessments and recommended uses are our interpretation
of the documented fields and coverage.

Status codes: **F** = a data file or API extract was downloaded and inspected;
**D** = a data dictionary/codebook was inspected locally, but not the population
file; **P** = primary provider documentation or published tables inspected;
**L** = a lead with no current usable data extract established. None implies
complete validation. Acquisition hashes are in the
[manifest](acquisition-manifest.csv); local lifetime workbooks are in the
[separate inventory](local-source-inventory.csv).

## Road vehicles

| ID / source | Evidence and access | Limitation and acquisition action |
|---|---|---|
| S01 — [Eurostat `road_eqs_carage`](https://ec.europa.eu/eurostat/databrowser/view/road_eqs_carage/default/table?lang=en) | **F**. Public API stock counts by country, year and age class; Germany 2024 extract inspected | Five age classes plus total; no technology split in this table. Check country-specific bins/flags, oldest open bin and coverage; seek class-specific tables separately |
| S02 — [DfT/DVLA vehicle tables](https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-tables) | **F**. `VEH1111.ods` inspected; surviving licensed vehicles by first-use year, body type, fuel and observation year. GB series begins 1994, UK 2014; public download | Imported/unknown first-use categories require attention. `VEH1107`, registrations and class-specific tables are complementary leads; do not interpret domestic registration as global manufacture |
| S03 — [Held et al., fleet survival and trade](https://doi.org/10.1186/s12544-020-00464-0), [UNECE trade-data note](https://unece.org/fileadmin/DAM/trans/doc/2019/wp6/ECE-TRANS-WP6-2019-5e.pdf) | **P**. Methods and evidence for regional fleet survival/used-vehicle transfers | Neither is a complete current global cohort registry. The UNECE note explains limits of trade classifications and missing used-vehicle ages. Obtain current customs definitions and age-at-transfer data before reconstruction |
| S28 — [DVSA anonymised MOT data](https://open.data.dvsa.gov.uk/mot-anonymised/index.html), [data catalogue](https://www.data.gov.uk/dataset/c63fca52-ae4c-4b75-bab5-8b4735e1a4c9/anonymised-mot-tests-and-results) | **P**. Public test records include odometers, vehicle attributes and test results; catalogue declares OGL | Candidate for annual mileage by age, not a complete fleet census or lifetime history. Check stable identifiers, exemptions, repeated tests, odometer units/errors and reporting breaks before linking |

ACEA means remain useful cross-checks, as already documented in
[evidence and references](evidence-and-references.md). They do not replace
these cohort tables.

## Electricity generation

| ID / source | Evidence and access | Limitation and acquisition action |
|---|---|---|
| S04 — [GEM Global Integrated Power Tracker](https://globalenergymonitor.org/projects/global-integrated-power-tracker) | **P**. Global unit/project capacity, technology, operational status, start and retirement fields; public download route | Technology-specific thresholds omit some small units. Current documentation lists wind ≥10 MW, hydro ≥30 MW, and operating solar above 1 MW. Obtain dated releases; inspect missing dates and distributed generation separately |
| S05 — [EIA-860](https://www.eia.gov/electricity/data/eia860/), [EIA-860M](https://www.eia.gov/electricity/data/eia860m/), [electricity data guide](https://www.eia.gov/electricity/data/guide/pdf/guide.pdf) | **P**. US generating-unit inventory, capacities, in-service dates and retirements; public files. Seek EIA-923 output joins through the guide | Facility threshold and changing historical coverage matter. Latest annual files do not retain every old retired plant; use historical releases and retirement lists. Check whether generation is generator- or plant-level |
| S06 — [German MaStR exports](https://www.marktstammdatenregister.de/mastrhilfe/subpages/datenexport.html), [public unit fields](https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/OeffentlicheEinheitenuebersicht) | **P**. Bulk/filtered unit data with commissioning dates, status, energy carrier and capacity | High-priority PV/wind source. Download a documented subset first; distinguish commissioning from registration and historical retirements from current operational stock. Check export terms and AC/DC fields |
| S07 — [Swiss SFOE generating plants](https://opendata.swiss/en/dataset/elektrizitatsproduktionsanlagen), [provider description](https://www.bfe.admin.ch/en/electricity-production-plants) | **P**. Public CSV/GPKG resources for operational plants registered in the guarantee-of-origin/support systems | Only operating plants; small voluntarily registered systems imply a coverage check. Retrieve schema to confirm date semantics and capacity before using as PV pilot. It does not establish past retired cohorts |
| S08 — [Danish Energy Agency wind register](https://ens.dk/en/analyses-and-statistics/overview-energy-sector) | **P**. Turbine technical data, grid connection/decommissioning dates and annual output; operating and decommissioned groups | Particularly useful for service-weighted cohorts. English page warns about production-data migration and links an older release. Resolve the current Danish release and freeze its date before treating it as contemporary evidence |
| S09 — [IRENA data](https://www.irena.org/data), [2026 capacity definitions](https://www.irena.org/-/media/Files/IRENA/Agency/Publication/2026/Mar/IRENA_DAT_RE_capacity_statistics_2026.pdf) | **P**. Country/technology capacity and generation series for totals and regional weighting | End-of-year installed capacity is not a gross-installation series. Differences omit replacements and retirements. Check unit basis and historical coverage; use as reconciliation evidence unless separate gross additions are obtained |

GEM advertises public downloads and a [Creative Commons licence](https://globalenergymonitor.org/creative-commons-license).
Archive the terms and attribution attached to each actual release: older and
tracker-specific releases may differ. No download registration was submitted
in this audit, and no access entitlement to commercial plant databases is assumed.

## Buildings and HVAC

| ID / source | Evidence and access | Limitation and acquisition action |
|---|---|---|
| S10 — [EU Building Stock Observatory](https://building-stock-observatory.energy.ec.europa.eu/database/), [indicator specification](https://energy.ec.europa.eu/document/download/c98de004-b2e1-418f-a742-1f61052ea242_en?filename=1_en_impact_assessment_part1_v3.pdf) | **P/L**. Public indicator portal; construction-period shares appear in the documented indicator framework | Current downloadable age series, population basis and country completeness remain to be established. Dashboard availability is not proof that every listed indicator is populated. Seek national census tables where absent |
| S11 — [Swiss FSO building stock by construction period](https://opendata.swiss/en/dataset/gebaude-nach-institutionellen-gliederungen-kanton-gemeinde-gebaudekategorie-energiequelle-der-h), [GWR field specification](https://www.housing-stat.ch/files/STAN_f_DEF_2022-06-18_eCH-0206_V2.0.0_Donnees_RegBL_aux_tiers.pdf) | **P**. Official building-stock tables; register specification distinguishes construction, renovation and demolition fields | Extract counts and floor-area evidence in compatible categories. Heating-energy source alongside building vintage is not heating-equipment vintage; a field's existence does not prove completeness or universal public access |
| S12 — [EIA RECS 2020](https://www.eia.gov/consumption/residential/data/2020/index.php?view=microdata) | **D**. Codebook downloaded: `EQUIPAGE`, `ACEQUIPAGE`, `WHEATAGE`, imputation flags and analysis/replicate weights. Public household microdata offered | Useful US residential HVAC age bands. Retrieve microdata and reproduce weighted tables; do not treat each household as one equally weighted unit. Consumption requires conversion before use as delivered heat/cooling |
| S13 — [EIA CBECS 2018](https://www.eia.gov/consumption/commercial/data/2018/index.php?view=microdata) | **P**. Public commercial-building microdata, codebook, floorspace/building characteristics and consumption | Strong building-stock lead. Inspect equipment-specific age fields before claiming HVAC-cohort coverage. Commercial-building sample does not represent all industrial facilities |
| S24 — [ASHRAE service-life database](https://costdatabase.ashrae.org/default.asp?c_class=1), [survival-method discussion](https://handbook.ashrae.org/Handbooks/A23/IP/A23_Ch38/A23_Ch38_ip.aspx) | **P**. Public queries distinguish equipment still operating from replaced equipment; download options and censored-survival discussion | Useful for selected HVAC, pumps and controls. It is a sample, not a national equipment census. Obtain observation dates and denominators, distinguish age-in-service from completed life, assess sample selection and verify redistribution terms |

## Networks and civil infrastructure

| ID / source | Evidence and access | Limitation and acquisition action |
|---|---|---|
| S14 — [Statistics Canada table 34-10-0289-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410028901), [survey instrument](https://www.statcan.gc.ca/en/statistical-programs/instrument/5173_Q12_V3) | **F**. Full public CSV downloaded and parsed: 2020/2022 observations, construction bands, unknown construction year, 127 asset labels including aggregates, counts/percentages | Covers public organisations and many asset types; quality flags and suppression need preservation. Confirm asset-specific physical units and avoid summing overlapping geography/owner/category totals. No component replacement history implied |
| S15 — [FHWA National Bridge Inventory tables](https://www.fhwa.dot.gov/bridge/britab.cfm), [field specification](https://www.fhwa.dot.gov/bridge/snbi/snbi_march_2022_publication.pdf) | **P**. Public bridge information by construction/reconstruction year and material; underlying annual inventory is the extraction target | Separate original build and rehabilitation; use area/traffic only when compatible with inventory service. Historic field changes and missing years need review; not a source for every tunnel or civil structure |
| S16 — [PHMSA decade inventories](https://www.phmsa.dot.gov/data-and-statistics/pipeline-replacement/decade-inventory), [annual source files](https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids) | **P**. US pipeline mileage by installation decade; public annual ZIPs with field descriptions, material/application data | Distinguish gas distribution, transmission and liquids; unknown dates and reporting changes matter. Does not describe drinking-water networks. Length-weighted age needs conversion if the LCI unit depends on size/material |
| S17 — [Ofgem RIIO-ED2 guidance, AP1](https://www.ofgem.gov.uk/sites/default/files/2021-10/ED2%20BPDT%20Guidance.pdf) | **P/L**. Guidance requires asset quantities by network-installation year and component class, plus life assumptions | Completed operator submissions, access, redactions and licence remain unverified. A blank regulatory template contains no observed fleet. Seek published distribution-company submissions before approving grid coverage |
| S18 — [ORR infrastructure portal](https://dataportal.orr.gov.uk/statistics/infrastructure-and-environment/rail-infrastructure-and-assets/), [track-age study](https://www.orr.gov.uk/media/27387/download), [component-age caveats](https://www.orr.gov.uk/sites/default/files/om/cr-Track_Service_Life_Proj_FinRepR3.pdf) | **P/L**. Public network totals; research documents use Network Rail track/sleeper/ballast ages | No reusable component-cohort extract obtained. Rolling-stock mean age is a different asset class; spot replacement can make section age misleading. Seek operator data or clearly scoped renewal-history reconstruction |
| S19 — [USACE navigation data framework](https://rsm.usace.army.mil/initiatives/r%26d/NDIF-Concept-091613.pdf) | **L**. Official documentation describes a lock-characteristics database and construction/replacement information | Current download endpoint, release and field availability unresolved. Use as a bounded search lead, not available global harbour-stock data; seek traffic linkage and distinct civil/electromechanical components |

## Industrial assets, ships, aircraft and charging

| ID / source | Evidence and access | Limitation and acquisition action |
|---|---|---|
| S20 — [GEM Global Iron and Steel Tracker](https://globalenergymonitor.org/projects/global-iron-steel-tracker) | **P**. Plant and furnace files include capacity, technology, status, age/detail and blast-furnace relining information | Documented plant threshold is 500,000 tonnes/year; generic machinery, rolling mills and small production are not comprehensively covered. Retrieve unit dates and distinguish relining from complete replacement |
| S21 — [UNCTAD maritime fleet age tables](https://unctad.org/system/files/official-document/rmt2024ch2_en.pdf) | **P**. Published ship-type age groups with both vessel-count and deadweight weights; worldwide seagoing fleet within stated scope | The 2024 report is a verified historical benchmark, not a claim to be the latest release. Open-ended oldest group and differing weights matter. Underlying Clarksons microdata access is not established; inland/small vessels need another source |
| S22 — [FAA releasable registry](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download), [BTS B-43 fields](https://www.bts.gov/topics/airlines-and-airports/number-288-electronic-submission-air-carrier-reports), [manufacture-year clarification](https://www.bts.gov/archive/subject_areas/airline_information/accounting_and_reporting_directives/number_319) | **P**. Public US registry and carrier inventory routes; carrier fields distinguish original manufacture from carrier acquisition and operating status | Verify actual download fields, deduplicate airframes and filter the service fleet. Carrier accounting life is not realised physical life; engines are different units. Global fleet and utilisation linkage remain open |
| S23 — [Bundesnetzagentur charging downloads](https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/DownloadundKontakt.html), [field/coverage FAQ](https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/FAQ/start.html) | **P**. Public German charging installations with commissioning dates; CSV/XLSX offered, CC BY 4.0 stated | Reporting is incomplete even for public chargers; private/home charging absent from the target population. Snapshot excludes future commissioning. Archive releases to study exits, and obtain charging throughput separately |

## Small equipment and replacement events

| ID / source | Evidence and access | Limitation and acquisition action |
|---|---|---|
| S25 — [EPA small water-system asset inventory guide](https://www.epa.gov/sites/default/files/2015-04/documents/epa816k03002.pdf) | **P**. Useful-life guidance for pumps, treatment, controls, electrical equipment and structures | Generic engineering estimates from an older guide, not population vintages. Use to structure operator requests and cautiously bound priors; do not transfer a small-water-system default to all machinery |
| S26 — [EPA UV disinfection guidance](https://www.epa.gov/system/files/documents/2022-10/ultraviolet-disinfection-guidance-manual-2006.pdf) | **P**. Lamp ageing, performance and maintenance context | Does not establish a national lamp-age histogram. Seek equipment-specific replacement criteria/hours and operating logs; separate lamp, sleeve, ballast and reactor |
| S27 — [DOE compressed-air resources](https://betterbuildingssolutioncenter.energy.gov/better-plants/compressed-air), [market assessment](https://www1.eere.energy.gov/manufacturing/tech_assistance/pdfs/newmarket5.pdf) | **P/L**. Operating-hours, assessment and maintenance leads; historic market survey documents use intensity | No representative vintage dataset for small compressors established. Check population/class fit and age of evidence; seek actual replacements and installations. Energy-audit software alone is not a stock database |

No primary public population-age source was established for replaceable filters
or generic electrical cabinets. Selected HVAC controls appear in S24, but that
does not cover entire cabinets or all industrial applications. Manufacturer
manuals and operator maintenance systems are next-stage searches, with no access
or purchase assumed.

## File-level spot checks

- **S01:** Germany 2024 API response has total 49,339,166 cars. The five returned
  age classes sum exactly to that total. This checks one extraction; it does not
  validate survival, imports or a within-band annual distribution.
- **S02:** Parsed the ODS data sheet and notes. It has observation dates, geography,
  body/fuel classes and first-use-year columns through 2025. Notes distinguish
  imported/unknown vehicles. A complete stock-total and missingness audit is
  still required.
- **S12:** Downloaded codebook version 7. Confirmed age-band variables for heating,
  cooling and water heating, imputation flags and survey weights. No microdata
  estimates were calculated.
- **S14:** Parsed 602,532 CSV records. These include combinations of totals,
  organisations, geographies, measures and missing/suppressed cells, not that many
  independent assets. The source contains a dedicated unknown-construction-year
  category. No national profile has yet been approved.

The local sample files remain in an untracked research directory; the committed
manifest records their source addresses, hashes and scope. Full upstream files
and personal records are not copied into the documentation tree.

## Other sources to assess after the pilots

National vehicle registries outside Europe, port/rail/utility asset-management
registers, equipment sales by class, original MFA supplements, and licensed fleet
databases may improve coverage. Availability at PSI has not been established.
Search those in response to a measured gap rather than assume access. Apparent
consumption from production/trade tables, economic depreciation tables and
product catalogues must retain their distinct meanings.

Actual local IAM scenarios are a separate, high-priority evidence stream. See
the [IAM scenario assessment](iam-scenario-assessment.md) for files, variables,
units, model semantics and feasible reconstruction routes. Scenario stocks and
gross additions can complement these historical observations; they cannot
retroactively identify the unknown initial cohorts by themselves.
