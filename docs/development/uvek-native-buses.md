# Native bus services through the shared transport update

The four bus technologies used by REMIND already have complete BAFU 2026
transport services. Select those services directly instead of importing older
ecoinvent-based bus templates and converting their manufacturing supply chains.
The earlier legacy import stopped on 112 correspondence refusals in its first
failing cohort; those dependencies are unnecessary for these native services.

The mapping is in `premise/iam_variables_mapping/transport_bus.yaml`. Each exact
activity/reference-product pair supplies one positive passenger-kilometre:

| IAM role | Native Swiss service |
| --- | --- |
| Diesel | 2020 EURO-VI, 13 m single-deck urban bus |
| Compressed gas | 2020 EURO-VI, 13 m single-deck urban bus |
| Fuel cell | 2020 13 m single-deck urban bus, 700 bar SMR hydrogen supply |
| Battery electric | 2020 13 m single-deck urban bus, NMC-622, overnight charging |

These are explicit screening choices under the user's standing autonomous
approval, logged for later review. The source already allocates ten onboard
passengers. Do not divide by occupancy, convert vehicle-kilometres, or apply a
vehicle lifetime again. Keep the complete published service, including vehicle,
road, maintenance, wear and charging/fuelling infrastructure where present.
The battery service documents a 399 kWh pack. Its chemistry and charging pattern
are representative assumptions, not an IAM-derived battery mix. The fuel-cell
service retains its complete delivered hydrogen supply; do not add compression
or an electrolysis credit a second time. Geographic, fleet, operating pattern,
vintage and source inventory limitations remain, with unquantified score effects.

The ordinary `Transport` class performs regional copying, relative IAM
efficiency changes and fleet-market construction. Existing ecoinvent aliases,
IAM variables, units and calculations remain unchanged. The native fuel filter
recognises both the original low-voltage electricity supplier and the ordinary
Premise low-voltage markets: after an electricity update, geographic relinking
can select the latter before bus efficiency scaling. Medium-voltage supplies and
equipment are excluded. Diesel, gas and hydrogen retain their exact native
delivered-fuel interfaces.

One small shared annotation hook carries the YAML source limitations into dataset
comments before regional copying. It issues a warning when adding the comments,
is idempotent and changes no exchange quantities. This is needed because the
existing transport mapping machinery otherwise leaves those YAML notes behind
when exporting. The bus entry declares this mapping-file hook; the ecoinvent path
retains its existing behaviour.

The native generic bus fleet has no consumers elsewhere in the inspected source
database. No consumer redirection is therefore needed for that interface.
Historical fleet and trolleybus mixes are not re-labelled as the new generic IAM
fleet; users can select the new regional `market for transport, passenger bus`.

The bus-only diagnostic creates 52 regional services and 13 fleet markets,
imports no additional inventories and passes exhaustive graph checks. The five
new regression cases plus the existing transport selections pass 264 tests.
The combined eleven-sector update and export contain 16,256 activities, including
all 11,947 native source codes. All 52 bus efficiency checks preserve fixed
technosphere service totals through normal geographic supplier splitting. The
13 written fleet markets have normalised nonnegative shares; all 12 non-World
markets match every positive IAM bus role. All 130 signed LCI calculations pass.
The four original native bus fingerprints remain unchanged. A separate public
API run passes with default checkpoint persistence, no bypass, no disabled
validation, no additional inventories and no new Brightway database.

Buses are now enabled in the existing UVEK sector gate. The no-argument default
remains biomass, electricity and heat. Overall qualification is eleven of
nineteen sectors. Detailed evidence is in `uvek-native-buses-verification.json`.
The qualification uses the recorded archive snapshot; live freshness was not
checked. The source/native score difference is not established by finite LCI.
The private written-check script was corrected to pass numeric activity IDs to
Brightway repeat calculations; no production change or second export was needed.
Other IAM models' additional bus technologies are not established by this
REMIND scenario, and no cross-fuel proxy has been introduced for them.

Primary source inventory vectors and private scenario configuration remain
outside the repository. Published review evidence contains identities,
reference bases, source hashes and bounded verification results only.
