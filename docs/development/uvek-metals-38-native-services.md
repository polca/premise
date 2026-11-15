# Metals: six source-3.8 native services

Six exact ecoinvent 3.8 cut-off correspondences now use existing complete native
supplies. Source and native UVEK 2025/2026 references were independently checked:
each provides one positive unit. Callers retain signed quantities, uncertainty,
formulas and separate emissions at factor +1. No helper dataset is introduced.

| Source service | Complete native supply | Main limits |
| --- | --- | --- |
| Argentina high-voltage market | Argentina high-voltage grid | Generation/import mix, reference year, losses and network model |
| Chile high-voltage market | Chile high-voltage grid | Generation/import mix, reference year, losses and network model |
| Chinese high-voltage market group | China high-voltage grid | Subnational grouping replaced by the national model; mix, losses and vintage differ |
| RoW industrial natural-gas heat >100 kW | Swiss 1 MW industrial natural-gas heat | Geography, furnace size, operation and emissions; native excludes heat distribution |
| RoW delivered petroleum kerosene | European kerosene at regional storage | Refinery/delivery mix, losses, specification, geography and vintage |
| Global dry-bulk sea freight | Native transoceanic freight ship service | Vessel type/capacity, route, load, empty trips, fuel, ports and vintage |

These are screening correspondences with unquantified score differences.
Complete native inventories replace source internal mixes; no source grid loss,
delivery, ship, port or furnace burden is added on top. The shipping service is
per tonne-km, not a vessel manufacturing process. The native heat service is
industrial production heat, not a district network service. Electricity remains
eligible for ordinary Premise regional-market relinking.

The frozen evidence is Brightpath's
`export/uvek_metals_38_native_services_review.json`. It contains identities,
reference checks, metadata/inventory hashes and limitations, without distributing
complete licensed inventories. Premise appends six exact source-3.8 approval
scopes. Standing user auto-approval covers the documented screening decisions.
Other profiles and the ten still-refused source-3.8 mappings gain no permission.

Kerosene already had an exact source-3.9 sign review. Its independently checked
positive source-3.8 reference needs an additional explicit sign-profile entry;
an approval alone correctly fails without it. Brightpath adds this entry through
a frozen maintainer generator hook. The selected source-3.9 rule is unchanged,
including target, factor and existing warnings; its full-rule approval fingerprint
is refreshed after that comparison. No other previous scope, mapping or recipe
changes. The older authoring test restores its exact historical parent before
replay and still checks refusal of the newer parent and changed fuel boundaries.
No production Python module changes are needed for this group.

Verification: **49 Brightpath tests**, **23 Premise connector/previous-kerosene
tests**, and **12 signed LCI solves** pass. The LCI checks use a transient memory
datapackage and existing native suppliers; no Brightway database, registered
activity or processed file is added, and source/biosphere metadata stays intact.
The actual complete metals retry reduces this first source-3.8 refusal group
from 16 to **10**, with all conversion/validation gates intact. Metals is still
unfinished and remains outside the public supported-sector gate. This local
trial explicitly uses the published CLIC archive snapshot, not live freshness.

A clean tracked Brightpath checkout also regenerates all three migration
resources byte for byte. Its temporary checkout and generated outputs are
removed after comparison.

An independent read-only audit of all 11,947 native activities also finds a
separate unfinished requirement: `activities_mapping.yml` has no UVEK aliases
for any of the 17 metal-intensity technology roles. The current ecoinvent filters
match 28 records across only two roles; counts alone do not qualify those
matches. Explicit native aliases and the shared sector's family propagation
must be reviewed before an operational metals claim. This audit changes no
filters or sector code. See `uvek-metals-native-filter-audit.json`.
