# Required truck inputs — 16 November 2026

The current truck import requires these 35 source-3.8 cut-off inputs. Each was
checked against the actual source and both native UVEK releases. Twenty-eight
cases need permission for an existing mapping; seven need a source-specific
mapping update. This does not approve unrelated catalog entries or conversions
between system models.

Twenty inputs supply materials or components. Complete native inventories
replace the source production/storage boundaries once. Their source producer
mixes, final delivery, allocation and uncertainty are not claimed preserved.
Named polymers and chemicals were checked for product and quantity basis: pure
ethylene glycol, K2CO3, acrylonitrile, R134a, solid PPS, rigid PUR and phenolic
resin use product kg without invented concentration or yield factors. The
source synthetic-rubber producer explicitly includes EPDM extrusion and
vulcanisation, as the native process does. Solder keeps its named alloy and
surface-mount resistors remain kg of components rather than component counts.
Historical metal supply and grade/recycling differences remain warnings.

Six inputs are forming or road services. Metal working and copper sheet rolling
exclude the bulk metal while including their own processing-loss material;
separate caller metal purchases remain. Road provision and maintenance stay
separate, per meter-year, with no added 100-year lifetime multiplier.

Eight waste/wear inputs have negative source references and positive native
service references. Factor minus one is applied exactly once to quantities,
formulas and uncertainty coordinates. The original caller sign is not replaced
by an absolute value. Whole-vehicle disposal uses the corresponding 16t, 28t
and 40t native services with their documented recycling cut-off boundaries.
Brake, road and tyre wear use their release inventories, without avoided-emission
credits or another load/GVW/distance conversion.

Two treatment matches needed correction. Lorry-production effluent now uses
the dedicated native class-1 lorry-effluent process rather than household sewage.
Reinforced-concrete waste now retains all three source treatment-route shares
and eight collection-freight entries. Its one new market helper uses the
corresponding native final-disposal, recycling and sorting services. The source
97% inert/3% steel composition matches their descriptions. No route is dropped
or normalized, and no extra demolition or recovered-steel credit is added.

The remaining input is heavy diesel Euro-VI freight. It reuses the existing
reviewed native 40t mix of long-haul, regional and urban services. Its historical
review concerned source 3.9; the same native model is independently approved
here for the inspected source-3.8 function. Native duty, load, fleet and vintage
differences remain explicit. No payload or empty-return factor is added.

Native screening limitations remain consequential for interpretation. Nickel
uses the historical 99.5% Class-I model, whose own documentation cautions against
relying on it for nickel-important studies; sensitivity or more detailed data
are needed for such results. Product performance, allocation, native emissions
coverage and score equivalence are unverified. Existing separately recorded
foreground exchanges remain in place.

All nine previously approved profiles affected by the seven rule changes select
the same full rule as before, and every previous recipe is unchanged. The normal
generator reproduces all three resources byte for byte. No runtime or generator
Python change is needed. Thirty Brightpath group/integrity checks, four Premise
group checks and eight previous Premise regressions pass. Existing runtime
checks for freight, materials and polymers also pass; historical authoring
fixtures are tested against their actual generator stage rather than later
operational data. A rail-related fixture failure reproduces on the published
baseline and requires no change to the production generator.

All 35 inputs pass finite positive/negative inventory calculations, 70 signed
solves in total, plus 35 native-demand comparisons within numerical tolerance.
These checks create no Brightway databases or registered activities. They test
the converted native model, not equivalence with the original source LCIA.

The actual truck preparation retry reduces its first source cohort from 84 to
49 pending rules. The truck update and export are still unfinished, and the
overall verified count remains 14 of 19 sectors. See the
[verification record](uvek-trucks-native-38-verification.json).

The installed packages pass another 26 group and integrity checks. Premise and
Brightpath runtime modules load from the isolated installed environment, with
packaged review/mapping bytes and the dependency pin matching the committed
checkouts. The ledger audit retains every previous operation: 35 are added and
only the nine recorded approvals for the five extended rules are refreshed.
The colleague notebook workflow is unchanged. Verified temporary resource
copies, superseded test wheels and isolated build output were cleaned; source
databases, current test artifacts and the last verified exports remain.
