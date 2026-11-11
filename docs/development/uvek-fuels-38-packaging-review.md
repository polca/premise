# Pending fertiliser and pesticide packaging reconstruction

This is an inspected implementation plan, not a qualified mapping or an export
result. Both source-3.8 cases still block fuel preparation. The current battery
`Module packaging` target is physically wrong and has the wrong functional basis.

## Reference and inventory basis

The source production records explicitly reference packaging **per kg packed
product**. Their kg unit does not mean kg plastic. Fertiliser packaging requires
0.00204918032787 kg HDPE and the same mass of stretch blow moulding per reference
unit. Pesticide packaging requires 0.0583501006036 kg HDPE and the same mass of
injection moulding. Each production record also has eight signed polyethylene
waste-market entries. Each source packaging market has three freight entries
and one input to its corresponding production service.

The replacement must retain the source packed-product reference, the actual
plastic/forming amounts, all eight waste entries and all original market
freight. It must not use one kg of battery enclosure, one kg plastic, a density
factor or another packaging mass inferred from a name. Source market freight is
retained as recorded; it must not be scaled down by the small packaging-plastic
mass or claimed to reproduce an independently checked shipment boundary.

## Dependency inspection

Read-only inspection covers the source HDPE market and eight waste markets in
both 3.8 and 3.9: eighteen roots and 94 root/supplier witnesses. None of their
normalized root vectors is identical across those releases. Source-3.9 market
helpers therefore cannot be silently accepted as exact 3.8 reconstructions.

The eight waste entries refer to RER, Colombia, India, South Africa, Brazil,
Peru, Cyprus and rest-of-world. Their source references are negative kg. Actual
receiving routes include open burning, climate-specific open dumping,
climate-specific unsanitary landfill and municipal incineration; the RER group
also links regional child markets. These inventories are nonempty. For example,
the inspected open-burning receiver has 79 direct biosphere exchanges, while a
wet open dump has 108; an unsanitary landfill also has technosphere inputs.
They are not empty recycling handovers or a common interchangeable incinerator.

The source HDPE market has two virgin-production shares, three freight entries
and four positive recycled-supply entries, each 2.93285310007683e-14 kg/kg. The HDPE dependency now has independently inspected 3.8 and 3.11 delivery
profiles. The 3.8 profile omits exactly these four nonempty recycled entries with
source-specific audits and warnings, without renormalization or an empty
handover substitute. The inspected 3.11 market contains only two virgin
producers and three freight entries and has no omission. Older 3.9/3.10 selected
behavior and helper closures are preserved. See the
[HDPE dependency receipt](uvek-packaging-hdpe-preparation-verification.json).
This does not qualify either packaging service or its waste closure.

## Implementation and verification

Use Brightpath recipe and migration data, leaving Premise's shared transformations
unchanged. Derive the two packaging production services and two delivery wrappers
from their inspected quantities. Close each HDPE and waste dependency against
independently reviewed native suppliers or existing complete service recipes.
Where a later reviewed service inventory proxies the older inventory, explicitly
retain the source fate/climate and reference convention, document that inventory
vintage is replaced, and do not claim that its direct emissions are identical.
Do not redistribute raw licensed inventories as a shortcut to reconstruction.

Preserve all earlier selected profiles and their recipe closures. Refresh an
older full-rule fingerprint only after proving its selected behavior, evidence
and helper closure unchanged. Test exact signed amounts, uncertainty/formulas,
packed-product basis, disposal signs, retained shares/freight, native/biosphere
links, cached conversion and scope refusals. Compare the helpers' functional
LCIs and rerun actual licensed preparation. Missing dependencies remain
unfinished work; they do not justify omitting these packaging services.


## Newly identified endpoint mismatch

The selected existing 3.9 rules for polyethylene open burning and all five
open-dump climate classes still use municipal incineration. Their reference
signs were reviewed, but that does not establish the disposal technology. These
endpoints must not be reused as if they preserve uncontrolled disposal. The
issue also requires review of existing parent waste-market helpers that embed
those endpoints; a passed migration stage alone does not establish scientific
qualification of their fate representation.

Read-only comparison of complete 3.8 source vectors found that the reviewed
polypropylene open-burning and wet-open-dump inventories are not identical to
polyethylene: their moisture is 15.9% versus 0.4%, and numerous emission means
and uncertainty values differ. Same-climate unsanitary-landfill inventories
also differ from open dumping, including carbon emissions, dust and land use.
Neither can be silently relabelled as exact polyethylene open disposal.

Doka's [public original calculation tools](https://www.doka.ch/publications.htm)
provide the original open-burning and climate-specific dump models. The 2017
archive was downloaded and inspected read-only. It includes the PE/PP waste
composition library and separate burning/landfill formula workbooks. The cached
example outputs concern test PVC and mixed municipal waste, so they do not prove
a calculated PE inventory. No macros were run, no workbook or Brightway data was
modified, and no new PE environmental recipe is qualified from these examples.
The next modelling step is an independently verified public-model calculation
with the PE inputs and actual climate classes, preserving the emission and
water compartments. Raw licensed source inventories are not a publishing
shortcut. The HDPE stock/dependency work proceeds independently.

#### Public-model execution check

The native Excel automation probe has not produced a calculated PE inventory.
The first probe timed out; an instrumented retry stopped before reading any
model output because the opened central workbook could not be retrieved by
name. Both POSIX and HFS filename forms were tried. This isolates the current
failure to workbook opening/automation, rather than proving a numerical model
failure. After the probes, Excel reported no open workbooks and its original
`msoAutomationSecurityLow` setting was restored. No model output from these
attempts is accepted as mapping evidence. The public workbook copies remain
read-only; no new disposal mapping or omission was introduced.

#### Independent queue review: lime is not a single-product shortcut

Inspection of the pending 3.8 RoW lime market (`bp-42ca176032e4b754`)
found three distinct supplier branches plus three road-freight entries. The
market describes quicklime, but both inspected milling branches consume crushed
limestone without a calcination step; Quebec documentation explicitly describes
pulverized limestone. Another branch is an allocated zinc-mining co-product.
The current hydraulic-lime candidate therefore cannot be approved simply by
changing its name to quicklime. A reviewed delivery helper must preserve the
three branches and freight separately, establish the actual material basis for
each producer, and retain allocation and source-documentation ambiguity in its
warnings. No limestone-to-CaO mass multiplier or generic quicklime substitution
has been authorized by this inspection. This is independent of the packaging
disposal-model calculation and can be resolved in parallel with that work.

A further attempt using Excel's standard file-open command also failed. Excel
then returned errors for the settings query itself, and computer-use inspection
found no accessible Excel window. Consequently the post-attempt automation
setting could not be verified; the previously verified restoration above applies
only to the earlier probes. No restart or termination of Excel was attempted.

Independent native-feasibility calculations now pass for complete Swiss milled
limestone and the Swedish contained-zinc beneficiation process. The source lime
co-product's 263 nonproduction coordinates equal the source zinc concentrate
coordinates scaled by the frozen price ratio 0.117/0.38; 262 lognormal coordinates
also preserve scale/pedigree and shift loc by the logarithm of that ratio.
With the documented 59% zinc content, the preliminary native mining demand is
0.1816578947368421 kg contained-Zn burden per kg co-product. This is an allocation
proxy, not a chemical conversion or an added metal credit. Both native demands
produce finite supply and biosphere inventory arrays. The summary and explicit
limitations are in `uvek-fuels-38-lime-feasibility.json`; full source inventories
remain private. Market-rule implementation and full fuel qualification remain
unfinished.

The separate lime delivery correspondence is now implemented in Brightpath
`db351f745001919b59142371a92d5511aceafa2d`, with one exact-profile Premise approval.
All six original producer/freight rows survive unchanged statistically, and the
allocated-mining helper is separate from both limestone-grinding branches.
The 94 Brightpath mapping/adjacent tests and two Premise fresh/cache/removed-
approval tests pass. Full replay and wheel data match; flattened complete-market
native LCI is finite with the source database unchanged. See
`uvek-fuels-38-lime-verification.json`. This resolves the lime correspondence,
not the packaging disposal boundary or the complete fuel update.
