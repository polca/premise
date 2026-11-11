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
and four positive recycled-supply entries, each 2.93285310007683e-14 kg/kg. Earlier
3.9 approval does not itself cover a different 3.8 inventory. If a new scoped
omission is used, all four entries need exact source/context evidence and an
explicit omission audit, with no renormalization, empty handover substitution or
claim of quantified score effects. Standing auto-approval covers a scientifically
supported similar decision; it does not eliminate evidence or validation.

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
