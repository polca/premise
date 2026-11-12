# Steel prerequisite: delivered compressed air

## Finding

Rule `bp-61178cfdbac64a96` currently suggests a best-generation compressor
process for the ecoinvent 3.9 cut-off European market for compressed air at
600 kPa gauge. That candidate has the wrong delivery boundary: the source
producers include distribution infrastructure and leakage; the candidate ends
at the compressor. Matching pressure and cubic-metre units is insufficient.

## Independently inspected evidence

The source market has three equal shares of best, optimized and average
large-installation (>30 kW) generation. Their direct electricity inputs are
respectively 0.10111111111111111, 0.14 and 0.1985714285714286 kWh per delivered
cubic metre. The market-weighted direct electricity is approximately
0.1465608466 kWh/m³. These figures describe the inspected source, not an
additional multiplier to apply to a native complete process.

The installed `BAFU:2026-reimported` database contains exactly one European
`Compressed air, average installation, >30kW, 6 bar gauge, at supply network`
process. Its production reference is +1 cubic metre. Its documentation
explicitly includes the distribution network and leakage. It purchases
1.3 cubic metres of average-generation compressed air at the compressor and
includes steel, section-bar rolling and transport. The compressor consumes
0.139 kWh/m³, giving 0.1807 kWh of direct compressor electricity per delivered
cubic metre before upstream burdens. This is not equivalent to the source mix.

## Decision for implementation

Use the complete native **supply-network** process as a screening proxy for
this exact source profile, at factor +1, under the standing authorization for
documented proxies. Preserve caller quantities, signs and uncertainty.
Retain the native 1.3 coefficient literally. Do not add a second network,
additional loss correction or a newly constructed helper. Six bar gauge and
600 kPa gauge denote the same pressure; no speculative gas-state conversion
is justified by the inspected records.

Warnings must describe the replacement of the source's equal three-way
technology mix with the native average installation, the differing electricity
and leakage assumptions, infrastructure, geography, vintage and internal
uncertainty. The score difference remains unquantified. Both source and native
documentation recommend a more specific inventory when compressed air materially
affects the study result.

## Status and next checks

Primary review complete; **the migration rule and Premise approval have not yet
been changed**. Before enabling it: verify the exact packaged target catalog,
freeze the source-profile-scoped evidence, update the Brightpath resource and
integrity manifest, test signed quantities and uncertainty, and rerun strict
steel preparation. This finding does not qualify the steel sector or other
source versions.

Inspection used the regular Brightway project
`bafu-2026-ecospold2-biosphere-310`; it did not change inventories. Licensed
source vectors remain in the private review workspace.
