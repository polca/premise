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

Implementation and scoped approval are now complete. The exact native target
exists in the packaged UVEK 2025 bridge catalog and the installed UVEK 2026
database. The source-scoped migration rule, frozen review, deterministic
maintainer replay and integrity manifest have been updated. This does not
qualify the complete steel sector or other source versions.

Inspection used the regular Brightway project
`bafu-2026-ecospold2-biosphere-310`; it did not change inventories. Licensed
source vectors remain in the private review workspace.


## Implementation evidence

Brightpath commit `3bc23eec91b9363091e7bad4ab7d6dd6db4f708d` is pushed and
installed from GitHub in the regular conda environment. The frozen review is
`export/uvek_steel_39_air_review.json`, SHA256
`007378f77d3801d4e9e402b226fa4bb77f7a7d86ec88cf7c6fe619a2cf7df9f4`.
The rule uses the complete native network supplier directly; no helper is added.
Runtime Python modules remain unchanged. Python edits are maintainer replay
scripts, their generator hook and tests.

Seven Brightpath checks pass, covering positive/negative demand, normal and
lognormal uncertainty, formulas, immutable foreground input, refusal for an
unreviewed source release, replay idempotence and atomic parent-drift refusal.
The Premise strict fresh/cache/approval-withdrawal test passes (17.54 seconds).
Black and Ruff pass. The native network supplier passes two finite signed LCIs
with exact sign reversal; the 11,947-activity source and modification metadata
remain unchanged.

The actual encrypted REMIND SSP2-NPi 2050 diagnostic steel preparation now has
**37 pending correspondences, down from 38**. Strict migration validation stayed
enabled; only the steel support guard was bypassed. It still stops at inert-waste
final disposal, so no successful steel transformation or export is claimed.
Private audit receipt:
`d5db54834999209161029b2611256e0d8c4397f19ddc0db6d8ffcb1ea7499614.audit.json.gz`.

## Following waste-treatment review

The remaining Swiss inert-waste market has a negative kg reference, whereas
its suggested native construction-waste disposal has a positive reference and
little descriptive metadata. The source treatment explicitly contains generic
inert-landfill operation and infrastructure, without direct leachate emissions.
That candidate is not approved merely because it is a disposal process.

The two pig-iron wastewater markets also have negative references. Their source
model places direct emissions and sewering in the market, separate from treatment.
A direct substitution with the native class-3 treatment alone could drop those
burdens. Review the complete market vectors and treatment boundary before
approving or reconstructing these correspondences; no new omission is authorized
by this review.
