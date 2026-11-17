# Transport inputs for the remaining recycling chains

This batch covers thirty external transport correspondences needed by the
source-3.8 recycled-HDPE and PET chains. It extends twenty-two road-freight
rules for source 3.8 and adds eight approvals of unchanged services and seventeen native mixture
helpers. Repeated source locations share a mixture; the existing EURO-V mixture
and Swiss rail mixture are reused. No runtime or generator Python is changed.

The road mixtures use complete diesel services matching the source EURO
standard. Native size/duty weights are conditioned on that standard and
normalized within the positive native subset. The caller's tonne-kilometres
remain unchanged. Electric vehicles, gas vehicles and other EURO standards are
excluded. Both native UVEK releases contain the same nominal subsets and weights.
The former >32t rest-of-world all-weight-classes candidate is corrected to the
32--40t class and EURO-IV diesel.

These mixtures still use historical Swiss-2020 fleet evidence, including under
RER labels. A 7.5t vehicle represents the source 3.5--7.5t band; 18/26/32t vehicles
represent 16--32t; 40t represents the open-ended >32t band and excludes heavier
vehicles. Geography, payload, empty running, duty, vintage and internal
uncertainty differ. No capacity, fuel, occupancy, distance, emissions or lifetime
multiplier is applied. Complete native vehicle, road, fuel, maintenance,
emissions and end-of-life burdens are included once.

The other services retain US diesel rail, the reviewed Swiss rail split, the
complete European rail and heavy-lorry proxies, container shipping and a Swiss
large diesel EURO-4 passenger car per vehicle-kilometre. The source car's stated
mass, passenger load and lifetime differ from the native design; no passenger-km
or occupancy conversion is invented. Numerical source/native impact-score and
stochastic equivalence have not been established.

Eleven older approved physical mappings retain their supplier, factor and recipe
contents. One previously global rule gains explicit sign metadata for its
already-approved 3.9 context; its older mixed-fleet choice remains unchanged.
Tests verify that it also remains usable when the new 3.8 approvals are withdrawn.

The Chinese rail component is documented for the required parent recycling
recipes only. Its shared global rule is unchanged, preserving the existing
dolomite dependency. The new decisions do not authorize a general source-3.8 CN
rail conversion. That component must be bound explicitly when the recycling
recipes are assembled.

Validation: 32 Brightpath checks, six Premise checks, sixty finite signed LCIs,
and thirty independent native-demand comparisons pass. The three normal
generator outputs match the packaged resources; both biosphere resources remain
unchanged. No Brightway database, registered activity or processed file is added.

The required-input diagnostic falls from 106 to 76 unapproved correspondences:
31 waste/infrastructure, 18 materials, 10 heat/fuel, nine water/wastewater,
seven electricity and the one parent-specific rail component. This is a
component diagnostic, not a complete truck-sector run or export. The two
top-level polymer mappings and the full truck sector remain unfinished;
fourteen of nineteen sectors remain qualified.
