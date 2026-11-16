# Three source-3.7 car corrections

All three corrections now pass the actual shared car update, alongside the
other 13 locally qualified sectors. Both runs produce 390 regional services
and 13 markets, covering all 30 REMIND car roles. Exhaustive graph checks,
independent quantity/share checks and 86 signed LCIs per run pass. These checks
precede public package/export qualification; they are not an LCIA-equivalence
claim.

The AlLi market no longer uses the chemically different AlMg3 alloy. Three
recipes retain its original market, two separately allocated producers, both
producer shares and four freight entries. All six market entries and 29/50
producer inputs remain, including the direct water emissions. Twenty-six
native supplies were checked in both releases. Existing reviewed source-3.9
supply and treatment models replace their upstream inventories, with separate
caller/model provenance and 158 pinned recipe dependencies.

Titanium uses the existing complete titanium-metal reconstruction rather than
the incorrect cadmium candidate. Quebec electricity uses the complete Canadian
medium-voltage grid as an explicit geographic proxy. Native KCl is expressed
per kg K2O, representing a commercial salt with 60% K2O content: the 0.6 factor
is a commercial-grade mass proxy, with source purity/nutrient-label differences
retained as limitations. It does not establish chemical-purity or nutrient
equivalence. NaOH keeps its documented active-substance basis at factor one.
The source AlLi inventory is retained as documented; its name is not used to
invent a missing lithium-metal input or alloy composition.

Ten source formulas depend on missing parameters or source allocation context.
The evaluated signed amounts and marginal uncertainty are retained. Original
formula text is stored as audit metadata, and the available source parameters
remain with the helper. Executable internal parameterization and covariance
are not reproduced. Caller formulas remain unchanged. No new physical source
input is omitted. Seven existing child omission audits concern their original
model context and retain the exact prior authority. Verified production-only
cut-off stocks retain their allocation; no recovery credit is invented.

Delivered petrol now uses complete native Swiss fossil supply at regional
storage, including distribution and station/storage operations. The refinery-only
candidate and the European supply containing biomass ethanol are not used.
Geography, MTBE blend, sulfur specification, allocation and vintage remain
screening differences. CNG gains the missing exact source-3.7 positive-kg
decision; its existing native station already includes compression electricity,
losses and gas supply, so no extra compression stage is added.

Nine focused migration/replay checks and 88 Premise car/energy regressions pass.
Clean generation reproduces all three forward resources byte for byte. The
existing historical-model support required a minimal Brightpath validator
correction, recorded separately with 21 passing stock/profile checks. No
Premise transformation module changes are needed for these mappings. The
generator retains two pre-existing import-order lint findings; there are no
new findings in that file and the new maintenance/test files pass lint.

The shared IAM policy reports usable energy/service efficiency signals for
114 of the 390 services; the other 276 retain their baseline efficiency and
record the unavailable signal. The selected sources contain no battery
range-sizing contract. Their stock/service demands therefore follow the
existing legacy policy; capacity is not inferred from native battery kg.
These limitations are not silently treated as new future vehicle designs.
