# Confirmed reference-output classifications

The user confirmed that the negative-reference cases discussed in the SimaPro
comparison are waste treatment. The earlier description of all 51 reference-output
differences as sign differences was incorrect: 30 are negative reference outputs,
while 21 are positive output quantities other than one.

Updated 21 existing activity/reference-product pairs in Premise's
`simapro_categories.csv`, covering the 30 confirmed scenario processes. These are
explicit reviewed categories consumed by both the legacy exporter and the Brightpath
comparison's unresolved-category fallback; no general inference rule was broadened.
ISIC folder paths can still be assigned independently.

The reviewed pairs cover clinker co-treatment, battery disassembly/recycling and
perovskite exhaust treatment. The 21 positive non-unit outputs retain ordinary-product
categories and their actual source quantities in Brightpath.

Verification through Brightpath checked all 30 reference outputs become +1 and
all 30 synthetic incoming links change from -3 to +3. It also checked that the
21 positive non-unit cases retain non-waste categories. This tests sign behavior;
it is not a regenerated full scenario export or LCIA comparison.

The user explicitly declined UUIDs in comments. No UUID-comment addition is required
or implemented. Brightpath's dedicated process identifiers remain in use.

Local per-dataset review details are in
`export/simapro-comparison/full-market-names/confirmed-reference-review.json`.
