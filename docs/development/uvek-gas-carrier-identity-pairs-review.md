# Gas carrier identity triples

The wider fuel regression run exposed a stale expectation from before the
reviewed volume-based gas helpers were added for steel: the default carrier
classification now intentionally includes those helpers, whereas fuel-market
supply remains restricted to its separate reviewed supplier declaration.

The same fixture reproduced a real filter weakness. Lists of names, products
and units independently admitted a volume-based helper labelled in MJ, or a
native MJ carrier labelled in cubic metres. Those combinations are not reviewed
and must not acquire a gas-carrier classification.

The natural-gas YAML now lists the six accepted name/product/unit triples.
The shared mapping builder accepts optional `exact_identities` to narrow its
existing filter results. Thirteen Python lines are necessary because the
existing YAML filter syntax cannot express these paired alternatives. Existing
filter declarations without this optional restriction keep their behavior;
the normal CLIC role-binding and family provenance rules remain in place.
No transformation equation, fuel amount, heating value, supplier recipe or
validation threshold changes.

The regression tests both list and indexed containers, rejects crossed products
and units, retains the approved helper and native carrier classification, checks
the separate market supplier selection and checks repeated mapping calls.
This does not independently qualify the entire fuels sector for public use.

Verification: 343 focused tests pass across activity maps, native mapping,
steel gas validation, gas-carrier composition, fuel background axes, handovers,
native vehicle references, fuel energy and carbon accounting. Ruff passes for
the changed Python module and test. These checks include ecoinvent filter and
background-axis regressions; they are not a licensed full ecoinvent build.
