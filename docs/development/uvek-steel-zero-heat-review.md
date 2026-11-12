# Recovered-heat allocation in retained steel dependencies

Thirty-six ecoinvent 3.9 cut-off heat correspondences now use the existing native UVEK heat-at-plant handover. Twelve source co-products contain production only; twenty-four heat-only forwarding paths terminate entirely in unburdened heat. Their combined share in the industrial heat prerequisite is 0.0073301763 MJ per MJ. No helper dataset is created.

This preserves an allocation boundary, rather than claiming identical incineration technology. Native UVEK assigns the treatment burdens to disposal and none to this recovered heat. District heating delivery, infrastructure and losses are outside this at-plant handover. The original source identities and the geographic and technology limitations remain in conversion provenance.

The approval covers positive-reference MJ heat only, from ecoinvent 3.9 cut-off. The twelve corresponding negative-reference kg waste-treatment services have nonempty inventories and are not covered. Two Swiss and rest-of-world heat forwarders contain nonempty internal-reuse inventories and remain excluded. Other releases and consequential profiles are not authorized.

## Verification

All 72 signed source LCI calculations are finite and exactly zero. Two signed native calculations are finite. An exact matrix check proves that the native production-only column meets its demand and has no biosphere burden. The numerical solver produces roundoff up to 1.232903015218289e-17; this is recorded without changing or rounding inventory exchanges.

Thirteen Brightpath tests pass, including both excluded forwarders, negative-kg waste services, wrong release/system model, signed quantities, uncertainty, and atomic replay guards. The Premise strict fresh/cache/approval-withdrawal test passes. Black and Ruff pass for the changed tests. The numerical receipt is `uvek-steel-zero-heat-lci-verification.json`.

Brightpath implementation: cd84445fb3c7e0736b752a8bbd0ff6ddf43a30a3; additional exclusion tests and installed dependency: 411467a16931aa69af7bcc1d1238b39316533510. Review resource SHA-256: 08c8c2b7811ae662c15ebcbe69096728ceeca5879d58ca4a3ffb9f3f3bf9c83c.

## Remaining work

These are leaf prerequisites, not approval of the complete industrial heat mix. Fifteen preparation-root correspondences still remain; steel transformation and export have not passed. The separate industrial heat allocation preflight examines fourteen nonzero co-products and grants no approvals. In particular, pulp heat includes a non-fossil carbon resource correction that cannot be discarded, and internal-reuse heat must not be treated as unburdened. No runtime Python module changes are included in this batch.
