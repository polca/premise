# Shared transport background settings (15 November 2026)

The shipping callback was still constructing Transport as ecoinvent2026 when NewDatabase selected UVEK2026 with biosphere3.10. Native electricity product aliases therefore differed from the source validation baseline: a4,156-node cycle retained the same size but232activity keys differed. This produced a false new-cycle refusal. The trace also selected heat services, an electricity-network kilometre and methanol-powered vehicles through legacy fuel-name filters.

Pass the existing family/biosphere arguments from NewDatabase through the shared vehicle callback into BaseTransformation and InventorySet. Use the declared biosphere version for the existing vehicle carbon check. All six vehicle types use the same shared path. No transformation equations, regional-market logic, validation rules, thresholds, default ecoinvent behaviour or public UVEK sector gate are changed.

A small native-grid feedback fixture reproduces the product-face mismatch and verifies that UVEK transport now preserves the same canonical cycle signature and all coefficients. The ordinary transport mapping, emissions, PAH and battery-mass suites pass:258cases. The full shipping rerun and explicit native/fuel YAML filters are checked separately; shipping remains unqualified pending those checks and live verification.
