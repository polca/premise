# Approved data-driven regional material suppliers

BAFU provides complete regional-storage indium, but the shared metals lookup
accepts only World, GLO and RoW suppliers. The future material rule can therefore
skip a real available supplier. Creating a global wrapper would add a dataset
only to satisfy that location restriction.

The proposed change modifies one runtime file, `premise/materials/metals.py`.
It retains the existing global preference order and allows additional exact
locations only when a version-scoped provider alias explicitly lists them and
records limitations. The example mapping authorizes only the existing RER
indium supply. No arbitrary regional fallback is enabled; the existing platinum
restriction stays unchanged. Additional regional suppliers must have a positive
one-kg reference. The lookup warns when it selects such a supplier and records
its deterministic preference rank. Material equations, capacity conversions,
uncertainty rules, target years and global defaults are unchanged.

The review patch also contains the exact indium mapping declaration and one
regression test file covering fourteen scenarios. All 314 existing metals and
resource-boundary tests plus those fourteen checks pass in a private interpreter
preview. A real native CdTe record receives its projected indium through the
existing shared material subrule, with its documented 84 Wp / 0.72 m2 basis,
without creating a wrapper or other inventory. This is a geographic screening
proxy; source composition, trade, allocation, vintage and score differences
remain explicit limitations.

The user approved this exact runtime/mapping/test packet on 18 November 2026.
Its bytes are applied, and all 328 tests also pass against the repository source.
The public metals gate remains unchanged. Installed combined coverage and public
export remain separate requirements. Review `proposed-metals-regional-provider-with-tests.patch`
and `uvek-metals-regional-supplier-proposal-verification.json`.
