# Declarative composition baselines

These reports are the immutable pre-refactor measurements for
`feature/declarative-sector-composition`. They were captured from Premise commit
`19fc6b44077de13639158a5ff02806e40cff0f24` with encrypted REMIND IAM data.

Runtime measurements include only `NewDatabase.update`; graph materialization
and hashing happen afterwards. Each comparison warms both modes, alternates
their measured order, and uses at least three repetitions.

The initial process-tree RSS sampler runs all repetitions in one Python process.
Python retains some materialized graph memory, so later absolute RSS readings
are an upper envelope rather than independent per-run peaks. Use the first
measured run of each mode as the pre-refactor absolute reference and replace
this limitation with isolated-process sampling before enforcing the final RSS
gate.

