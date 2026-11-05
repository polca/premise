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
measured run of each mode as the pre-refactor absolute reference. The executable
gate compares that first-run measurement under the same warmup and ordering;
isolated-process sampling would strengthen the absolute-memory evidence further.

Run the executable comparison with the matching baseline artifact:

```bash
python benchmarks/check_factorization_mode.py \
  --sector electricity --sector emissions \
  --baseline benchmarks/baselines/remind-ssp2-npi-2050-electricity-emissions.json \
  --output export/declarative-composition/electricity-emissions-current.json
```

The gate enforces current standard runtime within 5% of the pre-refactor
standard, declarative runtime within 10% of that standard, current declarative
runtime within 10% of the alternating current standard, and peak process-tree
RSS within 20% of the pre-refactor compact standard. Ratios over 20% are also
reported explicitly as absolute runtime failures. Baseline scenario metadata
must match exactly; a two-sector artifact cannot certify the full-19 run.

