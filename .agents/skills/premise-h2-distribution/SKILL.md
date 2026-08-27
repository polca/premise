---
name: premise-h2-distribution
description: Engineer and evaluate the fork-specific hydrogen-distribution extension in premise_h2-distribution. Use for H2 final-energy aggregation, demand nodes, logistics rules, sector markets, consumer relinking, supporting inventories and reports, or the H2-distribution LCIA workflow. Use the separate premise skill for unrelated upstream polca/premise work.
---

# Premise H2 Distribution

Work on the hydrogen-distribution extension in the checkout named by the user.
Treat source and tests at that commit as authoritative. This fork contains much
of upstream `polca/premise`; do not turn a fork-specific assumption into a
general Premise rule or expand an H2 request into unrelated upstream cleanup.

## Ground the Task

For implementation, review, or debugging, run:

```powershell
python .agents/skills/premise-h2-distribution/scripts/inspect_h2_extension.py .
```

Use the repository's actual Python interpreter if `python` is not the intended
environment. The inspector reads source, YAML, and Git metadata without
importing Premise. It does not fetch remotes; do not fetch merely to use this
skill. Treat its warnings as review prompts, not automatic authorization to
change behavior; resolve them against source, tests, and documented intent.

## Route to Focused Guidance

- For code, decision-tree YAML, inventories, logging, docs, or tests, read
  [references/implementation.md](references/implementation.md).
- For Brightway LCIA, market comparison, contribution analysis, plots, or
  result-table reconciliation, read [references/lcia.md](references/lcia.md).
- For a task spanning both, read both references and validate the generated
  database before interpreting LCIA results.

For general `NewDatabase`, IAM mapping, export, installation, or upstream
Premise development that is not specific to this extension, use the separate
`premise` skill instead.

## Preserve the Feature Contract

- Trace the full affected path: normalized IAM data, demand classification,
  demand nodes, rule selection, market construction, consumer relinking,
  diagnostics, validation, and optional LCIA. Fix the earliest incorrect stage.
- Reuse the current model-specific hierarchy and `World` helper. Do not combine
  aggregate and detailed IAM variables or invent additional global-region
  aliases.
- Keep rule semantics aligned with the evaluator: lower numeric priority wins;
  bounds are `min_demand <= value < max_demand`; missing share is acceptable
  only for an explicitly documented onsite-production remainder.
- Generate sector-region markets only when their current prerequisites hold.
  A missing sector market can be a valid scenario result.
- Treat `Other` consumer routing as configured, narrow coverage—not a fallback
  for every unmatched consumer. Honor general-market and logistics exclusions.
- Preserve exchange identities and units. Keep database, transformation index,
  and cache consistent when activities or links change.
- Preserve fail-fast ordering for mandatory logistics, audit logging, market
  creation, and relinking. Never present a partial transformation as complete.
- Keep credentials, licensed ecoinvent data, Brightway projects, caches, and
  local scenario inputs out of committed artifacts.

## Verify Proportionally

Run the inspector after changing rule, routing, reporting, inventory, or wiring
contracts. Start behavior checks with:

```powershell
python -m pytest tests/test_hydrogen.py tests/test_fuels.py -q
```

Expand only to the shared or integration layers actually touched. Report tests
that could not run because the prepared Premise/Brightway environment,
licensed ecoinvent input, IAM data, time, or memory was unavailable.
