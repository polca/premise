# Premise UI Discovery

This folder is the working reference for the Premise GUI project.

## Purpose

The goal is to define the scope, constraints, and acceptance criteria for a
graphical user interface that can drive the main Premise workflows from an
interface instead of from ad hoc scripts.

## Files

- `discovery-questionnaire.md`: the full question bank used to clarify scope,
  users, workflows, constraints, and non-goals.
- `requirements.md`: the living product and technical requirements document.
- `decision-log.md`: the record of explicit choices, tradeoffs, and reversals.
- `architecture.md`: the proposed technical architecture for the GUI, including
  package boundaries, execution model, storage model, and implementation
  sequence.
- `scenario-explorer-plan.md`: the implementation plan for embedded IAM
  scenario exploration, comparison, and the later editing roadmap.
- `scenario-explorer-phase0.md`: the concrete build spec for the first
  Scenario Explorer implementation slice, including files, endpoints, and
  milestones.
- `production-readiness-plan.md`: the implementation plan for config
  migrations, installed-app smoke coverage, dialog/package validation,
  Scenario Explorer performance hardening, and accessibility work.
- `install-and-upgrade.md`: the operational guide for installing `premise-ui`,
  launching it, understanding local storage behavior, and handling upgrades of
  saved configurations.
- `release-checklist.md`: the manual release verification checklist for
  launcher behavior, dialogs, saved configurations, execution, artifacts,
  and Scenario Explorer sanity checks.

## Working Method

1. Answer the open questions in `discovery-questionnaire.md`.
2. Convert agreed answers into concrete requirements in `requirements.md`.
3. Record important choices in `decision-log.md`.
4. Translate the agreed requirements into implementation structure in
   `architecture.md`.
5. Plan major feature expansions in dedicated implementation documents such as
   `scenario-explorer-plan.md`.
6. Turn approved feature plans into implementation-ready build specs such as
   `scenario-explorer-phase0.md`.
7. Plan release-hardening work in dedicated readiness documents such as
   `production-readiness-plan.md`.
8. Add release-operational documentation such as `install-and-upgrade.md`
   before broadening distribution.
9. Revisit the questionnaire whenever a new unknown appears.

## Current Status

- Product discovery: questionnaire completed
- UX scope: draft requirements captured
- Technical architecture: proposed architecture documented
- Scenario explorer: proposed implementation plan documented
- Scenario explorer Phase 0: concrete build spec documented
- Production readiness: implementation plan documented
- Install and upgrade: operational guidance documented
- Delivery phases: draft priorities captured
