# Premise UI Install and Upgrade Guide

This document describes how to install, launch, upgrade, and support the
local browser-based GUI shipped with `premise`.

It is intended as the operational companion to the public GUI guide in
`docs/gui.rst` and the release-hardening notes in
`docs/ui/production-readiness-plan.md`.

## Scope

This guide covers:

- installing `premise` so that `premise-ui` is available;
- first launch and launch behavior;
- Brightway and IAM prerequisites;
- where configurations, run artifacts, recents, and credentials are stored;
- what happens when older saved configurations are reopened after an upgrade;
- the most common install and startup support checks.

## Installation

### Standard pip install

For normal end-user installation:

```bash
pip install premise
```

This installs the Python package, the GUI runtime, and the `premise-ui`
launcher.

If the target workflow needs Brightway 2 compatibility instead of the default
Brightway 2.5-oriented dependency set:

```bash
pip install "premise[bw2]"
```

### Conda packages

The conda-forge packages remain valid installation paths for users who prefer
conda-managed environments:

```bash
conda install -c conda-forge premise-bw25
```

or:

```bash
conda install -c conda-forge premise-bw2
```

After installation, activate that environment before launching `premise-ui`.

### Editable development install

For development work inside the repository:

```bash
pip install -e .
```

This exposes the same `premise-ui` launcher from the active development
environment.

## First Launch

After activating the environment where `premise` is installed:

```bash
premise-ui
```

This starts a small local web service and opens the browser automatically once
the app is reachable.

If you want to open the browser yourself:

```bash
premise-ui --no-browser
```

The launcher does not require the current working directory to be the
repository root. It can be started from any location as long as the correct
Python environment is active.

## Prerequisites

### Brightway workflows

If you use a Brightway source database or Brightway export target, the active
environment must also have a working Brightway installation and the expected
Brightway projects/databases available to that environment.

The GUI treats configurations and Brightway projects as separate things:

- a Premise configuration is a saved JSON file for the GUI;
- a Brightway project is the Brightway data space selected in the Source tab.

### Ecospold workflows

If you use an ecospold source, the GUI needs a real directory path to the
local ecospold dataset. Native file dialogs are used when available, but typed
path entry remains the fallback.

### IAM scenarios

The GUI can work with IAM scenario files that are already available locally, or
it can download the known bundled scenario set into Premise's IAM output
directory.

If encrypted IAM scenarios are used, the `IAM_FILES_KEY` decryption key must be
provided. The GUI exposes this through the `IAM Scenario Key` panel and can
remember it across sessions.

## Local Storage Model

### Saved configurations

Configurations are saved where the user chooses. They are ordinary JSON files
and are separate from Brightway projects.

### Run artifacts

When a configuration is saved to disk, run artifacts are stored beside it
under:

```text
<configuration directory>/.premise-ui/runs/<run_id>/
```

This keeps runs and support data portable with the saved configuration.

If a run is launched without a saved configuration path, the app falls back to
the user-level Premise UI data directory.

### User-level GUI state

Global UI state is stored under the platform-specific user data directory for
`premise-ui`.

This directory contains, at minimum:

- `recents.json`: recent configurations and recent paths;
- `runs/`: run data for unsaved configurations;
- `credentials.json`: only when keyring is unavailable and a file fallback is
  needed.

The exact base path is determined through `platformdirs` in
`premise_ui/core/paths.py`.

### Credentials

The GUI stores the IAM decryption key using this order of preference:

1. system keyring;
2. local file fallback in the Premise UI user data directory.

At runtime, the key is also loaded into the current process environment so that
Premise can use it during validation and execution.

## Upgrade Behavior

### Upgrading the package

Upgrade `premise` in the usual way for the installation method you use:

```bash
pip install --upgrade premise
```

or reinstall the desired conda package version.

For releases before `2.4.0`, users may need to reinstall or upgrade explicitly
to get the bundled GUI launcher and its runtime dependencies in the base
installation.

### Upgrading saved configurations

Saved GUI configurations include a `schema_version`.

When an older configuration is opened:

- the GUI migration layer upgrades it to the current supported schema;
- the loaded in-memory configuration is normalized to current defaults;
- the upgraded schema is written back the next time the configuration is saved.

This behavior is handled by `premise_ui/core/project_migrations.py`.

Current GUI schema support:

- current schema version: `1`
- older supported schema: `0`, migrated on load

If a configuration was saved by a newer GUI release than the one currently
installed, the older GUI will refuse to open it and show an explicit schema
version error instead of guessing.

### Browser refresh after upgrade

After upgrading the package, restart any running `premise-ui` process before
launching a new session. If a browser tab was already open, do a hard refresh
so the rebuilt frontend assets are reloaded.

## Common Support Checks

### `premise-ui` command not found

Check that:

- `premise` is installed in the active environment;
- the intended environment is activated;
- the installation completed without dependency errors.

### Environment panel stays empty

Check that:

- the local API is reachable;
- the active environment is the one that contains `premise`;
- startup diagnostics in the GUI do not show a failed bootstrap step.

### Brightway projects are missing

Check that:

- the active environment is the one that contains the Brightway installation;
- the expected Brightway projects exist in that environment;
- the GUI `Environment` panel does not report a Brightway discovery problem.

### Native file dialogs do not open

The GUI should remain usable even if native dialogs are unavailable. In that
case:

- the Environment panel reports dialog capability status;
- manual path entry remains available in the relevant tabs.

### IAM scenarios are missing

Check that:

- the local IAM output directory contains the expected files;
- the GUI can rescan them through the Scenario and Scenario Explorer views;
- the IAM decryption key is present when encrypted scenarios are required.

## Related Documents

- `docs/gui.rst`: public GUI usage guide
- `docs/ui/release-checklist.md`: manual release verification checklist
- `docs/ui/production-readiness-plan.md`: remaining readiness tracks and scope
