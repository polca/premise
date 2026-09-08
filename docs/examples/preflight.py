"""Check first-scenario inputs without constructing or modifying a database."""

import os
from pathlib import Path


def validate_inputs(settings, projects, databases):
    """Pure checks; callers provide names from their existing installation."""
    required = (
        "PREMISE_BW_PROJECT",
        "PREMISE_SOURCE_DB",
        "PREMISE_BIOSPHERE",
        "PREMISE_KEY",
    )
    missing = [name for name in required if not settings.get(name)]
    if missing:
        raise ValueError("Set required environment variables: " + ", ".join(missing))
    if settings["PREMISE_BW_PROJECT"] not in projects:
        raise ValueError("Source project does not exist; select an existing project.")
    for variable in ("PREMISE_SOURCE_DB", "PREMISE_BIOSPHERE"):
        if settings[variable] not in databases:
            raise ValueError(
                f"Database named by {variable} is unavailable in this project."
            )
    output = settings.get("PREMISE_OUTPUT_DB", "docs-example-remind-SSP2-NPi-2025")
    if output in databases:
        raise ValueError("Output already exists; choose another PREMISE_OUTPUT_DB.")
    directory = settings.get("PREMISE_IAM_DIR")
    if directory and not Path(directory).is_dir():
        raise ValueError("PREMISE_IAM_DIR is not an existing directory.")
    return output


def main():
    import bw2data as bd

    settings = dict(os.environ)
    project_names = {project.name for project in bd.projects}
    project = settings.get("PREMISE_BW_PROJECT")
    if project not in project_names:
        raise ValueError("Set PREMISE_BW_PROJECT to an existing project.")
    previous = bd.projects.current
    try:
        bd.projects.set_current(project)
        output = validate_inputs(settings, project_names, set(bd.databases))
    finally:
        bd.projects.set_current(previous)
    print(f"Names and configuration checked; output available: {output}")
    print(
        "No database built. IAM decryption and complete linking are checked during construction."
    )


if __name__ == "__main__":
    main()
