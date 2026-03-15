"""Worker runner implementation for the Premise UI scaffold."""

from __future__ import annotations

import os
import platform
import sys
import traceback
from contextlib import contextmanager
from pathlib import Path

from premise_ui import __version__ as UI_VERSION
from premise_ui.core.adapters import WorkflowValidationError, get_workflow_adapter
from premise_ui.core.credentials import iam_key_state
from premise_ui.core.manifests import RunManifest
from premise_ui.core.premise_metadata import load_premise_version
from premise_ui.core.storage import read_json, write_json
from premise_ui.worker.events import EventWriter


def _metadata_payload(manifest: RunManifest, dry_run: bool) -> dict:
    return {
        "run_id": manifest.run_id,
        "workflow": manifest.workflow,
        "project_name": manifest.project_name,
        "dry_run": dry_run,
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "premise_version": load_premise_version(),
        "ui_version": UI_VERSION,
    }


@contextmanager
def _working_directory(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def run_manifest(manifest_path: str | Path, *, dry_run: bool = False) -> int:
    manifest_file = Path(manifest_path).expanduser().resolve()
    run_dir = manifest_file.parent
    manifest = RunManifest.from_dict(read_json(manifest_file))
    writer = EventWriter(run_dir, manifest.run_id)

    # Load remembered credentials into the worker runtime before validation/execution.
    iam_key_state()

    write_json(run_dir / "metadata.json", _metadata_payload(manifest, dry_run))

    writer.emit(
        "job_started",
        phase="bootstrap",
        message=f"Starting scaffold worker for workflow `{manifest.workflow}`.",
    )

    try:
        if dry_run:
            writer.emit(
                "phase_started",
                phase="dry_run",
                message="Running scaffold dry-run. No Premise workflow will execute.",
            )
            write_json(
                run_dir / "diagnostics.json",
                {
                    "detail": "Dry-run scaffold completed successfully.",
                    "manifest": manifest.to_dict(),
                },
            )
            writer.emit(
                "phase_completed",
                phase="dry_run",
                message="Dry-run scaffold completed.",
            )
            writer.emit(
                "job_completed",
                phase="finalize",
                message="Scaffold worker completed successfully.",
            )
            return 0

        adapter = get_workflow_adapter(manifest, writer=writer)

        with _working_directory(run_dir):
            writer.emit(
                "phase_started",
                phase="validation",
                message="Validating run manifest before execution.",
            )
            validation = adapter.validate()
            if validation.errors:
                raise WorkflowValidationError(validation.errors, validation.warnings)
            writer.emit(
                "phase_completed",
                phase="validation",
                message="Run manifest validation completed.",
                details={"warnings": validation.warnings},
            )

            result = adapter.execute()

        write_json(run_dir / "result.json", result)
        writer.emit(
            "job_completed",
            phase="finalize",
            message="Workflow execution completed successfully.",
            details=result,
        )
        if result.get("output_location"):
            writer.emit(
                "output_location",
                phase="output",
                message=str(result["output_location"]),
            )
        write_json(
            run_dir / "diagnostics.json",
            {
                "kind": "success",
                "message": "Workflow execution completed successfully.",
                "result": result,
            },
        )
        return 0
    except WorkflowValidationError as exc:
        write_json(
            run_dir / "diagnostics.json",
            {
                "kind": "validation_failed",
                "message": "Run manifest validation failed.",
                "errors": exc.errors,
                "warnings": exc.warnings,
            },
        )
        writer.emit(
            "job_failed",
            level="error",
            phase="validation",
            message="Run manifest validation failed.",
            details={"errors": exc.errors, "warnings": exc.warnings},
        )
        return 1
    except Exception as exc:
        write_json(
            run_dir / "diagnostics.json",
            {
                "kind": "execution_failed",
                "message": str(exc),
                "exception_type": type(exc).__name__,
                "traceback": traceback.format_exc(),
            },
        )
        writer.emit(
            "job_failed",
            level="error",
            phase="execution",
            message=str(exc),
            details={"exception_type": type(exc).__name__},
        )
        return 1
