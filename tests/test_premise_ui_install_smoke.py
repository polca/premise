import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote
from urllib import error, request

import pytest


def _find_launcher() -> list[str] | None:
    launcher = shutil.which("premise-ui")
    if launcher:
        return [launcher]

    interpreter_dir = Path(sys.executable).resolve().parent
    for name in ("premise-ui", "premise-ui.exe"):
        candidate = interpreter_dir / name
        if candidate.exists():
            return [str(candidate)]

    return None


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _request_json(
    url: str, *, method: str = "GET", payload: dict | None = None
) -> dict:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=5.0) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"{method} {url} failed with {exc.code}: {body}") from exc


def _request_bytes(url: str) -> bytes:
    with request.urlopen(url, timeout=5.0) as response:
        return response.read()


def _wait_for_json(url: str, *, timeout_seconds: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            return _request_json(url)
        except Exception as exc:  # pragma: no cover - exercised only on startup races
            last_error = exc
            time.sleep(0.25)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Timed out waiting for {url}")


def _wait_for_run_complete(
    base_url: str, run_id: str, *, timeout_seconds: float = 30.0
) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict | None = None

    while time.monotonic() < deadline:
        payload = _request_json(f"{base_url}/api/jobs/{run_id}")
        last_payload = payload
        if payload.get("status") in {"completed", "failed", "cancelled"}:
            return payload
        time.sleep(0.25)

    raise AssertionError(f"Timed out waiting for run completion: {last_payload}")


def _usable_brightway_context(base_url: str) -> tuple[str, str] | None:
    payload = _request_json(
        f"{base_url}/api/discovery/brightway",
        method="POST",
        payload={},
    )

    def choose_database(names: list[str]) -> str | None:
        if not names:
            return None
        for name in names:
            if "biosphere" not in name.lower():
                return name
        return names[0]

    current_project = payload.get("current_project")
    current_db = choose_database(list(payload.get("databases", [])))
    if current_project and current_db:
        return str(current_project), str(current_db)

    for project_name in payload.get("projects", []):
        project_payload = _request_json(
            f"{base_url}/api/discovery/brightway/project",
            method="POST",
            payload={"project_name": project_name},
        )
        database_name = choose_database(list(project_payload.get("databases", [])))
        if project_payload.get("current_project") and database_name:
            return str(project_payload["current_project"]), str(database_name)

    return None


def _scenario_payload(base_url: str, environment_payload: dict) -> dict | None:
    local_payload = _request_json(f"{base_url}/api/discovery/iam-scenarios/local")
    scenarios = list(local_payload.get("scenarios", []))
    if scenarios:
        selected = scenarios[0]
        preview = _request_json(
            f"{base_url}/api/discovery/scenario-preview",
            method="POST",
            payload={"path": selected["path"]},
        )
        years = list(preview.get("years", []))
        year = years[0] if years else 2030
        return {
            "model": selected["model"],
            "pathway": selected["pathway"],
            "year": year,
            "filepath": str(Path(selected["path"]).parent),
        }

    if environment_payload.get("credentials", {}).get("IAM_FILES_KEY"):
        return {
            "model": "remind",
            "pathway": "SSP2-Base",
            "year": 2030,
        }

    return None


@pytest.mark.slow
def test_premise_ui_installed_launcher_dry_run_smoke(tmp_path):
    launcher = _find_launcher()
    if launcher is None:
        pytest.skip("`premise-ui` launcher is not installed in this environment.")

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    project_path = tmp_path / "smoke-config.json"
    export_dir = tmp_path / "exports"
    fake_ui_data_dir = tmp_path / "ui-data"
    fake_ui_data_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PREMISE_UI_DATA_DIR"] = str(fake_ui_data_dir)
    env.setdefault("PYTHONUNBUFFERED", "1")

    process = subprocess.Popen(
        [*launcher, "--no-browser", "--port", str(port)],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    captured_output = ""
    unexpected_returncode = None
    try:
        health = _wait_for_json(f"{base_url}/api/health")
        environment = _request_json(f"{base_url}/api/environment")
        capabilities = _request_json(f"{base_url}/api/capabilities")

        assert health["service"] == "premise-ui"
        assert environment["python_version"]
        assert environment["platform"]
        assert "credentials" in environment
        assert "iam_models" in capabilities

        brightway_context = _usable_brightway_context(base_url)
        if brightway_context is None:
            pytest.skip("No usable Brightway project/database pair is available.")
        source_project, source_db = brightway_context

        scenario = _scenario_payload(base_url, environment)
        if scenario is None:
            pytest.skip(
                "No local IAM scenario files are available and no IAM_FILES_KEY is configured."
            )

        template_payload = _request_json(
            f"{base_url}/api/projects/template?workflow=new_database"
        )
        project = template_payload["project"]
        project["project_name"] = "Installed launcher smoke"
        project["config"]["source_project"] = source_project
        project["config"]["source_db"] = source_db
        project["config"]["export"] = {
            "type": "matrices",
            "options": {"filepath": str(export_dir)},
        }
        project["scenario_sets"] = [{"name": "default", "scenarios": [scenario]}]

        saved = _request_json(
            f"{base_url}/api/projects/save",
            method="POST",
            payload={"path": str(project_path), "project": project},
        )
        opened = _request_json(
            f"{base_url}/api/projects/open",
            method="POST",
            payload={"path": str(project_path)},
        )
        queued = _request_json(
            f"{base_url}/api/jobs/enqueue-project",
            method="POST",
            payload={"path": str(project_path), "dry_run": True},
        )

        assert saved["path"] == str(project_path.resolve())
        assert opened["project"]["project_name"] == "Installed launcher smoke"
        assert queued["run_id"]

        final_status = _wait_for_run_complete(base_url, queued["run_id"])
        artifacts = _request_json(f"{base_url}/api/jobs/{queued['run_id']}/artifacts")
        history = _request_json(
            f"{base_url}/api/projects/history",
            method="POST",
            payload={"path": str(project_path)},
        )
        support_bundle = _request_bytes(
            f"{base_url}/api/jobs/{queued['run_id']}/support-bundle?project_path="
            f"{quote(str(project_path.resolve()), safe='')}"
        )

        assert final_status["status"] == "completed"
        assert any(
            event["event_type"] == "job_completed" for event in final_status["events"]
        )
        assert "diagnostics.json" in artifacts["artifacts"]
        assert "events.jsonl" in artifacts["artifacts"]
        assert history["run_history"][0]["run_id"] == queued["run_id"]
        assert support_bundle[:2] == b"PK"
        unexpected_returncode = process.poll()
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10.0)

        if process.stdout is not None:
            captured_output = process.stdout.read()

    assert unexpected_returncode is None, captured_output
