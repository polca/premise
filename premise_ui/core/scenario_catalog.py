"""IAM scenario catalog and local file helpers for the Premise UI."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from premise_ui.core.premise_metadata import load_premise_version

ZENODO_RECORD_ID = "18790143"
ZENODO_RECORD_API_URL = f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}"
SUPPORTED_DOWNLOAD_AVAILABILITY = {"bundled", "download-on-demand"}
SUPPORTED_SCENARIO_SUFFIXES = (".csv", ".mif", ".xls", ".xlsx")
_DOWNLOAD_JOBS: dict[str, dict[str, Any]] = {}
_DOWNLOAD_JOBS_LOCK = threading.Lock()


def _scenario_catalog_path() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "iam_scenarios.json"


def _premise_iam_output_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "premise" / "data" / "iam_output_files"


def load_iam_scenario_catalog() -> dict[str, Any]:
    with open(_scenario_catalog_path(), "r", encoding="utf-8") as handle:
        return json.load(handle)


def _request_headers() -> dict[str, str]:
    version = load_premise_version()
    return {"User-Agent": f"premise-ui/{version} (https://github.com/polca/premise)"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_scenario_name(path: Path) -> tuple[str, str] | None:
    if "_" not in path.stem:
        return None
    model, pathway = path.stem.split("_", 1)
    if not model or not pathway:
        return None
    return model, pathway


def _local_scenario_entry(path: Path, model: str, pathway: str) -> dict[str, str]:
    return {
        "id": f"{model}-{pathway}".lower(),
        "model": model,
        "pathway": pathway,
        "file_name": path.name,
        "path": str(path.resolve()),
    }


def _archive_scenario_entry(file_name: str, model: str, pathway: str) -> dict[str, Any]:
    return {
        "id": f"{model}-{pathway}".lower(),
        "model": model,
        "pathway": pathway,
        "label": f"{model} / {pathway}",
        "description": f"Available from the Premise IAM Zenodo archive as {file_name}.",
        "default_source_path_group": "premise-default",
        "availability": "download-on-demand",
        "bundled": False,
        "known_years": [],
        "archive_file_name": file_name,
    }


def _scenario_entry_from_archive_file_name(file_name: str) -> dict[str, Any] | None:
    path = Path(file_name)
    if path.suffix.lower() not in SUPPORTED_SCENARIO_SUFFIXES:
        return None

    parsed = _parse_scenario_name(path)
    if parsed is None:
        return None

    model, pathway = parsed
    return _archive_scenario_entry(file_name, model, pathway)


def fetch_zenodo_iam_scenario_catalog() -> dict[str, Any]:
    response = requests.get(
        ZENODO_RECORD_API_URL,
        timeout=60,
        headers=_request_headers(),
    )
    response.raise_for_status()
    payload = response.json()

    scenarios: dict[tuple[str, str], dict[str, Any]] = {}
    for file_entry in payload.get("files", []):
        file_name = file_entry.get("key") or file_entry.get("filename")
        if not file_name:
            continue
        scenario_entry = _scenario_entry_from_archive_file_name(str(file_name))
        if scenario_entry is None:
            continue
        key = (
            scenario_entry["model"].lower(),
            scenario_entry["pathway"].lower(),
        )
        scenarios[key] = scenario_entry

    return {
        "schema_version": 2,
        "updated_at": _utc_now_iso(),
        "record_id": str(payload.get("id") or ZENODO_RECORD_ID),
        "description": "Live IAM scenario catalog fetched from the Premise Zenodo archive.",
        "scenarios": sorted(
            scenarios.values(),
            key=lambda entry: (entry["model"].lower(), entry["pathway"].lower()),
        ),
    }


def load_downloadable_iam_scenario_catalog() -> dict[str, Any]:
    try:
        return fetch_zenodo_iam_scenario_catalog()
    except Exception:
        return load_iam_scenario_catalog()


def find_local_iam_scenario_file(
    model: str,
    pathway: str,
    scenario_dir: Path | None = None,
) -> Path | None:
    base_dir = scenario_dir or _premise_iam_output_dir()
    stem = f"{model}_{pathway}"
    for suffix in SUPPORTED_SCENARIO_SUFFIXES:
        candidate = base_dir / f"{stem}{suffix}"
        if candidate.exists():
            return candidate
    return None


def list_local_iam_scenarios() -> list[dict[str, str]]:
    scenario_dir = _premise_iam_output_dir()
    if not scenario_dir.exists():
        return []

    scenarios: dict[tuple[str, str], dict[str, str]] = {}
    for suffix in SUPPORTED_SCENARIO_SUFFIXES:
        for path in sorted(scenario_dir.glob(f"*{suffix}")):
            if not path.is_file():
                continue
            parsed = _parse_scenario_name(path)
            if parsed is None:
                continue
            model, pathway = parsed
            key = (model.lower(), pathway.lower())
            if key in scenarios:
                continue
            scenarios[key] = _local_scenario_entry(path, model, pathway)

    return sorted(
        scenarios.values(),
        key=lambda entry: (entry["model"].lower(), entry["pathway"].lower()),
    )


def _download_csv(
    file_name: str,
    url: str,
    download_folder: Path,
    progress_callback: Callable[[int, int], None] | None = None,
) -> Path:
    download_folder.mkdir(parents=True, exist_ok=True)
    file_path = download_folder / file_name

    if file_path.exists():
        if progress_callback is not None:
            progress_callback(1, 1)
        return file_path

    response = requests.get(url, stream=True, timeout=60, headers=_request_headers())
    response.raise_for_status()

    total_bytes = int(response.headers.get("Content-Length", "0") or "0")
    written_bytes = 0
    with open(file_path, "wb") as handle:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                handle.write(chunk)
                written_bytes += len(chunk)
                if progress_callback is not None and total_bytes > 0:
                    progress_callback(written_bytes, total_bytes)

    if progress_callback is not None and total_bytes <= 0:
        progress_callback(1, 1)

    return file_path


def _downloadable_catalog_entries(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in catalog.get("scenarios", []):
        availability = entry.get("availability")
        if availability and availability not in SUPPORTED_DOWNLOAD_AVAILABILITY:
            continue

        model = str(entry.get("model") or "").strip()
        pathway = str(entry.get("pathway") or "").strip()
        if not model or not pathway:
            continue

        file_name = str(
            entry.get("archive_file_name") or f"{model}_{pathway}.csv"
        ).strip()
        if not file_name:
            continue

        suffix = Path(file_name).suffix.lower()
        if suffix not in SUPPORTED_SCENARIO_SUFFIXES:
            continue

        key = (model.lower(), pathway.lower(), file_name.lower())
        if key in seen:
            continue
        seen.add(key)

        normalized_entry = dict(entry)
        normalized_entry["model"] = model
        normalized_entry["pathway"] = pathway
        normalized_entry["archive_file_name"] = file_name
        entries.append(normalized_entry)

    return entries


def _archive_file_url(file_name: str) -> str:
    return f"https://zenodo.org/records/{ZENODO_RECORD_ID}/files/{file_name}"


def _copy_download_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "directory": job["directory"],
        "started_at": job["started_at"],
        "updated_at": job["updated_at"],
        "total_count": job["total_count"],
        "processed_count": job["processed_count"],
        "progress": job["progress"],
        "current_file": job["current_file"],
        "current_file_progress": job["current_file_progress"],
        "downloaded": list(job["downloaded"]),
        "existing": list(job["existing"]),
        "failed": [dict(entry) for entry in job["failed"]],
        "scenarios": [dict(entry) for entry in job["scenarios"]],
    }


def _update_download_job(job_id: str, **patch: Any) -> dict[str, Any]:
    with _DOWNLOAD_JOBS_LOCK:
        job = _DOWNLOAD_JOBS[job_id]
        job.update(patch)
        job["updated_at"] = _utc_now_iso()
        return _copy_download_job(job)


def current_download_job() -> dict[str, Any] | None:
    with _DOWNLOAD_JOBS_LOCK:
        for job in _DOWNLOAD_JOBS.values():
            if job["status"] == "running":
                return _copy_download_job(job)
    return None


def get_download_all_known_iam_scenarios_status(job_id: str) -> dict[str, Any] | None:
    with _DOWNLOAD_JOBS_LOCK:
        job = _DOWNLOAD_JOBS.get(job_id)
        if job is None:
            return None
        return _copy_download_job(job)


def _progress_ratio(
    processed_count: int,
    total_count: int,
    current_fraction: float | None = None,
) -> float:
    if total_count <= 0:
        return 1.0
    value = processed_count / total_count
    if current_fraction is not None:
        value = (processed_count + current_fraction) / total_count
    return max(0.0, min(1.0, value))


def download_all_known_iam_scenarios() -> dict[str, Any]:
    scenario_dir = _premise_iam_output_dir()
    catalog = load_downloadable_iam_scenario_catalog()
    downloaded: list[str] = []
    existing: list[str] = []
    failed: list[dict[str, str]] = []

    for entry in _downloadable_catalog_entries(catalog):
        model = entry["model"]
        pathway = entry["pathway"]
        file_name = entry.get("archive_file_name") or f"{model}_{pathway}.csv"
        existing_file = find_local_iam_scenario_file(model, pathway, scenario_dir)
        if existing_file is not None:
            existing.append(existing_file.name)
            continue

        url = _archive_file_url(file_name)
        try:
            _download_csv(file_name, url, scenario_dir)
            downloaded.append(file_name)
        except Exception as exc:
            failed.append({"file_name": file_name, "error": str(exc)})

    local_scenarios = list_local_iam_scenarios()
    return {
        "directory": str(scenario_dir.resolve()),
        "downloaded": downloaded,
        "existing": existing,
        "failed": failed,
        "scenarios": local_scenarios,
    }


def _run_download_all_known_iam_scenarios_job(job_id: str) -> None:
    scenario_dir = _premise_iam_output_dir()
    scenario_dir.mkdir(parents=True, exist_ok=True)
    catalog = load_downloadable_iam_scenario_catalog()
    entries = _downloadable_catalog_entries(catalog)
    total_count = len(entries)

    _update_download_job(
        job_id,
        total_count=total_count,
        progress=_progress_ratio(0, total_count),
    )

    processed_count = 0
    downloaded: list[str] = []
    existing: list[str] = []
    failed: list[dict[str, str]] = []

    try:
        for entry in entries:
            model = entry["model"]
            pathway = entry["pathway"]
            file_name = entry.get("archive_file_name") or f"{model}_{pathway}.csv"
            _update_download_job(
                job_id,
                current_file=file_name,
                current_file_progress=0.0,
                downloaded=downloaded,
                existing=existing,
                failed=failed,
            )

            existing_file = find_local_iam_scenario_file(model, pathway, scenario_dir)
            if existing_file is not None:
                existing.append(existing_file.name)
                processed_count += 1
                _update_download_job(
                    job_id,
                    processed_count=processed_count,
                    progress=_progress_ratio(processed_count, total_count),
                    current_file=None,
                    current_file_progress=None,
                    existing=existing,
                )
                continue

            url = _archive_file_url(file_name)

            def on_progress(downloaded_bytes: int, total_bytes: int) -> None:
                fraction = None
                if total_bytes > 0:
                    fraction = downloaded_bytes / total_bytes
                _update_download_job(
                    job_id,
                    progress=_progress_ratio(processed_count, total_count, fraction),
                    current_file_progress=fraction,
                )

            try:
                _download_csv(
                    file_name,
                    url,
                    scenario_dir,
                    progress_callback=on_progress,
                )
                downloaded.append(file_name)
            except Exception as exc:
                failed.append({"file_name": file_name, "error": str(exc)})

            processed_count += 1
            _update_download_job(
                job_id,
                processed_count=processed_count,
                progress=_progress_ratio(processed_count, total_count),
                current_file=None,
                current_file_progress=None,
                downloaded=downloaded,
                existing=existing,
                failed=failed,
            )

        _update_download_job(
            job_id,
            status="completed",
            progress=1.0,
            current_file=None,
            current_file_progress=None,
            downloaded=downloaded,
            existing=existing,
            failed=failed,
            scenarios=list_local_iam_scenarios(),
        )
    except Exception as exc:
        _update_download_job(
            job_id,
            status="failed",
            current_file=None,
            current_file_progress=None,
            failed=failed + [{"file_name": "<job>", "error": str(exc)}],
            scenarios=list_local_iam_scenarios(),
        )


def start_download_all_known_iam_scenarios() -> dict[str, Any]:
    active_job = current_download_job()
    if active_job is not None:
        return active_job

    scenario_dir = _premise_iam_output_dir()
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "running",
        "directory": str(scenario_dir.resolve()),
        "started_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "total_count": 0,
        "processed_count": 0,
        "progress": 0.0,
        "current_file": None,
        "current_file_progress": None,
        "downloaded": [],
        "existing": [],
        "failed": [],
        "scenarios": list_local_iam_scenarios(),
    }
    with _DOWNLOAD_JOBS_LOCK:
        _DOWNLOAD_JOBS[job_id] = job

    thread = threading.Thread(
        target=_run_download_all_known_iam_scenarios_job,
        args=(job_id,),
        daemon=True,
        name=f"premise-ui-iam-download-{job_id[:8]}",
    )
    thread.start()
    return _copy_download_job(job)


def clear_local_iam_scenarios() -> dict[str, Any]:
    scenario_dir = _premise_iam_output_dir()
    removed: list[str] = []
    if scenario_dir.exists():
        for suffix in SUPPORTED_SCENARIO_SUFFIXES:
            for path in sorted(scenario_dir.glob(f"*{suffix}")):
                if not path.is_file():
                    continue
                path.unlink()
                removed.append(path.name)

    return {
        "directory": str(scenario_dir.resolve()),
        "removed": removed,
        "removed_count": len(removed),
        "scenarios": list_local_iam_scenarios(),
    }
