"""Atomic caching and freshness checks for the public CLIC distribution."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import portalocker
import requests
from requests.adapters import HTTPAdapter
from tqdm.auto import tqdm
from urllib3.util.retry import Retry

from .clic_archive import ClicArchive, ClicArchiveError, ClicInventoryError

DEFAULT_CLIC_RECORD = "23935622"
DEFAULT_CLIC_API = "https://clic.psi.ch/api"


class ClicSnapshotOutdated(ClicInventoryError):
    """A successful latest-version build cannot use this older snapshot."""


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(data, stream, sort_keys=True, allow_nan=False)
        Path(name).replace(path)
    finally:
        Path(name).unlink(missing_ok=True)


def _session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "premise/CLIC-inventory-provider"
    return session


class ClicArchiveStore:
    """Cache verified archives; latest discovery does not redownload their data.

    Explicit record IDs can be frozen by lockfiles. Otherwise the Zenodo latest
    link is checked on each build. Offline use requires a previously verified
    local snapshot and an explicit choice by the caller.
    """

    def __init__(self, cache_dir, *, session=None, timeout=30):
        self.cache_dir = Path(cache_dir)
        self.session = session if session is not None else _session()
        self.timeout = timeout

    def _get(self, url, **kwargs):
        # A caller may share an authenticated CLIC client. Public Zenodo
        # downloads must never forward its bearer token or session auth.
        return self.session.get(url, headers={"Authorization": None}, auth=(), **kwargs)

    def get(
        self, record_id=DEFAULT_CLIC_RECORD, *, latest=True, offline=False
    ) -> ClicArchive:
        if not str(record_id).isdigit():
            raise ClicArchiveError("A Zenodo record ID must be numeric.")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        pointer = (
            self.cache_dir
            / f"record-{record_id}-{'latest' if latest else 'exact'}.json"
        )
        with portalocker.Lock(str(self.cache_dir / ".archive.lock"), timeout=60):
            if offline:
                try:
                    saved = json.loads(pointer.read_text(encoding="utf-8"))
                    archive = self._read_cached(saved)
                except (OSError, ValueError, KeyError) as error:
                    raise ClicArchiveError(
                        "No verified CLIC archive is cached for offline use."
                    ) from error
                return archive
            suffix = "/versions/latest" if latest else ""
            response = self._get(
                f"https://zenodo.org/api/records/{record_id}{suffix}",
                timeout=self.timeout,
            )
            response.raise_for_status()
            record = response.json()
            if not str(record.get("id", "")).isdigit():
                raise ClicArchiveError("Zenodo returned an invalid record ID.")
            if not latest and int(record["id"]) != int(record_id):
                raise ClicArchiveError("Zenodo returned a different pinned record.")
            files = {item["key"]: item for item in record["files"]}
            try:
                archive_file = files["inventory-archive.zip"]
                manifest_file = files["manifest.json"]
                response = self._get(
                    manifest_file["links"]["self"], timeout=self.timeout
                )
                response.raise_for_status()
                manifest = response.json()
                expected_sha = manifest["archive_sha256"]
            except (KeyError, ValueError) as error:
                raise ClicArchiveError(
                    "Zenodo record lacks a checksummed CLIC archive."
                ) from error
            if (
                not isinstance(expected_sha, str)
                or len(expected_sha) != 64
                or any(char not in "0123456789abcdef" for char in expected_sha)
            ):
                raise ClicArchiveError("Invalid archive SHA-256 in Zenodo manifest.")
            name = f"archive-{expected_sha}.zip"
            destination = self.cache_dir / name
            saved = {
                "filename": name,
                "sha256": expected_sha,
                "record_id": record["id"],
                "doi": record["metadata"].get("doi"),
                "record_version": record["metadata"].get("version"),
                "publication_date": record["metadata"].get("publication_date"),
            }
            if (
                destination.exists()
                and hashlib.sha256(destination.read_bytes()).hexdigest() != expected_sha
            ):
                destination.unlink()
            if not destination.exists():
                descriptor, temporary = tempfile.mkstemp(
                    prefix=".clic-download-", dir=self.cache_dir
                )
                os.close(descriptor)
                temporary = Path(temporary)
                try:
                    with self._get(
                        archive_file["links"]["self"], timeout=self.timeout, stream=True
                    ) as response:
                        response.raise_for_status()
                        digest = hashlib.sha256()
                        size = 0
                        with (
                            temporary.open("wb") as stream,
                            tqdm(
                                total=archive_file["size"],
                                desc="Downloading CLIC archive",
                                unit="B",
                                unit_scale=True,
                                unit_divisor=1024,
                            ) as progress,
                        ):
                            for block in response.iter_content(chunk_size=1024 * 1024):
                                if block:
                                    stream.write(block)
                                    digest.update(block)
                                    size += len(block)
                                    progress.update(len(block))
                        if (
                            digest.hexdigest() != expected_sha
                            or size != archive_file["size"]
                        ):
                            raise ClicArchiveError(
                                "Downloaded CLIC archive checksum or size does not match Zenodo."
                            )
                    ClicArchive.read(temporary)
                    temporary.replace(destination)
                finally:
                    temporary.unlink(missing_ok=True)
            archive = self._read_cached(saved)
            # A latest discovery may resolve a concept/older record to another
            # immutable record. Lockfiles pin that actual record, so make the
            # verified payload available for exact offline replay as well.
            exact_pointer = self.cache_dir / f"record-{saved['record_id']}-exact.json"
            _write_json(exact_pointer, saved)
            _write_json(pointer, saved)
            return archive

    def _read_cached(self, saved):
        name = saved["filename"]
        if Path(name).name != name:
            raise ClicArchiveError("Invalid cached CLIC archive filename.")
        path = self.cache_dir / name
        if hashlib.sha256(path.read_bytes()).hexdigest() != saved["sha256"]:
            raise ClicArchiveError("Cached CLIC archive checksum mismatch.")
        archive = ClicArchive.read(path)
        archive.manifest.update(
            record_id=saved["record_id"],
            doi=saved.get("doi"),
            record_version=saved.get("record_version"),
        )
        version = saved.get("record_version") or f"Zenodo record {saved['record_id']}"
        date = archive.manifest.get("created_at") or saved.get("publication_date")
        tqdm.write(f"CLIC archive: version {version}; date: {date or 'unavailable'}")
        return archive


def verify_latest_versions(
    resolved, *, session=None, base_url=DEFAULT_CLIC_API, timeout=30
):
    """Verify unpinned versions against live CLIC, never silently accept staleness.

    This transfers public metadata only. Inventory payloads stay in the archive
    cache. A newer publication absent from the daily backup stops the build so
    that a successful default build cannot falsely claim to use latest data.
    """
    rows = [row for row in resolved.versions if not row["pinned"]]
    if not rows:
        return
    session = session if session is not None else _session()

    # For broad builds use the paginated metadata catalog: hundreds of small
    # per-slug requests cost much more than a few catalog pages. No inventory
    # outside the selected closure is imported or harmonized.
    catalog = None
    if len(rows) > 100:
        root = f"{base_url.rstrip('/')}/datasets/"

        def page(number):
            response = session.get(
                f"{root}?page_size=100&page={number}", timeout=timeout
            )
            response.raise_for_status()
            return response.json()

        first = page(1)
        catalog = {item["slug"]: item for item in first["results"]}
        with ThreadPoolExecutor(max_workers=4) as executor:
            for payload in executor.map(page, range(2, first["total_pages"] + 1)):
                catalog.update((item["slug"], item) for item in payload["results"])

    def check(row):
        if catalog is None:
            url = f"{base_url.rstrip('/')}/datasets/{quote(row['slug'], safe='')}/"
            response = session.get(url, timeout=timeout)
            if getattr(response, "status_code", None) == 404:
                return {"slug": row["slug"], "reason": "no longer publicly published"}
            response.raise_for_status()
            dataset = response.json()
        else:
            dataset = catalog.get(row["slug"], {})
        latest = dataset.get("latest_published_version")
        if dataset.get("status", "published") != "published" or not latest:
            return {"slug": row["slug"], "reason": "no longer publicly published"}
        if (
            latest["id"] != row["version_id"]
            or latest["semantic_version"] != row["semantic_version"]
        ):
            return {
                "slug": row["slug"],
                "archived": row["semantic_version"],
                "live": latest["semantic_version"],
                "version_id": latest["id"],
            }
        return None

    with ThreadPoolExecutor(max_workers=min(4, len(rows))) as executor:
        changes = [result for result in executor.map(check, rows) if result is not None]
    if changes:
        raise ClicSnapshotOutdated(
            "CLIC has newer or withdrawn datasets than the selected archive. "
            "Refresh the daily archive or explicitly choose a pinned recipe; "
            f"no stale inventories were imported. Changes: {json.dumps(changes, sort_keys=True)}"
        )
