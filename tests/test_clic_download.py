"""Network/cache boundaries must not silently replace latest with stale data."""

import copy
import hashlib
import json

import pytest

from premise.inventory.clic_archive import ClicArchiveError
from premise.inventory.clic_download import (
    ClicArchiveStore,
    ClicSnapshotOutdated,
    verify_latest_versions,
)
from test_clic_archive import write_archive


class Response:
    def __init__(self, value):
        self.value = value

    def raise_for_status(self):
        return None

    def json(self):
        return copy.deepcopy(self.value)

    def iter_content(self, chunk_size):
        yield self.value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


class Session:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response(self.responses[url])


@pytest.fixture
def remote(tmp_path):
    payload = write_archive(tmp_path / "source.zip").read_bytes()
    record = {
        "id": 42,
        "metadata": {"doi": "10.5281/zenodo.42", "version": "today"},
        "files": [
            {
                "key": "inventory-archive.zip",
                "size": len(payload),
                "links": {"self": "https://zenodo.org/archive"},
            },
            {"key": "manifest.json", "links": {"self": "https://zenodo.org/manifest"}},
        ],
    }
    session = Session(
        {
            "https://zenodo.org/api/records/42/versions/latest": record,
            "https://zenodo.org/manifest": {
                "archive_sha256": hashlib.sha256(payload).hexdigest()
            },
            "https://zenodo.org/archive": payload,
        }
    )
    return session


def test_freshness_checks_do_not_repeat_archive_download(tmp_path, remote):
    store = ClicArchiveStore(tmp_path / "cache", session=remote)
    first = store.get(42)
    second = store.get(42)
    assert first.manifest["record_id"] == second.manifest["record_id"] == 42
    assert [url for url, _ in remote.calls].count("https://zenodo.org/archive") == 1
    assert [url for url, _ in remote.calls].count(
        "https://zenodo.org/api/records/42/versions/latest"
    ) == 2
    remote.responses.clear()
    assert store.get(42, offline=True).datasets[1]["slug"] == "dataset-1"


def test_public_archive_requests_suppress_shared_client_credentials(tmp_path, remote):
    ClicArchiveStore(tmp_path / "cache", session=remote).get(42)
    assert all(
        kwargs["headers"]["Authorization"] is None and kwargs["auth"] == ()
        for _, kwargs in remote.calls
    )


def test_latest_download_is_available_by_its_pinned_record_offline(tmp_path, remote):
    # The requested discovery record need not be the immutable record returned.
    response = remote.responses["https://zenodo.org/api/records/42/versions/latest"]
    response["id"] = 43
    response["metadata"]["doi"] = "10.5281/zenodo.43"
    store = ClicArchiveStore(tmp_path / "cache", session=remote)
    first = store.get(42)
    remote.responses.clear()
    count = len(remote.calls)
    replay = store.get(43, latest=False, offline=True)
    assert first.manifest["record_id"] == replay.manifest["record_id"] == 43
    assert replay.path == first.path
    assert len(remote.calls) == count
    with pytest.raises(ClicArchiveError, match="offline"):
        store.get(42, latest=False, offline=True)
    replay.path.write_bytes(b"damaged")
    with pytest.raises(ClicArchiveError, match="offline"):
        store.get(43, latest=False, offline=True)


@pytest.mark.parametrize("record_id", [None, "../escape", "not-a-record"])
def test_invalid_response_record_cannot_name_an_exact_cache_file(
    tmp_path, remote, record_id
):
    remote.responses["https://zenodo.org/api/records/42/versions/latest"][
        "id"
    ] = record_id
    with pytest.raises(ClicArchiveError, match="invalid record ID"):
        ClicArchiveStore(tmp_path / "cache", session=remote).get(42)
    assert not list((tmp_path / "cache").glob("*.zip"))


def test_exact_online_request_cannot_receive_another_record(tmp_path, remote):
    response = remote.responses["https://zenodo.org/api/records/42/versions/latest"]
    response["id"] = 43
    remote.responses["https://zenodo.org/api/records/42"] = response
    with pytest.raises(ClicArchiveError, match="different pinned record"):
        ClicArchiveStore(tmp_path / "cache", session=remote).get(42, latest=False)


def test_incomplete_or_corrupt_download_cannot_become_cached_snapshot(tmp_path, remote):
    remote.responses["https://zenodo.org/archive"] = b"truncated data"
    store = ClicArchiveStore(tmp_path / "cache", session=remote)
    with pytest.raises(ClicArchiveError, match="checksum or size"):
        store.get(42)
    assert not list((tmp_path / "cache").glob("*.zip"))
    with pytest.raises(ClicArchiveError, match="offline"):
        store.get(42, offline=True)


def test_cached_archive_is_verified_again_before_use(tmp_path, remote):
    store = ClicArchiveStore(tmp_path / "cache", session=remote)
    archive = store.get(42)
    archive.path.write_bytes(b"corrupt cached data")
    with pytest.raises(ClicArchiveError, match="offline"):
        store.get(42, offline=True)


def test_corrupt_managed_archive_is_redownloaded_online(tmp_path, remote):
    store = ClicArchiveStore(tmp_path / "cache", session=remote)
    first = store.get(42)
    first.path.write_bytes(b"damaged")
    repaired = store.get(42)
    assert repaired.datasets[1]["slug"] == "dataset-1"
    assert [url for url, _ in remote.calls].count("https://zenodo.org/archive") == 2


def test_live_check_uses_only_unpinned_selected_datasets():
    from types import SimpleNamespace

    resolved = SimpleNamespace(
        versions=[
            {
                "slug": "one",
                "version_id": 1,
                "semantic_version": "1.0.0",
                "pinned": False,
            },
            {
                "slug": "pinned",
                "version_id": 2,
                "semantic_version": "1.0.0",
                "pinned": True,
            },
        ]
    )
    url = "https://clic.psi.ch/api/datasets/one/"
    session = Session(
        {url: {"latest_published_version": {"id": 1, "semantic_version": "1.0.0"}}}
    )
    verify_latest_versions(resolved, session=session)
    assert [item[0] for item in session.calls] == [url]
    session.responses[url]["latest_published_version"] = {
        "id": 3,
        "semantic_version": "1.0.1",
    }
    with pytest.raises(ClicSnapshotOutdated, match="1.0.1"):
        verify_latest_versions(resolved, session=session)


def test_live_withdrawal_does_not_reuse_a_cached_published_version():
    from types import SimpleNamespace

    resolved = SimpleNamespace(
        versions=[
            {
                "slug": "one",
                "version_id": 1,
                "semantic_version": "1.0.0",
                "pinned": False,
            }
        ]
    )
    session = Session(
        {
            "https://clic.psi.ch/api/datasets/one/": {
                "status": "withdrawn",
                "latest_published_version": None,
            }
        }
    )
    with pytest.raises(ClicSnapshotOutdated, match="no longer publicly published"):
        verify_latest_versions(resolved, session=session)


def test_broad_scope_checks_latest_using_catalog_pages_without_inventory_downloads():
    from types import SimpleNamespace

    rows = [
        {"slug": str(i), "version_id": i, "semantic_version": "1.0.0", "pinned": False}
        for i in range(101)
    ]
    items = [
        {
            "slug": row["slug"],
            "latest_published_version": {
                "id": row["version_id"],
                "semantic_version": "1.0.0",
            },
        }
        for row in rows
    ]
    session = Session(
        {
            f"https://clic.psi.ch/api/datasets/?page_size=100&page={i + 1}": {
                "total_pages": 2,
                "results": items[i * 100 : (i + 1) * 100],
            }
            for i in range(2)
        }
    )
    verify_latest_versions(SimpleNamespace(versions=rows), session=session)
    assert len(session.calls) == 2
