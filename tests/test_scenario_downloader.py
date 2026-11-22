import json
from hashlib import sha256
from io import BytesIO
from unittest.mock import MagicMock
from zipfile import ZipFile

import pytest
import requests

import premise.scenario_downloader as downloader
from premise.data_collection import IAMDataCollection
from premise.iam import vintage_downloader
from premise.iam.vintage_downloader import _find_vintage_package
from premise.scenario_downloader import (
    ZENODO_IAM_SCENARIO_RECORD_ID,
    get_scenario_file_stems,
    get_scenario_url,
)


def test_scenario_urls_use_current_zenodo_record():
    assert ZENODO_IAM_SCENARIO_RECORD_ID == "22227290"
    assert get_scenario_url("remind", "SSP2-NPi") == (
        "https://zenodo.org/records/22227290/files/remind_SSP2-NPi.csv"
    )


def test_image_scenario_url_uses_archive_filename_convention():
    assert get_scenario_url("image", "SSP2-VLHO") == (
        "https://zenodo.org/records/22227290/files/image_SSP2_VLHO.csv"
    )


def test_image_scenario_file_stems_support_both_naming_conventions():
    assert get_scenario_file_stems("image", "SSP2-VLHO") == (
        "image_SSP2-VLHO",
        "image_SSP2_VLHO",
    )
    assert get_scenario_file_stems("remind", "SSP2-NPi") == ("remind_SSP2-NPi",)


def test_iam_data_collection_reads_underscore_named_local_image_file(tmp_path):
    scenario_file = tmp_path / "image_SSP2_VLHO.csv"
    scenario_file.write_text(
        "Region,Variable,Unit,2020\nWEU,Example|Variable,EJ/yr,1\n",
        encoding="utf-8",
    )

    iam_data = object.__new__(IAMDataCollection)
    iam_data.model = "image"
    iam_data.pathway = "SSP2-VLHO"

    result = iam_data._IAMDataCollection__get_iam_data(
        key=None,
        filedir=tmp_path,
        variables=[],
    )

    assert result.sel(region="WEU", variables="Example|Variable", year=2020).item() == 1


def download_response(status=200, chunks=(b"complete scenario",)):
    response = MagicMock()
    response.__enter__.return_value = response
    response.status_code = status
    response.headers = {}
    response.iter_content.return_value = iter(chunks)
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(response=response)
    return response


def test_download_retries_gateway_timeout(monkeypatch, tmp_path):
    get = MagicMock(side_effect=[download_response(504), download_response()])
    monkeypatch.setattr(downloader.requests, "get", get)
    monkeypatch.setattr(downloader.time, "sleep", lambda _: None)

    path = downloader.download_csv("scenario.csv", "https://example.org", tmp_path)

    assert path.read_bytes() == b"complete scenario"
    assert get.call_count == 2
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("status, attempts", [(404, 1), (504, 4)])
def test_download_raises_http_error_without_cached_file(
    monkeypatch, tmp_path, status, attempts
):
    get = MagicMock(side_effect=lambda *a, **kw: download_response(status))
    monkeypatch.setattr(downloader.requests, "get", get)
    monkeypatch.setattr(downloader.time, "sleep", lambda _: None)

    with pytest.raises(requests.HTTPError):
        downloader.download_csv("scenario.csv", "https://example.org", tmp_path)

    assert get.call_count == attempts
    assert not list(tmp_path.iterdir())


def test_interrupted_download_does_not_cache_partial_data(monkeypatch, tmp_path):
    def interrupted_chunks():
        yield b"partial"
        raise requests.exceptions.ChunkedEncodingError("connection interrupted")

    get = MagicMock(
        side_effect=lambda *a, **kw: download_response(chunks=interrupted_chunks())
    )
    monkeypatch.setattr(downloader.requests, "get", get)
    monkeypatch.setattr(downloader.time, "sleep", lambda _: None)

    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        downloader.download_csv("scenario.csv", "https://example.org", tmp_path)

    assert get.call_count == 4
    assert not list(tmp_path.iterdir())


def test_download_preserves_existing_cache(monkeypatch, tmp_path):
    path = tmp_path / "scenario.csv"
    path.write_bytes(b"cached")
    get = MagicMock()
    monkeypatch.setattr(downloader.requests, "get", get)

    assert downloader.download_csv(path.name, "https://example.org", tmp_path) == path
    assert path.read_bytes() == b"cached"
    get.assert_not_called()


VINTAGE_FILENAME = "remind_SSP2-NPi_stock_vintages_v2.zip"


def vintage_zip(*, pathway="SSP2-NPi", corrupt=False):
    """Make a small ZIP with the file names and checksums of a v2 package."""
    resources = {
        "vintages.parquet": b"example vintage data",
        "validation.json": b'{"status": "pass"}',
    }
    manifest = {
        "profile": "iam-preprocessor-stock-vintages",
        "schema_version": 2,
        "model": "REMIND",
        "scenario": pathway,
        "resources": [
            {"path": name, "sha256": sha256(data).hexdigest()}
            for name, data in resources.items()
        ],
    }
    if corrupt:
        resources["vintages.parquet"] = b"changed vintage data"

    data = BytesIO()
    with ZipFile(data, "w") as archive:
        archive.writestr("datapackage.json", json.dumps(manifest))
        for name, content in resources.items():
            archive.writestr(name, content)
    return data.getvalue()


def test_vintage_package_prefers_internal_file(monkeypatch, tmp_path):
    monkeypatch.setattr(vintage_downloader, "IAM_VINTAGE_DIR", tmp_path)
    get = MagicMock()
    monkeypatch.setattr(downloader.requests, "get", get)
    path = tmp_path / VINTAGE_FILENAME
    path.write_bytes(vintage_zip())

    assert _find_vintage_package("remind", "SSP2-NPi", zenodo_record_id=12345) == path
    get.assert_not_called()


def test_vintage_package_missing_without_record_is_offline(monkeypatch, tmp_path):
    get = MagicMock()
    monkeypatch.setattr(downloader.requests, "get", get)
    monkeypatch.setattr(vintage_downloader, "IAM_VINTAGE_DIR", tmp_path)
    monkeypatch.setattr(vintage_downloader, "ZENODO_VINTAGE_RECORD_ID", None)

    assert _find_vintage_package("remind", "SSP2-NPi") is None
    with pytest.raises(FileNotFoundError, match=VINTAGE_FILENAME):
        _find_vintage_package("remind", "SSP2-NPi", local_dir=tmp_path)

    get.assert_not_called()


def test_vintage_package_uses_record_cache(monkeypatch, tmp_path):
    monkeypatch.setattr(vintage_downloader, "DIR_CACHED_FILES", tmp_path / "cache")
    data = vintage_zip()
    get = MagicMock(return_value=download_response(chunks=(data,)))
    monkeypatch.setattr(downloader.requests, "get", get)

    def resolve():
        return _find_vintage_package(
            "remind", "SSP2-NPi", local_dir=tmp_path, zenodo_record_id="12345"
        )

    path = resolve()
    assert path == tmp_path / "cache" / "iam_vintage_files" / "12345" / VINTAGE_FILENAME
    assert path.read_bytes() == data
    assert resolve() == path
    assert get.call_count == 1
    assert (
        get.call_args.args[0]
        == f"https://zenodo.org/records/12345/files/{VINTAGE_FILENAME}"
    )


@pytest.mark.parametrize(
    "data, match",
    [
        (vintage_zip(pathway="other"), "Expected remind/SSP2-NPi"),
        (vintage_zip(corrupt=True), "Checksum mismatch for vintages.parquet"),
        (b"incomplete ZIP", "File is not a zip file"),
    ],
)
def test_invalid_vintage_download_is_not_cached(monkeypatch, tmp_path, data, match):
    cache = tmp_path / "cache"
    monkeypatch.setattr(vintage_downloader, "DIR_CACHED_FILES", cache)
    get = MagicMock(return_value=download_response(chunks=(data,)))
    monkeypatch.setattr(downloader.requests, "get", get)

    with pytest.raises(ValueError, match=match):
        _find_vintage_package(
            "remind", "SSP2-NPi", local_dir=tmp_path, zenodo_record_id=12345
        )

    assert not list((cache / "iam_vintage_files" / "12345").iterdir())
