from unittest.mock import MagicMock

import pytest
import requests

import premise.scenario_downloader as downloader
from premise.data_collection import IAMDataCollection
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
