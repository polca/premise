import requests
import pytest

from premise.data_collection import IAMDataCollection
from premise.scenario_downloader import (
    ZENODO_IAM_SCENARIO_RECORD_ID,
    ZENODO_IAM_SCENARIOS,
    download_csv,
    get_scenario_cache_directory,
    get_scenario_file_stems,
    get_scenario_url,
    is_builtin_scenario,
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


def test_current_record_manifest_has_all_published_scenarios():
    assert sum(len(pathways) for pathways in ZENODO_IAM_SCENARIOS.values()) == 35
    assert ZENODO_IAM_SCENARIOS["tiam-ucl"] == {
        "SSP2-RCP19",
        "SSP2-RCP26",
        "SSP2-RCP45",
        "SSP2-RCP60",
    }
    assert is_builtin_scenario("message", "SSP2-M")


def test_tiam_base_is_not_a_builtin_scenario():
    with pytest.raises(ValueError, match="Use 'SSP2-RCP60' instead"):
        get_scenario_url("tiam-ucl", "SSP2-Base")


def test_tiam_rcp60_url_uses_current_record():
    assert get_scenario_url("tiam-ucl", "SSP2-RCP60") == (
        "https://zenodo.org/records/22227290/files/tiam-ucl_SSP2-RCP60.csv"
    )


def test_builtin_cache_directory_is_record_specific(tmp_path):
    assert get_scenario_cache_directory(tmp_path) == tmp_path / "zenodo-22227290"


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


class _Response:
    def __init__(self, chunks=(), status_code=200):
        self.chunks = chunks
        self.status_code = status_code
        self.headers = {"Content-Length": str(sum(len(chunk) for chunk in chunks))}
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")

    def iter_content(self, chunk_size):
        del chunk_size
        yield from self.chunks

    def close(self):
        self.closed = True


def test_download_csv_writes_atomically_and_reuses_completed_file(
    tmp_path, monkeypatch
):
    response = _Response([b"encrypted", b"-data"])
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return response

    monkeypatch.setattr("premise.scenario_downloader.requests.get", fake_get)
    cache_dir = get_scenario_cache_directory(tmp_path)

    result = download_csv("message_SSP2-M.csv", "https://example.test", cache_dir)
    assert result.read_bytes() == b"encrypted-data"
    assert not result.with_suffix(".csv.part").exists()
    assert response.closed

    assert download_csv(
        "message_SSP2-M.csv", "https://example.test", cache_dir
    ) == result
    assert len(calls) == 1


def test_failed_download_removes_partial_file(tmp_path, monkeypatch):
    response = _Response(status_code=404)
    monkeypatch.setattr(
        "premise.scenario_downloader.requests.get", lambda *args, **kwargs: response
    )

    with pytest.raises(requests.HTTPError):
        download_csv("missing.csv", "https://example.test", tmp_path)

    assert not (tmp_path / "missing.csv").exists()
    assert not (tmp_path / "missing.csv.part").exists()
    assert response.closed
