from premise.data_collection import IAMDataCollection
from premise.scenario_downloader import (
    ZENODO_IAM_SCENARIO_RECORD_ID,
    get_scenario_file_stems,
    get_scenario_url,
)


def test_scenario_urls_use_current_zenodo_record():
    assert ZENODO_IAM_SCENARIO_RECORD_ID == "21790981"
    assert get_scenario_url("remind", "SSP2-NPi") == (
        "https://zenodo.org/records/21790981/files/remind_SSP2-NPi.csv"
    )


def test_image_scenario_url_uses_archive_filename_convention():
    assert get_scenario_url("image", "SSP2-VLHO") == (
        "https://zenodo.org/records/21790981/files/image_SSP2_VLHO.csv"
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
