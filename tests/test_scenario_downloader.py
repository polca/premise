from premise.scenario_downloader import (
    ZENODO_IAM_SCENARIO_RECORD_ID,
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
