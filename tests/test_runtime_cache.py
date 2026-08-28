from concurrent.futures import ThreadPoolExecutor

import xarray as xr

import premise.new_database as new_database_module
from premise.runtime_cache import (
    cache_iam_resource,
    clear_constructor_caches,
    get_cached_iam_resource,
    load_yaml_cached,
    runtime_cache_sizes,
    secret_fingerprint,
    stable_fingerprint,
)


def test_yaml_cache_is_thread_safe_and_invalidates_by_file_signature(tmp_path):
    path = tmp_path / "mapping.yaml"
    path.write_text("value: 1\n", encoding="utf-8")

    with ThreadPoolExecutor(max_workers=4) as executor:
        loaded = list(executor.map(lambda _: load_yaml_cached(path), range(16)))

    assert all(item == loaded[0] for item in loaded)
    assert loaded[0] == {"value": 1}
    loaded[0]["value"] = 99
    assert load_yaml_cached(path) == {"value": 1}
    path.write_text("value: 200\n", encoding="utf-8")
    assert load_yaml_cached(path) == {"value": 200}


def test_iam_cache_returns_isolated_state_and_runtime_clear_clears_all(tmp_path):
    clear_constructor_caches()
    path = tmp_path / "mapping.yaml"
    path.write_text("value: 1\n", encoding="utf-8")
    load_yaml_cached(path)
    resource = xr.DataArray([1.0, 2.0], dims=("year",), coords={"year": [2030, 2050]})
    cache_iam_resource(("resource",), resource)

    first = get_cached_iam_resource(("resource",))
    second = get_cached_iam_resource(("resource",))
    first.loc[{"year": 2030}] = 99

    assert second.sel(year=2030).item() == 1.0
    assert runtime_cache_sizes() == (1, 1)
    clear_constructor_caches()
    assert runtime_cache_sizes() == (0, 0)


def test_unstable_external_resource_bypasses_fingerprinting_and_secret_is_hashed():
    class UnstableResource:
        pass

    secret = "do-not-retain-this-key"
    fingerprint = secret_fingerprint(secret)

    assert stable_fingerprint(UnstableResource()) is None
    assert secret not in fingerprint
    assert len(fingerprint) == 64


def test_parallel_workbook_extraction_preserves_input_order(monkeypatch):
    calls = []

    def fake_importer(path):
        calls.append(path)
        return {"path": path}

    monkeypatch.setattr(new_database_module, "ExcelImporter", fake_importer)
    filepaths = [("third.xlsx", "3.8"), ("first.xlsx", "3.9"), ("second.xlsx", "3.10")]

    extracted = new_database_module._extract_default_inventory_importers(filepaths)

    assert extracted == [
        {"path": "third.xlsx"},
        {"path": "first.xlsx"},
        {"path": "second.xlsx"},
    ]
    assert set(calls) == {"third.xlsx", "first.xlsx", "second.xlsx"}
