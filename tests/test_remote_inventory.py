from pathlib import Path, PurePosixPath, PureWindowsPath
from types import SimpleNamespace

import pytest

import premise.inventory_imports as inventories


@pytest.fixture
def inventory_io(monkeypatch, tmp_path):
    for name in (
        "get_biosphere_code",
        "get_correspondence_bio_flows",
        "get_consequential_blacklist",
        "get_classifications",
    ):
        monkeypatch.setattr(inventories, name, lambda *args: {})

    monkeypatch.setattr(inventories, "TEMP_CSV_FILE", tmp_path / "temp.csv")
    monkeypatch.setattr(inventories, "TEMP_EXCEL_FILE", tmp_path / "temp.xlsx")
    calls = []

    def head(url):
        calls.append(("HEAD", url))
        return SimpleNamespace(status_code=200)

    def get(url, stream):
        calls.append(("GET", url))
        # Validate the URL without making a network request. This reproduces
        # requests' InvalidURL error for a Windows filesystem path.
        inventories.requests.Request("GET", url).prepare()
        assert stream is True
        return SimpleNamespace(
            raise_for_status=lambda: None,
            iter_content=lambda chunk_size: [b"downloaded workbook"],
            iter_lines=lambda: [b"Database,example"],
        )

    monkeypatch.setattr(inventories.requests, "head", head)
    monkeypatch.setattr(inventories.requests, "get", get)
    monkeypatch.setattr(
        inventories,
        "ExcelImporter",
        lambda path: SimpleNamespace(data=[], path=path, format="xlsx"),
    )
    monkeypatch.setattr(
        inventories,
        "CSVImporter",
        lambda path: SimpleNamespace(data=[], path=path, format="csv"),
    )
    return calls


def import_inventory(path):
    return inventories.AdditionalInventory(
        database=[],
        version_in="3.12",
        version_out="3.12",
        path=path,
        system_model="cutoff",
    )


@pytest.mark.parametrize("path_flavour", [PureWindowsPath, PurePosixPath])
@pytest.mark.parametrize(
    ("url", "file_format"),
    [
        ("https://raw.githubusercontent.com/example/repo/main/inventory.xlsx", "xlsx"),
        ("http://example.org/inventory.csv", "csv"),
        ("https://example.org/inventory.csv?download=1#inventory", "csv"),
        (
            "HTTPS://example.org/folder//inventory.xlsx?next=https://example.org//a",
            "xlsx",
        ),
    ],
)
def test_remote_inventory_preserves_url_on_both_path_flavours(
    monkeypatch, inventory_io, path_flavour, url, file_format
):
    # Only replace the importer's Path binding, so the Windows regression can
    # run on POSIX without changing pytest's own filesystem handling.
    monkeypatch.setattr(inventories, "Path", path_flavour)
    importer = import_inventory(url)

    assert importer.path == url
    assert inventory_io == [("HEAD", url), ("GET", url)]
    assert importer.import_db.format == file_format
    assert importer.import_db.path.suffix == f".{file_format}"
    expected = "downloaded workbook" if file_format == "xlsx" else "Database,example\n"
    assert importer.import_db.path.read_text() == expected


@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.parametrize("extension", ["xlsx", "csv"])
def test_local_inventory_with_http_in_its_name_does_not_use_network(
    inventory_io, tmp_path, path_type, extension
):
    path = tmp_path / f"http_inventory.{extension}"
    path.write_text("local inventory")
    importer = import_inventory(path_type(path))

    assert inventory_io == []
    assert importer.path == path
    assert importer.import_db.path == path
    assert importer.import_db.format == extension


def test_remote_inventory_missing_links_report_its_filename(inventory_io, capsys):
    importer = import_inventory("https://example.org/inventory.xlsx?download=1")
    assert importer.correct_product_field(("missing", "GLO", "kilogram", None)) is None
    assert importer.list_unlinked[-1][-1] == "inventory.xlsx"

    importer.import_db.data = [
        {
            "exchanges": [
                {
                    "name": "missing biosphere flow",
                    "categories": ("air",),
                    "unit": "kilogram",
                    "type": "biosphere",
                }
            ]
        }
    ]
    importer.add_biosphere_links()
    assert importer.import_db.data[0]["exchanges"] == []
    assert "in inventory.xlsx. Flow ignored." in capsys.readouterr().out
