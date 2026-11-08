"""UVEK preparation links the selected technosphere and canonical biosphere."""

from copy import deepcopy
from unittest.mock import Mock

import pytest

from premise.inventory.clic import ClicInventoryProvider
from premise.inventory.clic_archive import ClicInventoryError
from test_clic_archive import catalog, native, purchase


@pytest.fixture
def uvek_source():
    from brightpath import BiosphereProfile, TechnosphereProfile
    from brightpath.background import (
        BiosphereCatalog,
        InMemoryCatalogProvider,
        TechnosphereCatalog,
    )

    supplier = ("Native UVEK supplier", "Native UVEK supplier", "GLO", "kilogram")
    emission = ("Carbon dioxide, fossil", ("air",), "kilogram")
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                TechnosphereProfile("uvek", "2026", "cutoff"), frozenset({supplier})
            )
        ],
        biosphere=[
            BiosphereCatalog(
                BiosphereProfile("ecoinvent", "3.10"),
                frozenset({emission}),
                references={emission: "11111111-1111-4111-8111-111111111111"},
            )
        ],
    )
    root = native(
        1,
        1,
        "Foreground",
        exchanges=[
            purchase(
                supplier[0],
                amount=-3.5,
                formula="dose",
                **{"uncertainty type": 3, "loc": -3.5, "scale": 0.1},
            ),
            {
                "name": emission[0],
                "categories": ["air"],
                "unit": "kilogram",
                "type": "biosphere",
                "amount": 0.7,
            },
        ],
    )
    root.update(
        background_family="uvek", background_version="2026", biosphere_version="3.10"
    )
    root["inventory_snapshot"]["parameters"] = [{"name": "dose", "amount": -3.5}]
    destination = dict(zip(("name", "reference product", "location", "unit"), supplier))
    destination.update(database="actual-uvek", code="supplier-code")
    return provider, catalog(root), destination


def test_uvek_provider_uses_canonical_biosphere_and_actual_destination_keys(
    tmp_path, uvek_source
):
    provider, archive, destination = uvek_source
    before = deepcopy(archive.versions)
    client = ClicInventoryProvider(
        archive=archive,
        catalog_provider=provider,
        options={"cache_dir": tmp_path, "offline": True},
    )
    first = client.prepare(
        ["dataset-1"],
        [destination],
        family="uvek",
        version="2026",
        biosphere_name="actual-biosphere310",
    )
    root = first[0]
    assert root["exchanges"][1]["input"] == ("actual-uvek", "supplier-code")
    for field, value in {
        "amount": -3.5,
        "formula": "dose",
        "loc": -3.5,
        "scale": 0.1,
        "uncertainty type": 3,
    }.items():
        assert root["exchanges"][1][field] == value
    assert root["exchanges"][2]["input"] == (
        "actual-biosphere310",
        "11111111-1111-4111-8111-111111111111",
    )
    assert client.manifest["target"]["biosphere"] == {
        "family": "ecoinvent",
        "version": "3.10",
    }
    assert client.manifest["target"]["technosphere"]["family"] == "uvek"
    assert not client.manifest["cohorts"][0]["cache_hit"]
    repeated = client.prepare(
        ["dataset-1"],
        [destination],
        family="uvek",
        version="2026",
        biosphere_name="actual-biosphere310",
    )
    assert client.manifest["cohorts"][0]["cache_hit"]
    assert repeated == first
    assert archive.versions == before


def test_uvek_without_biosphere_identifiers_never_uses_an_ecoinvent_fallback(
    tmp_path, uvek_source
):
    from brightpath import BiosphereProfile
    from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider

    provider, archive, destination = uvek_source
    biosphere = provider.load_biosphere(BiosphereProfile("ecoinvent", "3.10"))
    provider = InMemoryCatalogProvider(
        technosphere=[
            provider.load_technosphere(p) for p in provider.technosphere_profiles()
        ],
        biosphere=[BiosphereCatalog(biosphere.profile, biosphere.identities)],
    )
    client = ClicInventoryProvider(
        archive=archive,
        catalog_provider=provider,
        options={"cache_dir": tmp_path, "offline": True},
    )
    with pytest.raises(ClicInventoryError, match="Biosphere flow is absent"):
        client.prepare(["dataset-1"], [destination], family="uvek", version="2026")


def test_uvek_cross_model_refusal_precedes_archive_access(tmp_path):
    client = ClicInventoryProvider(options={"cache_dir": tmp_path})
    client.get_archive = Mock(side_effect=AssertionError("Unexpected download"))
    with pytest.raises(ValueError, match="requires system_model='cutoff'"):
        client.prepare(
            [], [], family="uvek", version="2026", system_model="consequential"
        )
    client.get_archive.assert_not_called()
