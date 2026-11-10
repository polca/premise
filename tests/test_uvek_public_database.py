"""Public pilot boundaries fail before source or scenario mutation."""

from unittest.mock import Mock

import pytest


def test_public_update_refuses_unverified_sector_before_mutation():
    from premise import NewDatabase

    owner = object.__new__(NewDatabase)
    owner.source_family = "uvek"
    owner.prepare_inventories = Mock(
        side_effect=AssertionError("Unexpected preparation")
    )
    with pytest.raises(ValueError, match="supports biomass, electricity and heat"):
        owner.update("cement")
    owner.prepare_inventories.assert_not_called()


def test_uvek_constructor_requires_the_supported_inventory_provider():
    from premise import NewDatabase

    with pytest.raises(ValueError, match="requires inventory_source='clic'"):
        NewDatabase([], source_family="uvek", source_version="2026")


def test_uvek_cache_separates_biosphere_database_and_source_revision(monkeypatch):
    import bw2data as bd
    from premise import NewDatabase
    from premise.database.background import resolve_source_background

    owner = object.__new__(NewDatabase)
    owner.source_family = "uvek"
    owner.background = resolve_source_background("2026", family="uvek")
    owner.source = "same-source"
    owner.biosphere_name = "canonical-biosphere310"
    owner.keep_source_db_uncertainty = True
    metadata = {"modified": "first"}
    monkeypatch.setattr(bd, "databases", {owner.source: metadata})
    original = owner._database_cache_path(owner.source)
    owner.biosphere_name = "other-biosphere310"
    assert owner._database_cache_path(owner.source) != original
    owner.biosphere_name = "canonical-biosphere310"
    metadata["modified"] = "second"
    assert owner._database_cache_path(owner.source) != original
