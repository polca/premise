"""Published slugs keep IAM roles stable across inventory-name revisions."""

from types import SimpleNamespace

import xarray as xr

from premise.inventory.activity_maps import InventorySet
from premise.inventory import clic_requirements

CSP_SLUG = "electricity-production-at-110-mw-concentrating-solar-power-plant-with-thermal-energy-storage"


def test_uvek_csp_root_is_selected_only_for_positive_iam_role():
    def scenario(amount):
        return {
            "model": "remind",
            "iam data": SimpleNamespace(
                electricity_mix=xr.DataArray(
                    [amount], dims="variables", coords={"variables": ["Solar CSP"]}
                )
            ),
        }

    # No matching legacy workbook is required for this approved family override.
    assert CSP_SLUG in clic_requirements.required_roots(
        ["electricity"], [scenario(1)], source_files=[], family="uvek"
    )
    assert CSP_SLUG not in clic_requirements.required_roots(
        ["electricity"], [scenario(0)], source_files=[], family="uvek"
    )
    assert CSP_SLUG not in clic_requirements.required_roots(
        ["electricity"], [scenario(1)], source_files=[]
    )


def test_family_override_replaces_legacy_binding_and_preserves_default(monkeypatch):
    overrides = clic_requirements.family_role_overrides("uvek")
    data = {
        "sector_mappings": {"electricity": ["electricity.yaml"]},
        "hybrid_sources": {
            "nodes": [
                {
                    "file": "old.xlsx",
                    "clic_slug": "legacy",
                    "identity": ["old", "old", "GLO", "kilowatt hour"],
                }
            ],
            "roles": {"electricity.yaml": {"Solar CSP": [0]}},
        },
    }
    monkeypatch.setattr(clic_requirements, "registry", lambda: data)
    monkeypatch.setattr(
        clic_requirements,
        "family_role_overrides",
        lambda family: overrides if family == "uvek" else {},
    )
    old = {"unit": "kilowatt hour", "clic provenance": {"slug": "legacy"}}
    new = {
        "unit": "kilowatt hour",
        "exchanges": [{"type": "production", "amount": 1, "unit": "kilowatt hour"}],
        "clic provenance": {"slug": CSP_SLUG},
    }
    sources = [("old.xlsx", "3.12")]
    clic_requirements.bind_activity_roles([old, new], sources)
    assert old["clic provenance"]["role_bindings"] == ["electricity.yaml:Solar CSP"]
    assert new["clic provenance"]["role_bindings"] == []
    bindings = clic_requirements.bind_activity_roles([old, new], sources, family="uvek")
    assert old["clic provenance"]["role_bindings"] == []
    assert bindings["electricity.yaml"]["Solar CSP"] == (CSP_SLUG,)
    provenance = new["clic provenance"]
    assert (
        "nine hours"
        in provenance["role_binding_limitations"]["electricity.yaml:Solar CSP"][0]
    )
    clic_requirements.bind_activity_roles([new], sources)
    assert "role_binding_limitations" not in provenance


def test_uvek_role_rejects_changed_csp_production_reference():
    import pytest
    from premise.inventory.clic_archive import ClicInventoryError

    for references in (
        [],
        [{"type": "production", "amount": -1}],
        [{"type": "production", "amount": 2}],
        [{"type": "production", "amount": 1, "unit": "megajoule"}],
        [{"type": "production", "amount": 1}] * 2,
    ):
        activity = {
            "unit": "kilowatt hour",
            "exchanges": references,
            "clic provenance": {"slug": CSP_SLUG},
        }
        with pytest.raises(ClicInventoryError, match="production reference changed"):
            clic_requirements.bind_activity_roles([activity], [], family="uvek")


def test_role_binding_rejects_unreviewed_reference_unit_change(monkeypatch):
    import pytest
    from premise.inventory.clic_archive import ClicInventoryError

    data = {
        "hybrid_sources": {
            "nodes": [
                {
                    "file": "source.xlsx",
                    "clic_slug": "dataset",
                    "identity": ["name", "product", "GLO", "kilogram"],
                }
            ],
            "roles": {"mapping.yaml": {"role": [0]}},
        }
    }
    monkeypatch.setattr(clic_requirements, "registry", lambda: data)
    activity = {"unit": "unit", "clic provenance": {"slug": "dataset"}}
    with pytest.raises(ClicInventoryError, match="reference unit changed"):
        clic_requirements.bind_activity_roles([activity], [("source.xlsx", "3.12")])
    activity["unit"] = "kilogram"
    clic_requirements.bind_activity_roles([activity], [("source.xlsx", "3.12")])
    assert activity["clic provenance"]["role_bindings"] == ["mapping.yaml:role"]


def test_zero_technologies_in_other_years_do_not_add_current_scenario_roots():
    iam = SimpleNamespace(
        electricity_mix=xr.DataArray(
            [[0, 1], [1, 0]],
            dims=("variables", "year"),
            coords={"variables": ["Coal", "Solar"], "year": [2030, 2050]},
        )
    )
    assert clic_requirements.applicable_roles(
        "electricity", {"Coal": {}, "Solar": {}}, {"iam data": iam, "year": 2050}
    ) == ("Coal",)


def test_target_source_profiles_exclude_replaced_legacy_public_roots(monkeypatch):
    data = {
        "schema_version": 1,
        "support": {},
        "sector_mappings": {"electricity": ["electricity.yaml"]},
        "mappings": {"electricity.yaml": {"Solar": ["legacy-solar", "current-solar"]}},
        "hybrid_sources": {
            "nodes": [
                {"file": "legacy.xlsx", "clic_slug": "legacy-solar"},
                {"file": "current.xlsx", "clic_slug": "current-solar"},
            ],
            "roles": {"electricity.yaml": {"Solar": [0, 1]}},
        },
    }
    monkeypatch.setattr(clic_requirements, "registry", lambda: data)
    import premise.inventory.activity_maps as maps

    monkeypatch.setattr(maps, "get_mapping", lambda *a, **k: {"Solar": {}})
    assert clic_requirements.required_roots(
        ["electricity"], source_files=[("current.xlsx", "3.12")]
    ) == ("current-solar",)


def test_required_older_upstream_does_not_reenter_primary_role_after_source_replacement():
    native = {
        "name": "solar",
        "reference product": "electricity",
        "location": "GLO",
        "unit": "kilowatt hour",
    }
    current = {
        **native,
        "name": "modern solar",
        "clic provenance": {
            "slug": "current",
            "role_bindings": ["electricity.yaml:Solar"],
        },
    }
    upstream = {**native, "clic provenance": {"slug": "old", "role_bindings": []}}
    result = InventorySet([native, current, upstream]).generate_sets_from_filters(
        {
            "Solar": {
                "fltr": "solar",
                "clic_slugs": ["current", "old"],
                "clic_role_key": "electricity.yaml:Solar",
            }
        }
    )
    assert result["Solar"] == [native, current]


def test_procedural_roles_follow_reviewed_aliases_through_namespacing():
    from premise.inventory.activity_maps import expand_inventory_name_mapping

    records = [
        {
            "name": "new capacity label [CLIC vanadium]",
            "clic provenance": {
                "slug": "vanadium",
                "name": "new capacity label",
                "legacy_identities": [
                    ["legacy vanadium capacity", "capacity", "GLO", "kilowatt hour"]
                ],
            },
        }
    ]
    result = expand_inventory_name_mapping(
        records, {"legacy vanadium capacity": "VRFB"}
    )
    assert result[records[0]["name"]] == "VRFB"


def test_role_uses_reviewed_slug_with_changed_name_and_preserves_native_filters():
    native = {
        "name": "original role",
        "reference product": "product",
        "location": "GLO",
        "unit": "kilogram",
    }
    published = {
        **native,
        "name": "new scientific dataset name",
        "clic provenance": {"slug": "reviewed"},
    }
    unrelated = {**native, "clic provenance": {"slug": "another-boundary"}}
    mapping = InventorySet([native, published, unrelated]).generate_sets_from_filters(
        {
            "role": {"fltr": "original role", "clic_slugs": ["reviewed"]},
        }
    )
    assert mapping["role"] == [native, published]


def test_iam_scope_selects_only_declared_technology_roots(monkeypatch):
    data = {
        "schema_version": 1,
        "support": {},
        "sector_mappings": {"electricity": ["electricity.yaml"]},
        "mappings": {"electricity.yaml": {"Solar": ["solar"], "Coal": ["coal"]}},
    }
    monkeypatch.setattr(clic_requirements, "registry", lambda: data)
    import premise.inventory.activity_maps as maps

    monkeypatch.setattr(maps, "get_mapping", lambda *a, **k: {"Solar": {}, "Coal": {}})
    iam = SimpleNamespace(
        electricity_mix=xr.DataArray(
            [1], dims="variables", coords={"variables": ["Solar"]}
        )
    )
    assert clic_requirements.required_roots(
        ["electricity"], [{"model": "image", "iam data": iam}]
    ) == ("solar",)


def test_multiple_scenarios_union_requirements_without_unrelated_sectors(monkeypatch):
    data = {
        "schema_version": 1,
        "support": {"fuels": ["pipe"]},
        "sector_mappings": {
            "electricity": ["electricity.yaml"],
            "fuels": ["fuels.yaml"],
        },
        "mappings": {
            "electricity.yaml": {"Solar": ["solar"], "Coal": ["coal"]},
            "fuels.yaml": {"Hydrogen": ["h2"]},
        },
    }
    monkeypatch.setattr(clic_requirements, "registry", lambda: data)
    import premise.inventory.activity_maps as maps

    monkeypatch.setattr(maps, "get_mapping", lambda *a, **k: {"Solar": {}, "Coal": {}})
    scenarios = [
        {
            "iam data": SimpleNamespace(
                electricity_mix=xr.DataArray(
                    [1], dims="variables", coords={"variables": [role]}
                )
            )
        }
        for role in ["Solar", "Coal"]
    ]
    assert clic_requirements.required_roots(["electricity"], scenarios) == (
        "coal",
        "solar",
    )
