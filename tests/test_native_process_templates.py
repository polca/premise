"""Whole native proxy templates preserve resource attribution and uncertainty."""

import copy
import math
from dataclasses import asdict
from pathlib import Path

import pytest
import yaml

from premise.inventory import replacement_quantities as module
from premise.inventory.clic_archive import ClicInventoryError, activity_identity
from premise.inventory.clic_hybrid import _workbook_background
from premise.inventory.clic_target import resolve_inventory_target
from test_native_replacement_quantities import activity


@pytest.fixture
def template(tmp_path, monkeypatch):
    donor = activity("native parent")
    donor["exchanges"] += [
        {
            "type": "technosphere",
            "name": "native energy",
            "product": "energy",
            "location": "RER",
            "unit": "megajoule",
            "amount": -2,
            "input": ("native", "energy"),
            "uncertainty type": 3,
            "loc": -2,
            "scale": 0.2,
            "minimum": -3,
            "maximum": -1,
        },
        {
            "type": "biosphere",
            "name": "CO2",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 4,
            "input": ("bio", "co2"),
            "uncertainty type": 2,
            "loc": math.log(4),
            "scale": 0.1,
        },
        {
            "type": "biosphere",
            "name": "Parent metal",
            "categories": ["natural resource", "in ground"],
            "unit": "kilogram",
            "amount": 2,
            "input": ("bio", "parent"),
        },
    ]
    record = activity("owned allocated metal")
    record["inventory provenance"] = {
        "file": "owned.xlsx",
        "sha256": "source-workbook-review",
        "provider": "retained workbook",
        "record_id": "synthetic",
    }
    record["exchanges"] += [
        {
            "type": "technosphere",
            "name": "source input",
            "product": "input",
            "unit": "kilogram",
            "location": "GLO",
            "amount": 17,
        },
        {
            "type": "biosphere",
            "name": "Owned metal",
            "categories": "natural resource::in ground",
            "unit": "kilogram",
            "amount": 1,
            "comment": "Owned resource attribution is kept at one kg.",
        },
    ]
    source = _workbook_background("3.9")
    target = asdict(resolve_inventory_target("2026", family="uvek"))
    recipe = {
        "id": "synthetic-native-template",
        "file": "owned.xlsx",
        "workbook_sha256": "source-workbook-review",
        "source": source,
        "target": target,
        "activity": list(activity_identity(record)),
        "donor": list(activity_identity(donor)),
        "factors": {"technosphere": 3, "biosphere": 5},
        "retain_source_biosphere": [
            {"source": module._selector(record["exchanges"][-1]), "amount": 1}
        ],
        "exclude_donor_biosphere": [module._selector(donor["exchanges"][-1])],
        "limitations": [
            "Synthetic whole-process allocation proxy; score effect unknown."
        ],
    }
    path = tmp_path / "templates.yaml"
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    monkeypatch.setattr(module, "TEMPLATE_PATH", path)
    return record, donor, source, target, recipe, path


def prepare(template):
    record, donor, source, target, _, _ = template
    return module.prepare_replacement_quantities([record], [donor], source, target)


def test_native_template_preserves_per_type_scaling_resource_and_complete_audit(
    template,
):
    record, donor, _, _, _, _ = template
    source = copy.deepcopy(record)
    native = copy.deepcopy(donor)
    with pytest.warns(UserWarning, match="Reviewed native process proxy"):
        notes = prepare(template)
    assert donor == native
    assert record["exchanges"][0] == source["exchanges"][0]
    assert record["exchanges"][-1] == source["exchanges"][-1]
    edge = record["exchanges"][1]
    assert edge["input"] == ("native", "energy") and edge[module.NATIVE_INPUT]
    assert edge["amount"] == edge["loc"] == -6
    assert edge["scale"] == pytest.approx(0.6)
    assert (edge["minimum"], edge["maximum"]) == (-9, -3)
    (emission,) = record[module.NATIVE_BIOSPHERE]
    assert emission["amount"] == 20
    assert emission["loc"] == pytest.approx(math.log(20))
    assert emission["scale"] == 0.1 and emission["uncertainty type"] == 2
    audit = record["inventory provenance"]["native process template"]
    assert audit["source_exchanges_replaced"] == source["exchanges"]
    assert audit["donor_exchanges_excluded"] == [native["exchanges"][-1]]
    assert audit["source_resources_retained"] == [source["exchanges"][-1]]
    assert notes[0]["code"] == "clic.retained_native_process_template"
    assert notes[0]["severity"] == "warning"
    prepared = copy.deepcopy(record)
    assert prepare(template) == []
    assert record == prepared


@pytest.mark.parametrize(
    "change", ["source version", "system model", "target", "owner"]
)
def test_template_never_spills_into_another_profile_or_owner(template, change):
    record, _, source, target, _, _ = template
    if change == "source version":
        source["biosphere"]["version"] = "3.8"
    elif change == "system model":
        source["technosphere"]["system_model"] = "consequential"
    elif change == "target":
        target["technosphere"]["version"] = "2025"
    else:
        record["reference product"] = "another metal"
    original = copy.deepcopy(record)
    assert prepare(template) == []
    assert record == original


@pytest.mark.parametrize(
    "change",
    [
        "workbook",
        "donor",
        "source reference",
        "donor reference",
        "factor",
        "formula",
        "resource",
        "exclusion",
    ],
)
def test_changed_physical_scope_is_refused(template, change):
    record, donor, _, _, recipe, path = template
    if change == "workbook":
        record["inventory provenance"]["sha256"] = "changed"
    elif change == "donor":
        donor["name"] = "another donor"
    elif change == "source reference":
        record["exchanges"][0]["amount"] = -1
    elif change == "donor reference":
        donor["exchanges"][0]["amount"] = 2
    elif change == "factor":
        recipe["factors"]["technosphere"] = -1
    elif change == "formula":
        record["exchanges"][1]["formula"] = "dose"
    elif change == "resource":
        record["exchanges"][-1]["amount"] = 2
    else:
        donor["exchanges"][-1]["categories"] = ["water"]
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    original = copy.deepcopy(record)
    with pytest.raises(ClicInventoryError):
        prepare(template)
    assert record == original


def test_existing_placeholder_reference_guard_is_not_relaxed(template):
    # The native template path does not turn a known nonzero source inventory
    # into an unknown placeholder. The existing complete-reference guard is intact.
    record, donor, _, _, _, _ = template
    with pytest.raises(ClicInventoryError, match="only unknown source inputs"):
        module._reference_supply(
            record,
            record["exchanges"][1],
            {
                "native": [module._selector(donor["exchanges"][1])],
                "limitation": "proxy",
            },
            [],
            donor,
        )


def test_seven_packaged_templates_have_explicit_allocation_and_basis_evidence():
    review = yaml.safe_load(
        (
            Path(module.__file__).resolve().parents[1]
            / "data/utils/import/native_process_templates.yaml"
        ).read_text()
    )
    assert review["schema_version"] == 1 and len(review["recipes"]) == 7
    recipes = review["recipes"]
    assert len({r["id"] for r in recipes}) == 7
    za = next(r for r in recipes if "ruthenium-za" in r["id"])
    assert za["factors"]["technosphere"] != za["factors"]["biosphere"]
    for r in recipes:
        assert r["target"]["technosphere"] == {
            "family": "uvek",
            "version": "2026",
            "system_model": "cutoff",
        }
        assert r["source"]["technosphere"]["system_model"] == "cutoff"
        assert r["retain_source_biosphere"][0]["amount"] == 1
        assert r["evidence"]["source_parent_technosphere_coordinates_verified"] > 0
        assert r["evidence"]["basis"] and r["limitations"]
        if "source_parent_metal_content" in r["evidence"]:
            expected = (
                r["evidence"]["source_parent_technosphere_factor"]
                * r["evidence"]["source_parent_metal_content"]
            )
            assert r["factors"]["technosphere"] == pytest.approx(expected)
            assert r["factors"]["biosphere"] == pytest.approx(expected)
