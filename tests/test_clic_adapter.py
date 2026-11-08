"""Destination links must preserve quantities and select unique real suppliers."""

import copy
from types import SimpleNamespace

import pytest

from premise.inventory.clic_adapter import prepare_clic_for_premise
from premise.inventory.clic_archive import ClicInventoryError
from test_clic_archive import catalog, native, purchase


def background(name="background", *, code="native", unit="kilogram"):
    return {
        "name": name,
        "reference product": name,
        "location": "GLO",
        "unit": unit,
        "database": "ecoinvent-test",
        "code": code,
    }


def prepared():
    records = (
        catalog(
            native(
                1,
                1,
                "root",
                exchanges=[
                    purchase("component"),
                    purchase("background", amount=-3),
                    {
                        "name": "Carbon dioxide, fossil",
                        "categories": ["air"],
                        "unit": "kilogram",
                        "amount": 0.7,
                        "type": "biosphere",
                        "uncertainty type": 3,
                        "scale": 0.1,
                    },
                ],
            ),
            native(2, 2, "component"),
        )
        .resolve(["dataset-1"])
        .activities
    )
    return SimpleNamespace(activities=records)


def prepare(data, source, **kwargs):
    return prepare_clic_for_premise(
        data,
        source,
        version="3.12",
        biosphere_name="actual-biosphere",
        biosphere_codes={
            ("Carbon dioxide, fossil", "air", "unspecified", "kilogram"): "co2"
        },
        classifications={},
        **kwargs,
    )


def test_all_links_use_real_destination_keys_without_changing_amounts():
    data = prepared()
    source = [background()]
    saved = copy.deepcopy((data.activities, source))
    records, diagnostics = prepare(data, source)
    root = records[0]
    assert root["exchanges"][1]["input"] == ("premise-CLIC", "clic-2-2")
    assert root["exchanges"][2]["input"] == ("ecoinvent-test", "native")
    assert root["exchanges"][2]["amount"] == -3
    assert root["exchanges"][3]["input"] == ("actual-biosphere", "co2")
    assert (data.activities, source) == saved
    assert not diagnostics


def test_namespaced_collision_preserves_both_inventories_and_internal_links():
    data = prepared()
    records, diagnostics = prepare(
        data, [background(), background("component", code="existing-component")]
    )
    component = records[1]
    assert component["name"] == "component [CLIC dataset-2]"
    assert records[0]["exchanges"][1]["name"] == component["name"]
    assert records[0]["exchanges"][1]["input"] == (
        component["database"],
        component["code"],
    )
    assert component["exchanges"][0]["name"] == component["name"]
    assert diagnostics[0]["code"] == "clic.namespaced_identity"


@pytest.mark.parametrize(
    "source",
    [[], [background(), background(code="duplicate")], [background(unit="unit")]],
)
def test_missing_ambiguous_or_wrong_unit_background_cannot_be_used(source):
    with pytest.raises(ClicInventoryError, match="Expected one source supplier"):
        prepare(prepared(), source)


def test_biosphere_failure_never_drops_an_emission():
    with pytest.raises(ClicInventoryError, match="Biosphere flow is absent"):
        prepare_clic_for_premise(
            prepared(),
            [background()],
            version="3.12",
            biosphere_codes={},
            classifications={},
        )


def test_uncertainty_choice_applies_to_every_exchange():
    records, _ = prepare(prepared(), [background()], keep_uncertainty=False)
    for activity in records:
        for edge in activity["exchanges"]:
            assert edge["uncertainty type"] == 0
            assert edge["loc"] == edge["amount"]
            assert "scale" not in edge


def test_generated_helpers_link_by_identity_before_their_names_are_qualified():
    data = prepared()
    helper = background("generated helper", code="helper")
    helper.update(exchanges=[], **{"clic provenance": {"role": "generated helper"}})
    data.activities.append(helper)
    data.activities[0]["exchanges"].append(
        {**purchase("generated helper"), "amount": 2}
    )
    records, _ = prepare(data, [background()])
    edge = records[0]["exchanges"][-1]
    assert edge["input"] == ("premise-CLIC", "helper")
    assert edge["name"] == records[-1]["name"]


def test_unit_symbols_are_case_sensitive():
    data = prepared()
    data.activities[0]["exchanges"][2]["unit"] = "mW"
    with pytest.raises(ClicInventoryError, match="Expected one source supplier"):
        prepare(data, [background(unit="MW")])


def test_en15804_scrap_is_adapted_before_matching_native_suppliers():
    outlet = {
        "name": "aluminium scrap, new, Recycled Content cut-off",
        "product": "aluminium scrap, new",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": -0.2,
    }
    data = SimpleNamespace(
        activities=catalog(native(1, 1, "equipment", exchanges=[outlet]))
        .resolve(["dataset-1"])
        .activities
    )
    supplier = {
        "name": "market for aluminium scrap, new",
        "reference product": "aluminium scrap, new",
        "location": "RoW",
        "unit": "kilogram",
        "database": "en15804",
        "code": "scrap",
        "exchanges": [{"type": "production", "amount": 1}],
    }
    records, diagnostics = prepare(data, [supplier], system_model="EN15804")
    edge = records[0]["exchanges"][1]
    assert edge["input"] == ("en15804", "scrap")
    assert edge["location"] == "RoW"
    assert edge["amount"] == -0.2
    assert diagnostics[0]["code"] == "clic.en15804_native_scrap_adapted"


def test_null_optional_production_volumes_use_premise_fallback():
    data = prepared()
    data.activities[0]["production volume"] = None
    data.activities[0]["exchanges"][0]["production volume"] = None
    records, _ = prepare(data, [background()])
    assert "production volume" not in records[0]
    assert "production volume" not in records[0]["exchanges"][0]


def test_conditional_compression_contract_preserves_balance_and_pressure_caveats():
    from premise.transport.vehicle_hydrogen import prepare_hydrogen_compression
    from premise.inventory.clic_adapter import _normalize_compression_contract

    activity = {
        "name": "transport",
        "code": "transport",
        "hydrogen tank compression": {
            "electricity kWh per kg H2": 2,
            "delivery pressure is an explicit scenario not native-label evidence": True,
            "source_pressure_proof": {
                "source_market_proves_30bar_pipeline_delivery": False
            },
        },
        "exchanges": [
            {
                "type": "technosphere",
                "name": "hydrogen market",
                "product": "hydrogen, gaseous, low pressure",
                "unit": "kilogram",
                "amount": 0.1,
            },
            {
                "type": "technosphere",
                "name": "electricity market",
                "product": "electricity, low voltage",
                "unit": "kilowatt hour",
                "amount": 0.2,
                "comment": "Explicit 30-bar delivery scenario, conditional on fuel not already dispensed. Existing delivery retained.",
            },
        ],
    }
    amounts = [edge["amount"] for edge in activity["exchanges"]]
    diagnostics = []
    _normalize_compression_contract(activity, diagnostics)
    assert prepare_hydrogen_compression(activity) is not None
    assert [edge["amount"] for edge in activity["exchanges"]] == amounts
    assert (
        activity["hydrogen tank compression"]["source_pressure_proof"][
            "source_market_proves_30bar_pipeline_delivery"
        ]
        is False
    )
    activity["hydrogen tank compression"].pop("electricity product")
    activity["exchanges"][1]["amount"] = 0.3
    activity["exchanges"][1]["comment"] = activity["exchanges"][1][
        "comment"
    ].removeprefix("Hydrogen tank compression. ")
    with pytest.raises(ClicInventoryError, match="do not balance"):
        _normalize_compression_contract(activity, [])
