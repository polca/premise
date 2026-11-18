import pytest

import premise.materials.metals as module
from premise.metals_rules import MetalsConfigError, load_material_rules


def test_no_source_filter_retains_all_existing_rules_and_order():
    rules = load_material_rules().enabled_rules
    selected = module._select_material_rules(rules, {})
    for technology in {r.technology for r in rules}:
        assert selected[technology] == [r for r in rules if r.technology == technology]


def test_component_scope_selects_only_the_configured_neodymium_rule():
    rules = load_material_rules().enabled_rules
    rid = "wind-ddpm-neodymium-neodymium-oxide"
    selected = module._select_material_rules(
        rules, {"Wind-DDPM": {"material_rule_ids": [rid]}}
    )
    assert [r.id for r in selected["Wind-DDPM"]] == [rid]
    assert selected["PEMEL"] == [r for r in rules if r.technology == "PEMEL"]
    assert len([r for r in rules if r.technology == "Wind-DDPM"]) > 1


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        "one-rule",
        [123],
        ["not-a-rule"],
        ["wind-ddpm-neodymium-neodymium-oxide"] * 2,
        ["pemfc-platinum-platinum"],
    ],
)
def test_invalid_or_cross_technology_scopes_fail_before_updates(value):
    with pytest.raises(MetalsConfigError, match="source-qualified material rules"):
        module._select_material_rules(
            load_material_rules().enabled_rules,
            {"Wind-DDPM": {"material_rule_ids": value}},
        )


def test_source_declarations_are_not_mutated():
    rule_ids = ["wind-ddpm-neodymium-neodymium-oxide"]
    declarations = {"Wind-DDPM": {"material_rule_ids": rule_ids}}
    module._select_material_rules(load_material_rules().enabled_rules, declarations)
    assert rule_ids == ["wind-ddpm-neodymium-neodymium-oxide"]
