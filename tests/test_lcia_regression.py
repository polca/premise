import pytest

import lcia_regression

IPCC_2013 = (
    "IPCC 2013",
    "climate change",
    "global warming potential (GWP100)",
)


def test_get_lcia_regression_method_resolves_ecoinvent_prefix(monkeypatch):
    monkeypatch.setattr(
        lcia_regression,
        "_load_reference_scores",
        lambda: {
            "cases": {
                "ecoinvent-3.12-cutoff": {"method": list(IPCC_2013)},
            },
        },
    )
    monkeypatch.setattr(
        lcia_regression.bw2data,
        "methods",
        [("ecoinvent-3.12",) + IPCC_2013],
    )

    method = lcia_regression.get_lcia_regression_method("ecoinvent-3.12-cutoff")

    assert method == ("ecoinvent-3.12",) + IPCC_2013


def test_resolve_lcia_method_prefers_current_ecoinvent_version():
    available_methods = [
        ("ecoinvent-3.11",) + IPCC_2013,
        ("ecoinvent-3.12",) + IPCC_2013,
    ]

    method = lcia_regression._resolve_lcia_method(
        IPCC_2013,
        "ecoinvent-3.12-cutoff",
        available_methods,
    )

    assert method == ("ecoinvent-3.12",) + IPCC_2013


def test_resolve_lcia_method_accepts_unprefixed_registered_method():
    method = lcia_regression._resolve_lcia_method(
        ("ecoinvent-3.12",) + IPCC_2013,
        "ecoinvent-3.12-cutoff",
        [IPCC_2013],
    )

    assert method == IPCC_2013


def test_resolve_lcia_method_fails_on_ambiguous_suffix():
    available_methods = [
        ("ecoinvent-3.11",) + IPCC_2013,
        ("ecoinvent-3.12",) + IPCC_2013,
    ]

    with pytest.raises(AssertionError, match="Multiple LCIA regression methods"):
        lcia_regression._resolve_lcia_method(
            IPCC_2013,
            "custom-case",
            available_methods,
        )


def test_resolve_lcia_method_does_not_use_wrong_ecoinvent_version():
    with pytest.raises(AssertionError, match="another ecoinvent prefix"):
        lcia_regression._resolve_lcia_method(
            IPCC_2013,
            "ecoinvent-3.12-cutoff",
            [("ecoinvent-3.11",) + IPCC_2013],
        )
