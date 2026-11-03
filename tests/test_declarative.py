import itertools

import pytest

from premise.declarative import (
    ActivityProposal,
    ExchangeFactor,
    MatrixCoordinate,
    MetadataProposal,
    PatchConflict,
    ProviderProposal,
    SectorPatch,
    SharedOperation,
    compose_patches,
)


def patch(sector, *, factor=1.0, **kwargs):
    factors = kwargs.pop(
        "factors",
        (
            ExchangeFactor(
                MatrixCoordinate("technosphere", "provider", "consumer"), factor
            ),
        ),
    )
    return SectorPatch(
        sector=sector,
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        reference_digest="immutable-reference",
        factors=factors,
        **kwargs,
    )


def test_patch_digest_is_independent_of_declaration_order():
    factors = (
        ExchangeFactor(MatrixCoordinate("biosphere", "co2", "activity"), 0.8),
        ExchangeFactor(
            MatrixCoordinate("technosphere", "provider", "activity"), 1.2
        ),
    )
    assert patch("emissions", factors=factors).digest == patch(
        "emissions", factors=tuple(reversed(factors))
    ).digest


def test_composition_is_caller_order_invariant_and_multiplies_factors():
    patches = (patch("electricity", factor=2.0), patch("emissions", factor=0.25))
    results = [compose_patches(order) for order in itertools.permutations(patches)]

    assert len({result.digest for result in results}) == 1
    assert results[0].factors[0].factor == 0.5
    assert results[0].factors[0].sectors == ("electricity", "emissions")


def test_activation_is_idempotent_across_sectors():
    proposal = (ActivityProposal("latent-market", "activate"),)
    result = compose_patches(
        [
            patch("electricity", factors=(), activities=proposal),
            patch("fuels", factors=(), activities=proposal),
        ]
    )

    assert len(result.activities) == 1
    assert result.activities[0].sectors == ("electricity", "fuels")


def test_conflicting_activity_provider_and_metadata_proposals_fail():
    cases = (
        (
            {"activities": (ActivityProposal("activity", "activate"),)},
            {"activities": (ActivityProposal("activity", "deactivate"),)},
        ),
        (
            {"providers": (ProviderProposal("exchange", "first"),)},
            {"providers": (ProviderProposal("exchange", "second"),)},
        ),
        (
            {"metadata": (MetadataProposal.from_value("activity", "unit", "kg"),)},
            {"metadata": (MetadataProposal.from_value("activity", "unit", "MJ"),)},
        ),
    )
    for left, right in cases:
        with pytest.raises(PatchConflict):
            compose_patches(
                [patch("electricity", factors=(), **left), patch("fuels", factors=(), **right)]
            )


def test_shared_operator_retains_invariant_declarations_in_sector_order():
    electricity = patch(
        "electricity",
        factors=(),
        shared_operations=(
            SharedOperation.from_payload(
                "normalize-market", "electricity-market", {"weight": 0.8}
            ),
        ),
    )
    fuels = patch(
        "fuels",
        factors=(),
        shared_operations=(
            SharedOperation.from_payload(
                "normalize-market", "electricity-market", {"weight": 0.2}
            ),
        ),
    )

    result = compose_patches([fuels, electricity])

    assert result.shared_operations[0].identity == (
        "normalize-market",
        "electricity-market",
    )
    assert result.shared_operations[0].sectors == ("electricity", "fuels")


def test_reference_mismatch_duplicate_sector_and_nonfinite_factor_fail():
    other_reference = SectorPatch(
        sector="fuels",
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        reference_digest="other-reference",
    )
    with pytest.raises(PatchConflict):
        compose_patches([patch("electricity"), other_reference])
    with pytest.raises(ValueError, match="Duplicate sector"):
        compose_patches([patch("electricity"), patch("electricity")])
    with pytest.raises(ValueError, match="finite"):
        ExchangeFactor(
            MatrixCoordinate("technosphere", "provider", "consumer"), float("nan")
        )
