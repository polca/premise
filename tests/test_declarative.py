import itertools
import json

import pytest

from premise.declarative import (
    ActivityProposal,
    ExchangeFactor,
    LatentReference,
    MatrixCoordinate,
    MetadataProposal,
    PatchConflict,
    ProviderProposal,
    ReferenceActivity,
    ReferenceExchange,
    SectorPatch,
    SharedOperation,
    compose_patches,
)
from premise.emissions.declarative import EmissionsPatchPlanner
from premise.transport.declarative import vehicle_emission_operations


def patch(
    sector, *, factor=1.0, reference_digest="immutable-reference", **kwargs
):
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
        reference_digest=reference_digest,
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


def test_emissions_patch_covers_inactive_latent_electricity_activity():
    latent_electricity = ReferenceActivity.from_metadata(
        identity="latent-electricity",
        name="electricity production, gas",
        product="electricity",
        location="EUR",
        unit="kilowatt hour",
        exchanges=(
            ReferenceExchange.from_metadata(
                identity="latent-nox-exchange",
                input_identity="nox-flow",
                type="biosphere",
                name="Nitrogen oxides",
                amount=0.1,
                unit="kilogram",
            ),
        ),
    )
    reference = LatentReference(
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        activities=(latent_electricity,),
    )
    planner = EmissionsPatchPlanner(
        pollutant_mapping={"Nitrogen oxides": "NOx"},
        activity_sector_mapping={"electricity production, gas": "Power"},
        location_mapping={},
        gains_factors={("EUR", "NOx", "Power"): 0.4},
        gains_regions=frozenset({"EUR"}),
    )

    emissions = planner.plan(reference)
    electricity = patch(
        "electricity",
        factors=(),
        reference_digest=reference.digest,
        activities=(ActivityProposal("latent-electricity", "activate"),),
    )

    assert emissions.factors == (
        ExchangeFactor(
            MatrixCoordinate("biosphere", "nox-flow", "latent-electricity"),
            0.4,
        ),
    )
    assert planner.plan(reference).digest == emissions.digest
    assert compose_patches([electricity, emissions]).factors[0].factor == 0.4


def test_vehicle_emission_operation_declares_source_without_sector_context():
    source = ReferenceActivity.from_metadata(
        identity="source",
        name="transport, passenger car, gasoline, Medium, EURO-6",
        product="transport, passenger car",
        location="RER",
        unit="kilometer",
        exchanges=(
            ReferenceExchange.from_metadata(
                identity="source-co2",
                input_identity="source-only-co2",
                type="biosphere",
                name="Carbon dioxide, fossil",
                amount=0.1,
                metadata={"categories": ["air"]},
            ),
            ReferenceExchange.from_metadata(
                identity="source-nox",
                input_identity="nox",
                type="biosphere",
                name="Nitrogen oxides",
                amount=0.2,
                metadata={"categories": ["air", "urban air close to ground"]},
            ),
        ),
    )
    wrapper = ReferenceActivity.from_metadata(
        identity="wrapper",
        name="petrol, burned in passenger car",
        product="heat",
        location="RER",
        unit="megajoule",
        exchanges=(
            ReferenceExchange.from_metadata(
                identity="wrapper-co2",
                input_identity="co2",
                type="biosphere",
                name="Carbon dioxide, fossil",
                amount=0.07,
                metadata={"categories": ["air"]},
            ),
        ),
        metadata={
            "energy use source": json.dumps(
                {
                    "source": [
                        source.name,
                        source.product,
                        source.location,
                        source.unit,
                    ],
                    "purchased MJ per source service": 2.5,
                }
            )
        },
    )

    operations = vehicle_emission_operations((wrapper, source))

    assert len(operations) == 1
    assert operations[0].operator == "vehicle-emissions-from-source"
    assert operations[0].target == "wrapper"
    assert operations[0].payload == {
        "source": "source",
        "purchased_mj_per_source_service": 2.5,
        "fuel_proportional_flows": ["co2", "source-only-co2"],
    }
