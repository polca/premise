import itertools
import json

import pytest

from premise.core.declarative import (
    authoritative_column_operations,
    cached_provider_route_operation,
    canonical_provider_kind_operation,
    normalized_provider_vector_operation,
    relink_dataset_scope_operation,
    unavailable_provider_operation,
)
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
from premise.energy.final_energy import _declare_provider_kind_policies
from premise.factorization import FactorizationIdentityRegistry
from premise.transport.declarative import vehicle_emission_operations


def patch(sector, *, factor=1.0, reference_digest="immutable-reference", **kwargs):
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
        ExchangeFactor(MatrixCoordinate("technosphere", "provider", "activity"), 1.2),
    )
    assert (
        patch("emissions", factors=factors).digest
        == patch("emissions", factors=tuple(reversed(factors))).digest
    )


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
                [
                    patch("electricity", factors=(), **left),
                    patch("fuels", factors=(), **right),
                ]
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


def test_provider_kind_operation_declares_one_market_family_policy():
    operation = canonical_provider_kind_operation(
        source_name="market for natural gas, high pressure",
        target_names=("market group for natural gas, high pressure",),
        product="natural gas, high pressure",
        unit="cubic meter",
    )
    assert operation is not None
    assert operation.operator == "canonicalize-provider-kind"
    assert operation.target == "technosphere"
    assert operation.payload["source_names"] == [
        "market for natural gas, high pressure",
        "market group for natural gas, high pressure",
    ]
    assert (
        canonical_provider_kind_operation(
            source_name="market for natural gas, high pressure",
            target_names=("market for natural gas, high pressure",),
            product="natural gas, high pressure",
            unit="cubic meter",
        )
        is None
    )


def test_cached_provider_route_operation_is_canonical_and_validated():
    operation = cached_provider_route_operation(
        consumer_location="CAZ",
        request=(
            "market group for electricity, low voltage",
            "electricity, low voltage",
            "Canada without Quebec",
            "kilowatt hour",
        ),
        providers=(
            (
                "market group for electricity, low voltage",
                "electricity, low voltage",
                "Canada without Quebec",
                "kilowatt hour",
                0.4,
            ),
            (
                "market group for electricity, low voltage",
                "electricity, low voltage",
                "CA",
                "kilowatt hour",
                0.6,
            ),
        ),
    )

    assert operation.operator == "cached-provider-route"
    assert operation.payload["consumer_location"] == "CAZ"
    assert [provider["location"] for provider in operation.payload["providers"]] == [
        "CA",
        "Canada without Quebec",
    ]
    with pytest.raises(ValueError, match="sum to one"):
        cached_provider_route_operation(
            consumer_location="CAZ",
            request=("market", "electricity", "CA", "kilowatt hour"),
            providers=(("market", "electricity", "CA", "kilowatt hour", 0.5),),
        )


def test_relink_dataset_scope_operation_is_canonical():
    operation = relink_dataset_scope_operation(
        excludes_datasets=("legacy heat", "legacy heat"),
        alternative_names=("market group", "market"),
    )

    assert operation.operator == "relink-dataset-scope"
    assert operation.target == "technosphere"
    assert operation.payload == {
        "scope": "all-datasets",
        "exclude_name_substrings": ["legacy heat"],
        "alternative_names": ["market", "market group"],
    }


def test_final_energy_provider_kind_policies_are_selection_independent():
    registry = FactorizationIdentityRegistry()
    with registry.session("final energy"):
        _declare_provider_kind_policies()

    operations = registry.payload_for([])["shared_operations"]
    assert len(operations) == 2
    assert {operation["payload"]["product"] for operation in operations} == {
        "natural gas, high pressure",
        "heat, district or industrial, natural gas",
    }
    assert all(
        operation["operator"] == "canonicalize-provider-kind"
        for operation in operations
    )


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
    assert emissions.metadata[0].condition == "has-biosphere-exchanges"
    assert planner.plan(reference).digest == emissions.digest
    assert compose_patches([electricity, emissions]).factors[0].factor == 0.4


def test_emissions_patch_declares_vehicle_refresh_from_immutable_reference():
    source = ReferenceActivity.from_metadata(
        identity="source",
        name="transport, passenger bus, diesel",
        product="transport, passenger bus",
        location="RER",
        unit="person kilometer",
        exchanges=(
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
        name="diesel, burned in passenger bus",
        product="heat",
        location="RER",
        unit="megajoule",
        metadata={
            "energy use source": json.dumps(
                {
                    "source": [
                        source.name,
                        source.product,
                        source.location,
                        source.unit,
                    ],
                    "purchased MJ per source service": 2.0,
                }
            )
        },
    )
    reference = LatentReference(
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        activities=(source, wrapper),
    )
    planner = EmissionsPatchPlanner(
        pollutant_mapping={},
        activity_sector_mapping={},
        location_mapping={},
        gains_factors={},
        gains_regions=frozenset(),
    )

    patch = planner.plan(reference)

    assert patch.shared_operations == vehicle_emission_operations(reference.activities)
    assert patch.shared_operations[0].operator == "vehicle-emissions-from-source"
    assert patch.shared_operations[0].target == "wrapper"


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


def test_unavailable_provider_declaration_is_derived_from_sector_metadata():
    active = ReferenceActivity.from_metadata(
        identity="regional-steel",
        name="market for steel, low-alloyed",
        product="steel, low-alloyed",
        location="EUR",
        unit="kilogram",
    )
    emptied = ReferenceActivity.from_metadata(
        identity="global-steel",
        name="market for steel, low-alloyed",
        product="steel, low-alloyed",
        location="GLO",
        unit="kilogram",
        metadata={"emptied": True},
    )

    operation = unavailable_provider_operation((active, emptied))

    assert operation is not None
    assert operation.operator == "relink-unavailable-providers"
    assert operation.target == "technosphere"
    assert operation.payload == {"providers": ["global-steel"]}


def test_emptied_activity_declares_authoritative_technosphere_column():
    activity = ReferenceActivity.from_metadata(
        identity="old-cement",
        name="cement production",
        product="cement",
        location="PE",
        unit="kilogram",
        exchanges=(
            ReferenceExchange.from_metadata(
                identity="production",
                input_identity="old-cement",
                type="technosphere",
                name="cement production",
                amount=1.0,
            ),
            ReferenceExchange.from_metadata(
                identity="forward",
                input_identity="regional-cement",
                type="technosphere",
                name="cement production",
                amount=-1.0,
            ),
        ),
        metadata={"emptied": True},
    )

    operations = authoritative_column_operations((activity,))

    assert operations[0].operator == "authoritative-technosphere-column"
    assert operations[0].target == "old-cement"
    assert operations[0].payload == {
        "allowed_providers": ["old-cement", "regional-cement"]
    }


def test_normalized_provider_vector_declaration_is_canonical_and_validated():
    operation = normalized_provider_vector_operation(
        target="market",
        weights={"provider-b": 3, "provider-a": 1},
        total=-4,
        replace_providers=("legacy", "legacy"),
    )

    assert operation.operator == "normalize-provider-vector"
    assert operation.target == "market"
    assert operation.payload == {
        "replace_providers": ["legacy"],
        "total": -4.0,
        "weights": {"provider-a": 1.0, "provider-b": 3.0},
    }

    with pytest.raises(ValueError, match="sum above zero"):
        normalized_provider_vector_operation(
            target="market", weights={"provider": 0}, total=-1
        )
