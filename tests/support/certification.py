"""Typed certification substitutes for storage, reporting and export unit tests.

These substitutes perform no scientific validation. Tests of checks, cache
invalidation or certification must use the real NewDatabase implementation.
"""

from premise.validation.validation_framework import (
    VALIDATION_RULESET_VERSION,
    ValidationCertificate,
    ValidationReport,
)


def certification_report(scenario, store=None) -> ValidationReport:
    """Return an empty fixture report without opening or scanning an inventory."""
    from premise.database.new_database import NewDatabase

    return ValidationReport(
        scenario_identity=NewDatabase._scenario_identity(scenario),
        store_generation=getattr(store, "generation", 0),
        ruleset_version=VALIDATION_RULESET_VERSION,
        certificate_key="test-only-certification-stub",
        rule_results=(),
    )


def certification_certificate(
    scenario, store, *, intent=None, raise_on_error=True, exhaustive=False
) -> ValidationCertificate:
    """Match the bound certification signature for tests of storage mechanics."""
    report = certification_report(scenario, store)
    return ValidationCertificate(
        cache_key=report.certificate_key,
        store_generation=report.store_generation,
        ruleset_version=report.ruleset_version,
        scenario_identity=report.scenario_identity,
        source_fingerprint="test-only-source",
        iam_fingerprint="test-only-iam",
        system_model="test-only-system-model",
        version="test-only-version",
        report=report,
    )
