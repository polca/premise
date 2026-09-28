"""Illustrative findings using real report types; no inventory data required."""

from premise import (
    ValidationIssue,
    ValidationRuleResult,
    ValidationReport,
    PremiseValidationError,
)


def main():
    findings = (
        ValidationIssue(
            rule_id="example.proxy",
            severity="warning",
            message="Assess whether the global supplier represents this region.",
            activity_key=("example process", "example product", "CH"),
        ),
        ValidationIssue(
            rule_id="example.amount",
            severity="error",
            message="Exchange amount must be finite.",
            actual="NaN",
            expected="finite number",
            exchange_id=1,
        ),
    )
    rules = tuple(
        ValidationRuleResult(
            rule_id=i.rule_id,
            severity=i.severity,
            applicability="applicable",
            checked_object_count=1,
            issues=(i,),
        )
        for i in findings
    )
    report = ValidationReport(
        scenario_identity="illustration",
        store_generation=0,
        ruleset_version=0,
        certificate_key="example",
        rule_results=rules,
    )
    assert len(report.warnings) == len(report.errors) == 1
    for issue in report.issues:
        print(issue.severity, issue.rule_id, issue.message)
    try:
        report.raise_for_errors()
    except PremiseValidationError:
        print("Error prevents acceptance; fix the input and rebuild.")
    else:
        raise AssertionError("An error must prevent acceptance.")


if __name__ == "__main__":
    main()
