"""Validate pinned approvals for the exact UVEK electricity pilot target."""

import copy
import hashlib
import json
from pathlib import Path

from .clic_archive import ClicInventoryError


def _approval_ledger():
    ledger = json.loads(
        (
            Path(__file__).resolve().parents[1] / "data/clic/approved_rule_scopes.json"
        ).read_text(encoding="utf-8")
    )
    if ledger.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported CLIC compatibility-approval ledger.")
    return ledger


def compatibility_approval_metadata(approved_scopes):
    """Describe validated scopes for cache identity and portable provenance."""
    approved = set(approved_scopes)
    reviews = []
    if not approved:
        return reviews
    for decision in _approval_ledger()["operations"]:
        for scope in decision["scopes"]:
            if scope["fingerprint"] in approved:
                reviews.append(
                    {
                        "fingerprint": scope["fingerprint"],
                        **{
                            key: copy.deepcopy(decision[key])
                            for key in (
                                "rule_id",
                                "review_path",
                                "review_sha256",
                                "authorization",
                            )
                        },
                        "limitations": list(decision.get("limitations", ())),
                        **(
                            {"inspection": copy.deepcopy(decision["inspection"])}
                            if "inspection" in decision
                            else {}
                        ),
                    }
                )
    return sorted(reviews, key=lambda review: review["fingerprint"])


def annotate_compatibility_reviews(report, approval_metadata):
    """Retain review evidence and limitations for matched approved rules.

    A failed migration can contain preflight approvals alongside refusals. These
    findings describe the correspondence review, not a completed conversion.
    """
    indexed = {review["fingerprint"]: review for review in approval_metadata}
    matched = {}
    for stage in report.get("stages", ()):
        warnings = []
        for issue in stage.get("issues", ()):
            if issue["code"] != "migration.approved_correspondence":
                continue
            review = indexed.get(issue.get("details", {}).get("fingerprint"))
            if review is None:
                continue
            issue["details"]["premise_review"] = copy.deepcopy(review)
            matched[review["fingerprint"]] = copy.deepcopy(review)
            if review["limitations"]:
                warnings.append(
                    {
                        "code": "clic.compatibility_proxy_limitations",
                        "severity": "warning",
                        "stage": stage["stage"],
                        "path": issue.get("path", ""),
                        "message": " ".join(review["limitations"]),
                        "details": copy.deepcopy(review),
                        "suggested_fix": "",
                    }
                )
        stage.setdefault("issues", []).extend(warnings)
    if matched:
        report["premise_compatibility_reviews"] = [
            matched[key] for key in sorted(matched)
        ]


def approved_recipe_source_omission(loss, report):
    """Accept only a literal omission inside a matched, fingerprinted recipe review."""
    if loss.code != "migration.recipe_source_input_omission":
        return False
    details = loss.to_dict()["details"]
    index = details.get("omission_index")
    if type(index) is not int or index < 0:
        return False
    for review in report.get("premise_compatibility_reviews", ()):
        inspection = review.get("inspection", {})
        omissions = inspection.get("approved_recipe_source_omissions", {}).get(
            details.get("recipe_id"), []
        )
        if (
            index < len(omissions)
            and details.get("omission") == omissions[index]
            and details.get("source_profile") == inspection.get("source_profile")
            and details.get("basis")
            == {"amount": 1, "unit": inspection.get("source", {}).get("unit")}
        ):
            return True
    return False


def approved_compatibility_rule_scopes(target):
    """Authorize existing inspected pilot decisions, without approving new matches.

    Approval requires both the reviewed evidence and the complete rule/helper
    closure to retain their recorded fingerprints. UVEK 2026 uses the reviewed
    UVEK 2025 edge, followed by native release migration.
    """
    if (
        target.technosphere.family,
        target.technosphere.version,
        target.technosphere.system_model,
        target.biosphere.family,
        target.biosphere.version,
    ) != ("uvek", "2026", "cutoff", "ecoinvent", "3.10"):
        return ()
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    indexed = {
        (operation, rule["id"]): rule
        for operation in ("replace", "disaggregate", "delete")
        for rule in resource.get(operation, ())
    }
    bridge = TechnosphereProfile(**resource["target_profile"])
    scopes = set()
    # Pinned in Premise: new or changed Brightpath decisions cannot implicitly
    # broaden this pilot's independently reviewed approval set.
    ledger = _approval_ledger()
    for decision in ledger["operations"]:
        try:
            review_bytes = (DATA_DIR / decision["review_path"]).read_bytes()
            rule = indexed[(decision["operation"], decision["rule_id"])]
        except (OSError, KeyError) as error:
            raise ClicInventoryError(
                "Recorded compatibility approval has a missing packaged review or rule."
            ) from error
        if (
            hashlib.sha256(review_bytes).hexdigest() != decision["review_sha256"]
            or rule.get("mapping_method") != decision["mapping_method"]
        ):
            raise ClicInventoryError(
                "Recorded compatibility approval has a changed packaged review or method."
            )
        for scope in decision["scopes"]:
            source = TechnosphereProfile(**scope["source"])
            if TechnosphereProfile(**scope["target"]) != bridge:
                raise ClicInventoryError(
                    "Recorded compatibility approval targets a different route edge."
                )
            fingerprint = compatibility_rule_fingerprint(
                resource, decision["operation"], rule, source, bridge
            )
            if fingerprint != scope["fingerprint"]:
                raise ClicInventoryError(
                    "Recorded compatibility approval no longer matches its packaged rule or helper recipe."
                )
            scopes.add(fingerprint)
    return tuple(sorted(scopes))
