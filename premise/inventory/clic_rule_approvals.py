"""Compile exact rule scopes from the reviewed Brightpath pilot ledger."""

import json
import hashlib
from pathlib import Path

from .clic_archive import ClicInventoryError


def approved_compatibility_rule_scopes(target):
    """Authorize existing inspected pilot decisions, without approving new matches.

    The rule payload and source profile must agree with the packaged material
    review and its executable reference-coordinate gate. UVEK 2026 uses the
    independently validated UVEK 2025 edge, followed by native release migration.
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

    review = json.loads(
        (DATA_DIR / "export/uvek_pilot_material_review.json").read_text(
            encoding="utf-8"
        )
    )
    resource = load_uvek_technosphere_resource()
    indexed = {rule["id"]: rule for rule in resource["replace"]}
    bridge = TechnosphereProfile(**resource["target_profile"])
    scopes = set()
    for decision in review["rules"]:
        rule = indexed[decision["rule_id"]]
        if (
            rule.get("mapping_method") != "reviewed_pilot_material"
            or not rule.get("review_ref")
            or rule["source"] != decision["source"]
            or rule["target"] != decision["target"]
            or rule.get("conversion_factor") != decision["conversion_factor"]
        ):
            raise ClicInventoryError(
                "Brightpath pilot material approval no longer matches its executable rule."
            )
        profiles = rule.get("reference_flow_sign_review", {}).get("profiles", ())
        for observation in decision["profiles"]:
            exact = [
                p
                for p in profiles
                if p["source_profile"] == observation["source_profile"]
            ]
            if (
                len(exact) != 1
                or exact[0]["review"].get("status") != "verified"
                or exact[0]["conversion_factor"] != decision["conversion_factor"]
                or observation["reference_amount"] != 1
            ):
                raise ClicInventoryError(
                    "Brightpath pilot material approval has no matching inspected reference coordinate."
                )
            source = TechnosphereProfile(**observation["source_profile"])
            scopes.add(
                compatibility_rule_fingerprint(
                    resource, "replace", rule, source, bridge
                )
            )
    # This ledger pins an existing standing review, including each profile's
    # complete helper closure. It is separate from the pilot material decisions.
    ledger = json.loads(
        (
            Path(__file__).resolve().parents[1] / "data/clic/approved_rule_scopes.json"
        ).read_text(encoding="utf-8")
    )
    if ledger.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported CLIC compatibility-approval ledger.")
    for decision in ledger["operations"]:
        review_bytes = (DATA_DIR / decision["review_path"]).read_bytes()
        rule = indexed[decision["rule_id"]]
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
