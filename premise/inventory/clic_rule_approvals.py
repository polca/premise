"""Validate pinned approvals for the exact UVEK electricity pilot target."""

import hashlib
import json
from pathlib import Path

from .clic_archive import ClicInventoryError


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
    ledger = json.loads(
        (
            Path(__file__).resolve().parents[1] / "data/clic/approved_rule_scopes.json"
        ).read_text(encoding="utf-8")
    )
    if ledger.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported CLIC compatibility-approval ledger.")
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
