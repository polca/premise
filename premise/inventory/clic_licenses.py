"""Commercial-use screening of the selected CLIC foreground graph.

This checks declared permissions, not ownership of the underlying rights.
Background databases, retained workbooks and user inventories need their own
permission assessment. Source-license conflicts require a reviewed resolution.
"""

from __future__ import annotations

import re
from collections import deque

from .clic_archive import ClicInventoryError, content_hash

COMMERCIAL_LICENSES = {
    "CC0-1.0": "https://creativecommons.org/publicdomain/zero/1.0/",
    "CC-BY-3.0": "https://creativecommons.org/licenses/by/3.0/",
    "CC-BY-4.0": "https://creativecommons.org/licenses/by/4.0/",
    "CC-BY-SA-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
}
POLICY = {
    "name": "commercial",
    "schema_version": 1,
    "allowed_licenses": COMMERCIAL_LICENSES,
    "source_conflicts": "block_explicit_noncommercial_license",
}
POLICY_FINGERPRINT = content_hash(POLICY)
_NONCOMMERCIAL = re.compile(
    r"CC[\s-]*BY[\s-]*NC|creativecommons\.org/licenses/by-nc", re.I
)


class ClicLicenseError(ClicInventoryError):
    """The selected foreground contains inventories needing replacement/review."""

    def __init__(self, issues):
        self.issues = issues
        examples = "; ".join(
            f"{' -> '.join(row['path'])}: {row['reason']} ({row['license'] or 'missing license'})"
            for row in issues[:10]
        )
        super().__init__(
            f"CLIC commercial policy blocks {len(issues)} dataset(s). "
            f"Reviewed replacements or license evidence are required. {examples}"
        )


def _root_paths(resolved):
    """One shortest root-to-supplier witness per node, including cyclic graphs."""
    activities = {a["code"]: a for a in resolved.activities}
    slugs = {row["activity_code"]: row["slug"] for row in resolved.versions}
    parents = {code: None for code, slug in slugs.items() if slug in resolved.roots}
    queue = deque(parents)
    while queue:
        code = queue.popleft()
        for edge in activities[code]["exchanges"]:
            reference = edge.get("input")
            if (
                edge["type"] == "technosphere"
                and isinstance(reference, (list, tuple))
                and len(reference) == 2
                and reference[0] == "__clic__"
                and reference[1] in activities
                and reference[1] not in parents
            ):
                parents[reference[1]] = code
                queue.append(reference[1])
    return parents, slugs


def assess_commercial_licenses(resolved, archive):
    """Return selected permissions and review issues without changing inventories.

    Read source notes from the verified archive, rather than relying on cached
    manifest fields that older versions did not retain. A paper's license is
    not necessarily the inventory's license: explicit non-commercial conflicts
    are flagged for review, without asserting that the declaration is invalid.
    """
    datasets, issues = [], []
    for row in resolved.versions:
        native = archive.versions[row["version_id"]]
        license_id = (native.get("selected_license") or "").strip().upper()
        source_license = native.get("existing_license") or ""
        reason = None
        if license_id not in COMMERCIAL_LICENSES:
            reason = "license does not establish commercial permission"
        elif _NONCOMMERCIAL.search(source_license):
            reason = "source-license conflict needs review"
        entry = {
            "slug": row["slug"],
            "version_id": row["version_id"],
            "license": license_id,
            "license_url": COMMERCIAL_LICENSES.get(license_id),
            "source_license": source_license,
            "citation": row.get("citation", ""),
            "doi": row.get("doi", ""),
            "public_url": row.get("public_url"),
            "attribution_required": license_id != "CC0-1.0",
            "share_alike": license_id == "CC-BY-SA-4.0",
        }
        datasets.append(entry)
        if reason:
            issues.append(
                {**entry, "reason": reason, "activity_code": row["activity_code"]}
            )
    if issues:
        parents, slugs = _root_paths(resolved)
        for issue in issues:
            path, code = [], issue.pop("activity_code")
            while code is not None:
                path.append(slugs[code])
                code = parents.get(code)
            issue["path"] = list(reversed(path))
    return {
        "name": "commercial",
        "policy_fingerprint": POLICY_FINGERPRINT,
        "status": "blocked" if issues else "passed",
        "scope": "Selected CLIC foreground only; ecoinvent, retained workbooks and user inventories require separate permissions.",
        "datasets": datasets,
        "issues": issues,
    }


def enforce_commercial_licenses(resolved, archive):
    report = assess_commercial_licenses(resolved, archive)
    if report["issues"]:
        raise ClicLicenseError(report["issues"])
    return report
