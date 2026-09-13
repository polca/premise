"""Validate review coverage, example classifications and scientific metadata."""

import ast
import json
from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def canonical_pages():
    return sorted(
        p
        for group in (
            "getting_started",
            "user_guide",
            "methodology",
            "reference",
            "development",
        )
        for p in (DOCS / group).rglob("*.rst")
    )


def main():
    failures = []
    structure = json.loads(
        (DOCS / "development/methodology-structure.json").read_text()
    )
    diagrams = json.loads((DOCS / "methodology/process-diagrams.json").read_text())
    for name in structure["pages"]:
        source = (DOCS / name).read_text()
        diagram_key = name.removeprefix("methodology/").removesuffix(".rst")
        if diagram_key not in diagrams:
            failures.append(f"Missing process diagram definition: {name}")
        image = f"/_static/process-diagrams/{diagram_key.replace('/', '-')}.svg"
        scope = source.split("Inputs and applicability\n", 1)[0]
        if image not in scope or not (DOCS / image.lstrip("/")).exists():
            failures.append(f"Missing scope process diagram: {name}")
        sections = list(re.finditer(r"^([^\n]+)\n-{3,}\n", source, re.MULTILINE))
        if [m.group(1) for m in sections] != structure["headings"]:
            failures.append(f"Inconsistent sector structure: {name}")
        for i, section in enumerate(sections):
            end = sections[i + 1].start() if i + 1 < len(sections) else len(source)
            if not source[section.end() : end].strip():
                failures.append(f"Empty sector section: {name}: {section.group(1)}")
    register = yaml.safe_load((DOCS / "reference/source-register.yaml").read_text())
    required = {
        "id",
        "title",
        "page",
        "implementation",
        "data_years",
        "boundary",
        "applicability",
        "source",
        "candidate",
        "reviewed_on",
        "status",
        "finding",
        "backlog",
        "limitations",
    }
    ids = set()
    for record in register["sectors"]:
        if required - record.keys() or any(not record.get(k) for k in required):
            failures.append(f"Incomplete source record: {record.get('id')}")
        if record["id"] in ids:
            failures.append(f"Duplicate source record: {record['id']}")
        ids.add(record["id"])
        for path in (ROOT / record["implementation"], DOCS / (record["page"] + ".rst")):
            if not path.exists():
                failures.append(f"Missing source reference: {path}")
        for key in ("source", "candidate"):
            if not record[key].startswith("https://"):
                failures.append(f"Expected https source: {record['id']} {key}")
    reviews = json.loads((DOCS / "development/content-review.json").read_text())
    by_page = {r["page"]: r for r in reviews}
    for path in canonical_pages():
        name = path.relative_to(DOCS).as_posix()
        if name not in by_page:
            failures.append(f"No content-review disposition: {name}")
        elif not by_page[name].get("disposition"):
            failures.append(f"Empty review disposition: {name}")
    examples = json.loads((DOCS / "examples/status.json").read_text())
    for path in (DOCS / "examples").glob("*.py"):
        ast.parse(path.read_text())
        if path.name not in examples:
            failures.append(f"Unclassified example: {path.name}")
    for name, record in examples.items():
        if record["classification"] not in {
            "self-contained executable",
            "licensed executable",
            "preflight executable",
        }:
            failures.append(f"Invalid example classification: {name}")
    for path in canonical_pages():
        source = path.read_text()
        if re.search(r"https://doi\.org/https://|this is NEW|today \(2020\)", source):
            failures.append(f"Stale editorial marker: {path.relative_to(DOCS)}")
    if failures:
        raise SystemExit("\n".join(failures))
    print(
        f"Checked {len(by_page)} page dispositions, {len(structure['pages'])} sector structures, {len(ids)} sector source records and {len(examples)} executable classifications."
    )


if __name__ == "__main__":
    main()
