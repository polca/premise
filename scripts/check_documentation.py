"""Check a built Sphinx site without licensed data or third-party imports.

Run from the repository root: python scripts/check_documentation.py BUILD_DIR
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit


class Page(HTMLParser):
    def __init__(self, source):
        super().__init__(convert_charrefs=True)
        self.ids = []
        self.links = []
        self.assets = []
        self.headings = []
        self.blocks = []
        self._main = False
        self._heading = None
        self._text = []
        self._python = False
        self._pre = False
        self._code = []
        self.feed(source)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get("role") == "main":
            self._main = True
        if "id" in attrs:
            self.ids.append(attrs["id"])
        if tag == "a" and "href" in attrs:
            self.links.append(attrs["href"])
        if tag in {"img", "script"} and "src" in attrs:
            self.assets.append(attrs["src"])
        if tag == "link" and attrs.get("rel") == "stylesheet":
            self.assets.append(attrs.get("href", ""))
        if self._main and re.fullmatch("h[1-6]", tag):
            self._heading = int(tag[1])
            self._text = []
        if tag == "div" and "highlight-" in attrs.get("class", ""):
            self._python = "highlight-python" in attrs["class"]
        if tag == "pre":
            self._pre = True
            self._code = []

    def handle_data(self, data):
        if self._heading:
            self._text.append(data)
        if self._pre:
            self._code.append(data)

    def handle_endtag(self, tag):
        if self._heading and tag == f"h{self._heading}":
            self.headings.append((self._heading, "".join(self._text)))
            self._heading = None
        if tag == "pre":
            if self._python:
                self.blocks.append("".join(self._code))
            self._pre = False
        if tag == "footer":
            self._main = False


def resolve(root, page, href):
    url = urlsplit(href)
    if url.scheme or url.netloc or href.startswith("//"):
        return None
    path = unquote(url.path)
    target = (root / path.lstrip("/")) if path.startswith("/") else (page.parent / path)
    if not path:
        target = page
    if target.is_dir():
        target /= "index.html"
    return target.resolve(), unquote(url.fragment)


def check(root, repo):
    root = root.resolve()
    pages = {p.resolve(): Page(p.read_text()) for p in root.rglob("*.html")}
    errors = []
    manifest = json.loads((repo / "docs/legacy-links.json").read_text())
    legacy = {
        entry["old"].split("#")[0]
        for entry in manifest
        if entry.get("compatibility_page", True)
    } - {"index.html"}
    checked_examples = 0
    for path, page in pages.items():
        rel = path.relative_to(root).as_posix()
        for ident, count in Counter(page.ids).items():
            if count > 1:
                errors.append(f"{rel}: duplicate HTML id {ident}")
        for link in page.links + page.assets:
            dest = resolve(root, path, link)
            if dest is None:
                continue
            target, fragment = dest
            if not target.exists():
                errors.append(f"{rel}: missing target {link}")
            elif fragment and target in pages and fragment not in pages[target].ids:
                errors.append(f"{rel}: missing fragment {link}")
        if (
            rel not in legacy
            and rel not in {"genindex.html", "py-modindex.html", "search.html"}
            and not rel.startswith("_static/")
        ):
            headings = page.headings
            if sum(level == 1 for level, _ in headings) != 1:
                errors.append(f"{rel}: expected one H1")
            previous = 0
            for level, title in headings:
                if level > 4 or level > previous + 1:
                    errors.append(f"{rel}: heading level H{level}: {title}")
                previous = level
            for code in page.blocks:
                try:
                    ast.parse(code)
                    checked_examples += 1
                except SyntaxError as exc:
                    errors.append(f"{rel}: Python example line {exc.lineno}: {exc.msg}")
    # Check both historical entry points and their documented destinations.
    for entry in manifest:
        for field in ("old", "new"):
            target, fragment = resolve(root, root / "index.html", entry[field])
            if target not in pages or (fragment and fragment not in pages[target].ids):
                errors.append(f"legacy manifest: unresolved {field}: {entry[field]}")
    # Reachability is based on the source toctree, not sidebar links on orphan pages.
    docs = repo / "docs"
    visited = set()

    def visit(name):
        if name in visited:
            return
        visited.add(name)
        source = docs / (name + ".rst")
        if not source.exists():
            errors.append(f"navigation: missing {name}")
            return
        text = source.read_text()
        for block in re.findall(r"^\.\. toctree::\n((?:[ \t].*\n|\n)*)", text, re.M):
            for line in block.splitlines():
                line = line.strip()
                if not line or line.startswith(":"):
                    continue
                line = re.sub(r"^.*<(.+)>$", r"\1", line)
                if "://" not in line:
                    dest = (
                        line.lstrip("/")
                        if line.startswith("/")
                        else str(Path(name).parent / line)
                    )
                    visit(dest)

    visit("index")
    # Reading order is intentional and independent of sector execution order.
    expected_orders = {
        "methodology/index": [
            "workflow",
            "shared-principles",
            "regionalization",
            "electricity/index",
            "fuels/index",
            "heat",
            "batteries",
            "mining",
            "metals",
            "steel",
            "cement",
            "transport/index",
            "cdr",
            "emissions",
            "validation",
            "system-models",
        ],
        "methodology/electricity/index": ["generation", "photovoltaics", "markets"],
        "methodology/fuels/index": [
            "/methodology/biomass",
            "hydrogen",
            "biofuels",
            "synthetic",
            "ammonia",
            "markets",
        ],
        "methodology/transport/index": [
            "overview",
            "two-wheelers",
            "passenger-cars",
            "trucks",
            "buses",
            "rail",
            "shipping",
        ],
        "methodology/heat": ["final-energy"],
        "user_guide/index": [
            "choose-scenario",
            "source-databases",
            "external-scenarios",
            "create-external-scenario",
            "updates",
            "inventories",
            "reports",
            "validation",
            "interpreting-results",
            "export/index",
            "examples",
            "troubleshooting",
        ],
        "development/index": [
            "releases-2-5",
            "contributing",
            "documentation",
            "iam-mapping",
            "heat-mapping",
            "architecture",
            "internals",
            "validation-internals",
            "supplier-selection",
            "performance-history",
            "consequential-validation-history",
            "documentation-audit",
            "scientific-update-backlog",
        ],
    }
    for name, expected in expected_orders.items():
        parent = Path(name).parent
        expected = [
            x.lstrip("/") if x.startswith("/") else str(parent / x) for x in expected
        ]
        actual = []
        source_text = (docs / (name + ".rst")).read_text()
        for block in re.findall(
            r"^\.\. toctree::\n((?:[ \t].*\n|\n)*)", source_text, re.M
        ):
            for line in block.splitlines():
                line = line.strip()
                if line and not line.startswith(":"):
                    actual.append(
                        line.lstrip("/") if line.startswith("/") else str(parent / line)
                    )
        if actual != expected:
            errors.append(f"navigation: unexpected reading order in {name}: {actual}")
    captions = re.findall(
        r"^   :caption: (.+)$", (docs / "methodology/index.rst").read_text(), re.M
    )
    if captions != [
        "Foundations",
        "Energy supply and storage",
        "Materials and industry",
        "Transport",
        "Carbon management",
        "Assessment and limitations",
        "Consequential modelling",
    ]:
        errors.append("navigation: unexpected methodology groups")
    for source in docs.rglob("*.rst"):
        name = source.relative_to(docs).with_suffix("").as_posix()
        if (
            any(
                part.startswith("_") or part.startswith(".")
                for part in source.relative_to(docs).parts
            )
            or name + ".html" in legacy
        ):
            continue
        if name not in visited:
            errors.append(f"navigation: unreachable {name}")
    api = pages.get(root / "reference/api.html")
    for ident in (
        "premise.NewDatabase",
        "premise.NewDatabase.update",
        "premise.NewDatabase.get_inventory_store",
        "premise.NewDatabase.get_validation_report",
        "premise.NewDatabase.write_db_to_brightway",
        "premise.NewDatabase.write_scenario_array_db_to_brightway",
        "premise.InventoryStore",
        "premise.ChangeReportArtifacts",
    ):
        if not api or ident not in api.ids:
            errors.append(f"API: missing {ident}")
    api_text = (root / "reference/api.html").read_text()
    for term in (
        "source_version",
        "scenario",
        "writable",
        "Read-only access is the default",
    ):
        if term not in api_text:
            errors.append(f"API: missing signature/description text {term}")
    for path in (docs / "examples").glob("*.py"):
        ast.parse(path.read_text(), filename=str(path))
    for error in sorted(set(errors)):
        print(error)
    print(
        f"Checked {len(pages)} pages, {len(manifest)} historical anchors, {checked_examples} Python examples; {len(set(errors))} errors."
    )
    return bool(errors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build_dir", type=Path)
    args = parser.parse_args()
    sys.exit(check(args.build_dir, Path(__file__).resolve().parents[1]))
