"""Render portable, accessible SVG process diagrams from reviewed sector steps.

No browser JavaScript, network request or external rendering tool is required.
Use --check in CI to detect changes to the diagram definitions.
"""

import argparse
import html
import json
from pathlib import Path
import textwrap

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/methodology/process-diagrams.json"
OUTPUT = ROOT / "docs/_static/process-diagrams"


def render(spec):
    parts = []
    y = 16

    def box(title, detail, fill, stroke, number=None):
        nonlocal y
        titles = textwrap.wrap(title, width=51, break_long_words=False)
        details = textwrap.wrap(detail, width=65, break_long_words=False)
        height = 22 + 24 * len(titles) + 20 * len(details)
        parts.append(
            f'<rect x="20" y="{y}" width="600" height="{height}" rx="9" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>'
        )
        if number is not None:
            parts.append(
                f'<circle cx="44" cy="{y + 26}" r="12" fill="#245b78"/>'
                f'<text x="44" y="{y + 31}" text-anchor="middle" '
                f'font-size="13" fill="white">{number}</text>'
            )
        x = 66 if number is not None else 38
        for line in titles:
            parts.append(
                f'<text x="{x}" y="{y + 31}" font-size="18" font-weight="600">'
                f"{html.escape(line)}</text>"
            )
            y += 24
        for line in details:
            parts.append(
                f'<text x="{x}" y="{y + 28}" font-size="15" fill="#425466">'
                f"{html.escape(line)}</text>"
            )
            y += 20
        y += 22

    if spec.get("guard"):
        box("Data availability", spec["guard"], "#fff9e9", "#b9974f")
        y += 16
    for i, step in enumerate(spec["steps"], 1):
        if i > 1:
            parts.append(
                f'<path d="M320 {y} v24" stroke="#59768a" stroke-width="2" '
                'fill="none" marker-end="url(#arrow)"/>'
            )
            y += 36
        box(step["title"], step["detail"], "#f0f6fa", "#96b2c4", i)
    description = "; ".join(f'{s["title"]}: {s["detail"]}' for s in spec["steps"])
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="640" height="{y + 16}" '
        f'viewBox="0 0 640 {y + 16}" role="img" aria-labelledby="title description" '
        'font-family="Arial, Helvetica, sans-serif" fill="#203645">'
        f'<title id="title">{html.escape(spec["title"])}</title>'
        f'<desc id="description">{html.escape(spec.get("guard", "") + " " + description)}</desc>'
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
        '<path d="M0 0 L10 5 L0 10 Z" fill="#59768a"/></marker></defs>'
        + "".join(parts)
        + "</svg>\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    specs = json.loads(SPEC.read_text())
    stale = []
    if not args.check:
        OUTPUT.mkdir(parents=True, exist_ok=True)
    for name, spec in specs.items():
        target = OUTPUT / (name.replace("/", "-") + ".svg")
        svg = render(spec)
        if args.check:
            if not target.exists() or target.read_text() != svg:
                stale.append(str(target.relative_to(ROOT)))
        else:
            target.write_text(svg)
    if stale:
        raise SystemExit("Out-of-date process diagrams: " + ", ".join(stale))
    print(f"{'Checked' if args.check else 'Generated'} {len(specs)} process diagrams.")


if __name__ == "__main__":
    main()
