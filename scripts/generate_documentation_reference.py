"""Generate drift-checked reference tables from source, without licensed inputs.

Use --check in CI. The inventory table describes constructor selection, not
post-import filtering or successful availability in a built database.
"""

import argparse
import ast
from pathlib import Path
from types import SimpleNamespace

import yaml
from packaging.version import Version

ROOT = Path(__file__).resolve().parents[1]


def table(headers, rows):
    lines = [".. list-table::", "   :header-rows: 1", ""]
    for row in [headers, *rows]:
        lines.append("   * - " + str(row[0]))
        lines.extend("     - " + str(value).replace("\n", " ") for value in row[1:])
    return "\n".join(lines) + "\n"


def inventory_selection(tree, version, system_model):
    """Execute only the existing, isolated selection statements, never imports."""
    cls = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "NewDatabase"
    )
    method = next(
        n
        for n in cls.body
        if isinstance(n, ast.FunctionDef) and n.name == "__import_inventories"
    )
    statements = method.body
    start = next(
        i
        for i, n in enumerate(statements)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "filepaths" for t in n.targets)
    )
    end = next(
        i
        for i, n in enumerate(statements)
        if isinstance(n, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "preloaded_importers" for t in n.targets
        )
    )
    env = {
        "self": SimpleNamespace(version=version, system_model=system_model),
        "Version": Version,
    }
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id.startswith("FILEPATH_")
            for t in node.targets
        ):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    strings = [
                        v.value
                        for v in ast.walk(node.value)
                        if isinstance(v, ast.Constant)
                        and isinstance(v.value, str)
                        and v.value.endswith(".xlsx")
                    ]
                    if strings:
                        env[t.id] = strings[-1]
    pv_tree = ast.parse((ROOT / "premise/photovoltaic.py").read_text())
    fn = next(
        n
        for n in pv_tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "use_pv_2026"
    )
    # Function annotations use only builtins; its logic is self-contained.
    exec(
        compile(ast.Module(body=[fn], type_ignores=[]), "photovoltaic.py", "exec"), env
    )
    exec(
        compile(
            ast.Module(body=statements[start:end], type_ignores=[]),
            "inventory-selection",
            "exec",
        ),
        env,
    )
    return env["selected_filepaths"]


def generate():
    constants = yaml.safe_load(
        (ROOT / "premise/iam_variables_mapping/constants.yaml").read_text()
    )
    tree = ast.parse((ROOT / "premise/new_database.py").read_text())
    cls = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "NewDatabase"
    )
    init = next(
        n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "__init__"
    )
    defaults = dict(
        zip(
            [a.arg for a in init.args.args][-len(init.args.defaults) :],
            init.args.defaults,
        )
    )
    names = [
        "source_version",
        "system_model",
        "inventory_backend",
        "use_absolute_efficiency",
        "gains_scenario",
        "keep_source_db_uncertainty",
        "keep_imports_uncertainty",
        "generate_reports",
    ]
    result = {
        "defaults.inc": table(
            ["Argument", "Default"],
            [(f"``{n}``", f"``{ast.unparse(defaults[n])}``") for n in names],
        ),
        "supported.inc": table(
            ["Configuration", "Configured values"],
            [
                (name, ", ".join(f"``{v}``" for v in constants[key]))
                for name, key in [
                    ("Ecoinvent versions", "SUPPORTED_EI_VERSIONS"),
                    ("Model identifiers", "SUPPORTED_MODELS"),
                ]
            ],
        ),
    }
    densities = yaml.safe_load(
        (ROOT / "premise/data/battery/energy_density.yaml").read_text()
    )
    result["battery-density.inc"] = table(
        [
            "Chemistry",
            "2020 mean (kWh/kg cell)",
            "2050 mean (kWh/kg cell)",
            "2050 mass factor",
        ],
        [
            [
                name,
                entry["target"][2020]["mean"],
                entry["target"][2050]["mean"],
                f'{entry["target"][2020]["mean"] / entry["target"][2050]["mean"]:.4f}',
            ]
            for name, entry in densities.items()
        ],
    )
    configurations = []
    selections = {}
    for version in constants["SUPPORTED_EI_VERSIONS"]:
        for model in ["cutoff", "consequential"]:
            if model == "consequential" and Version(version) < Version("3.8"):
                continue
            key = f"{version} {model}"
            configurations.append(key)
            selections[key] = inventory_selection(tree, version, model)
    files = sorted({(f, v) for values in selections.values() for f, v in values})
    rows = []
    for filename, source_version in files:
        selected = [
            k for k in configurations if (filename, source_version) in selections[k]
        ]
        rows.append(
            (
                f"``{filename}``",
                source_version,
                (
                    "All supported configurations"
                    if len(selected) == len(configurations)
                    else "; ".join(selected)
                ),
            )
        )
    result["inventory-selection.inc"] = table(
        ["Workbook", "Workbook source version", "Selected for"], rows
    )
    mappings = {
        "Electricity": "electricity",
        "Heat": "heat",
        "Steel": "steel",
        "Cement": "cement",
        "Fuels": "fuels",
        "Biomass": "biomass",
        "Road freight": "transport_road_freight",
        "Passenger cars": "transport_passenger_cars",
        "Buses": "transport_bus",
        "Two-wheelers": "transport_two_wheelers",
        "Rail freight": "transport_rail_freight",
        "Sea freight": "transport_sea_freight",
        "End-use heating": "final_energy",
        "CDR": "carbon_dioxide_removal",
    }
    rows = []
    for sector, filename in mappings.items():
        data = yaml.safe_load(
            (ROOT / f"premise/iam_variables_mapping/{filename}.yaml").read_text()
        )
        aliases = set()

        def collect(value):
            if isinstance(value, dict):
                if isinstance(value.get("iam_aliases"), dict):
                    aliases.update(
                        k for k, v in value["iam_aliases"].items() if v is not None
                    )
                for v in value.values():
                    collect(v)
            elif isinstance(value, list):
                for v in value:
                    collect(v)

        collect(data)
        rows.append(
            [
                sector,
                *[
                    "Yes" if m in aliases else "No"
                    for m in constants["SUPPORTED_MODELS"]
                ],
            ]
        )
    result["mapping-coverage.inc"] = table(
        ["Sector", *constants["SUPPORTED_MODELS"]], rows
    )
    register = yaml.safe_load(
        (ROOT / "docs/reference/source-register.yaml").read_text()
    )
    for record in register["sectors"]:
        rows = [
            ["Source/data years", record["data_years"]],
            ["Boundary", record["boundary"]],
            ["Applicability", record["applicability"]],
        ]
        result[f"source-{record['id']}.inc"] = (
            table(["Source information", "Details"], rows)
            + f"\nSource context: `publication or data provider <{record['source']}>`__.\n"
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    directory = ROOT / "docs/reference/generated"
    failures = []
    for name, content in generate().items():
        path = directory / name
        if args.check:
            if not path.exists() or path.read_text() != content:
                failures.append(str(path.relative_to(ROOT)))
        else:
            directory.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    if failures:
        raise SystemExit(
            "Regenerate documentation reference tables: " + ", ".join(failures)
        )
    print("Reference tables checked." if args.check else "Reference tables generated.")


if __name__ == "__main__":
    main()
