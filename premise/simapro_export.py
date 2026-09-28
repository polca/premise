"""Export complete prepared scenarios through Brightpath's SimaPro writer."""

from collections import Counter
from dataclasses import asdict
import json
import math
from pathlib import Path
import sys
import tempfile
import warnings

from .export_payload import normalize_activity_parameters
from .olca_export import _identity, _plain, build_process_categories


def check_brightpath():
    """Fail before scenario preparation when the optional writer is unavailable."""
    if sys.version_info < (3, 12):
        raise RuntimeError("Brightpath SimaPro export requires Python 3.12+.")
    try:
        from brightpath.formats.simapro_csv import write_simapro_csv  # noqa: F401
        from brightpath.profiles.simapro_waste import (
            resolve_classified_waste,
        )  # noqa: F401
        from brightpath.utils import get_simapro_units
    except ImportError as error:
        raise ImportError(
            "Install Brightpath with SimaPro classification and full-inventory "
            "unit support in this Python environment (see the SimaPro export guide)."
        ) from error
    if (
        not {"kilogram day", "cubic meter-year", "guest night"}
        <= get_simapro_units().keys()
    ):
        raise ImportError(
            "Update Brightpath to a version with full-inventory SimaPro unit "
            "mappings (see the SimaPro export guide)."
        )


def build_document(scenario, version, system_model):
    """Prepare a detached inventory and record classification and flow coverage."""
    from brightpath.core import (
        BackgroundContext,
        BiosphereProfile,
        FormatProfile,
        InventoryContext,
        TechnosphereProfile,
    )
    from brightpath.models import InventoryDocument
    from brightpath.profiles.simapro_waste import resolve_classified_waste
    from brightpath.utils import is_blacklisted

    from .export import get_simapro_category_of_exchange, resolve_simapro_category

    data = [_plain(dataset) for dataset in scenario["database"]]
    suppliers = set()
    for dataset in data:
        identity = _identity(dataset)
        if not all(isinstance(value, str) and value for value in identity):
            raise ValueError(f"Incomplete SimaPro activity identity: {identity!r}.")
        if identity in suppliers:
            raise ValueError(f"Ambiguous SimaPro supplier identity: {identity!r}.")
        suppliers.add(identity)

    categories = get_simapro_category_of_exchange()
    fallback_classifications = []
    excluded = []
    classification_counts = Counter()
    for dataset in data:
        identity = _identity(dataset)
        production = [e for e in dataset["exchanges"] if e.get("type") == "production"]
        if len(production) != 1:
            raise ValueError(f"{identity!r} must have exactly one production exchange.")
        output = production[0]
        if not output.get("amount"):
            raise ValueError(f"Zero or missing production amount: {identity!r}.")
        for key in ("name", "reference product", "location", "unit"):
            output.setdefault(
                key,
                output.get("product") if key == "reference product" else dataset[key],
            )
        if not output.get("reference product"):
            output["reference product"] = dataset["reference product"]
        if _identity(output) != identity:
            raise ValueError(
                f"Production identity differs from activity: {identity!r}."
            )
        if (
            not isinstance(dataset.get("comment"), str)
            or not dataset["comment"].strip()
        ):
            dataset["comment"] = "Exported by premise (no source comment)."
        normalize_activity_parameters(dataset)
        resolution = resolve_classified_waste(dataset)
        classification_counts[resolution.rule] += 1
        if resolution.waste is None:
            entry = categories.get(
                (dataset["name"].lower(), dataset["reference product"].lower())
            )
            mapped = bool(
                entry
                and (entry.get("category") or "").strip()
                and (entry.get("sub_category") or "").strip()
            )
            main, sub = resolve_simapro_category(
                dataset["name"], dataset["reference product"], categories
            )
            category = main + "/" + sub.replace("\\", "/")
            output["simapro category"] = category
            fallback_classifications.append(
                {
                    "activity": list(identity),
                    "category": category,
                    "reason": resolution.rule,
                    "source": "premise_mapping" if mapped else "default_category",
                }
            )

        retained = []
        for index, exchange in enumerate(dataset["exchanges"]):
            amount = exchange.get("amount")
            if not isinstance(amount, (int, float)) or not math.isfinite(amount):
                raise ValueError(f"Invalid exchange amount in {identity!r}.")
            if exchange["type"] == "technosphere":
                exchange["reference product"] = exchange.get(
                    "reference product"
                ) or exchange.get("product")
                if _identity(exchange) not in suppliers:
                    raise ValueError(
                        f"Unresolved SimaPro supplier {_identity(exchange)!r} in {identity!r}."
                    )
                if is_blacklisted(exchange, "ecoinvent"):
                    raise ValueError(
                        f"Brightpath would exclude technosphere supplier {_identity(exchange)!r}."
                    )
            indicator = (
                exchange["type"] == "biosphere"
                and (exchange.get("categories") or [None])[0] == "inventory indicator"
            )
            blacklisted = exchange["type"] == "biosphere" and is_blacklisted(
                exchange, "ecoinvent"
            )
            if indicator or blacklisted:
                excluded.append(
                    {
                        "activity": list(identity),
                        "exchange_index": index,
                        "name": exchange["name"],
                        "categories": exchange.get("categories"),
                        "unit": exchange["unit"],
                        "amount": amount,
                        "reason": (
                            "inventory_indicator"
                            if indicator
                            else "brightpath_blacklist"
                        ),
                    }
                )
            if not indicator:
                retained.append(exchange)
        dataset["exchanges"] = retained

    assign_simapro_category_paths(data)
    metadata = assign_simapro_provenance(data, scenario, version, system_model)
    context = InventoryContext(
        format=FormatProfile("simapro_csv"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", str(version), system_model),
            biosphere=BiosphereProfile("ecoinvent", str(version)),
        ),
    )
    document = InventoryDocument(
        data=data,
        context=context,
        metadata=metadata,
        database_name=f"premise_{scenario['model']}_{scenario['pathway']}_{scenario['year']}",
    )
    return document, {
        "scenario": {key: scenario[key] for key in ("model", "pathway", "year")},
        "source_version": str(version),
        "system_model": system_model,
        "classification_rules": dict(classification_counts),
        "classification_fallbacks": fallback_classifications,
        "excluded_exchanges": excluded,
    }


def export_scenario(scenario, filepath, version, system_model):
    """Write CSV and a review report only after successful Brightpath rendering."""
    check_brightpath()
    from brightpath import __version__ as brightpath_version
    from brightpath.formats.simapro_csv import write_simapro_csv

    document, report = build_document(scenario, version, system_model)
    directory = Path(filepath)
    directory.mkdir(parents=True, exist_ok=True)
    destination = (
        directory
        / f"simapro_export_{scenario['model']}_{scenario['pathway']}_{scenario['year']}.csv"
    )
    report_path = destination.with_suffix(".export-report.json")
    with tempfile.TemporaryDirectory(prefix=".simapro-", dir=directory) as temporary:
        staged = Path(temporary) / destination.name
        _, result = write_simapro_csv(
            document, staged, category_mode="infer_classifications"
        )
        report["brightpath_version"] = brightpath_version
        report["issues"] = [asdict(issue) for issue in result.issues]
        report["issue_counts"] = dict(Counter(issue.code for issue in result.issues))
        staged_report = staged.with_suffix(".export-report.json")
        staged_report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        staged_report.replace(report_path)
        staged.replace(destination)
    if report["excluded_exchanges"] or report["classification_fallbacks"]:
        warnings.warn(
            f"SimaPro export recorded {len(report['excluded_exchanges'])} excluded "
            f"biosphere exchanges and {len(report['classification_fallbacks'])} "
            f"classification fallbacks. Review {report_path}.",
            UserWarning,
            stacklevel=2,
        )
    return destination


def assign_simapro_provenance(
    datasets, scenario, version, system_model, *, brightpath_version=None
):
    """Fill missing provenance on a detached payload and return document metadata.

    Pass the returned dictionary as ``metadata`` to Brightpath's ``from_data``.
    Existing non-empty process metadata and source comments are preserved.
    """
    from . import __version__

    if brightpath_version is None:
        from brightpath import __version__ as brightpath_version

    premise_version = ".".join(map(str, __version__))
    generator = f"premise {premise_version}; Brightpath {brightpath_version}"
    label = (
        f"ecoinvent {version} ({system_model}); "
        f"{scenario['model']} / {scenario['pathway']} / {scenario['year']}"
    )
    documentation = "https://premise.readthedocs.io/en/latest/introduction.html"
    defaults = {
        "Generator": generator,
        "System description": label,
        "External documents": documentation,
    }
    for dataset in datasets:
        metadata = dataset.get("simapro metadata")
        if metadata is None:
            metadata = dataset["simapro metadata"] = {}
        if not isinstance(metadata, dict):
            raise ValueError("simapro metadata must be a dictionary.")
        for field, value in defaults.items():
            # Brightpath gives top-level fields precedence over native metadata.
            top_level = field.lower()
            if dataset.get(top_level) not in (None, ""):
                continue
            if metadata.get(field) in (None, ""):
                metadata[field] = value
            if top_level in dataset:
                dataset[top_level] = metadata[field]
    return {
        "system description": {
            "name": label,
            "description": f"Prepared by {generator}. Scenario: {label}. {documentation}",
        }
    }


def assign_simapro_category_paths(datasets):
    """Assign the openLCA ISIC hierarchy to prepared SimaPro datasets in place.

    Call on the detached export payload. Only folder metadata is assigned;
    production categories and waste-identification evidence are left untouched.
    Missing ISIC uses the same consensus/CPC/Unclassified fallbacks as openLCA.
    """
    categories = build_process_categories(datasets)
    for dataset in datasets:
        dataset["simapro category path"] = categories[_identity(dataset)]
