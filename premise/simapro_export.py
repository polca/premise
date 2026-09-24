"""Premise metadata preparation for Brightpath SimaPro exports."""

from .olca_export import _identity, build_process_categories


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
