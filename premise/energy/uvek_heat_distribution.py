"""Plan native district-delivery copies while retaining actual network quantities."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from numbers import Real

from ..core.filesystem_constants import DATA_DIR
from ..inventory.profiles import identity

REVIEW_PATH = DATA_DIR / "heat" / "uvek2026_distribution_review.json"
DELIVERY_NAME = "district heat delivery, native UVEK network"
DELIVERY_PRODUCT = "heat, delivered district"
SECONDARY_NAME = "market for heat, secondary, district or industrial"
SECONDARY_PRODUCT = "heat, district or industrial"


def _one_MJ_reference(dataset):
    outputs = [e for e in dataset["exchanges"] if e["type"] == "production"]
    if (
        dataset["unit"] != "megajoule"
        or len(outputs) != 1
        or identity(outputs[0]) != identity(dataset)
        or not isinstance(outputs[0]["amount"], Real)
        or isinstance(outputs[0]["amount"], bool)
        or outputs[0]["amount"] != 1
    ):
        raise ValueError(
            "District delivery requires a matching positive +1MJ reference."
        )


def build_native_district_delivery(
    database,
    secondary_markets,
    *,
    model,
    pathway,
    year,
    version="2026",
    system_model="cutoff",
    biosphere_version="3.10",
):
    """Return owned delivery plans; do not mutate or append to the source graph.

    Secondary markets may be planned records not yet appended to ``database``.
    They must belong to the requested scenario and supply one MJ. Only the
    reviewed plant-heat edge changes supplier. Actual losses, infrastructure,
    signed quantities, formulas and uncertainty remain in the copied network.
    """
    if (str(version), system_model, biosphere_version) != ("2026", "cutoff", "3.10"):
        raise ValueError("Native heat delivery requires UVEK2026cutoff/biosphere3.10.")
    review = json.loads(REVIEW_PATH.read_text())
    if review.get("schema_version") != 1:
        raise ValueError("Unsupported native district-delivery review schema.")
    candidates = [a for a in database if identity(a) == identity(review["source"])]
    if len(candidates) != 1:
        raise ValueError(
            "Missing or ambiguous exact native district-delivery prototype."
        )
    source = candidates[0]
    if source.get("clic provenance") or source.get("brightpath_conversion"):
        raise ValueError("A foreground/helper cannot impersonate the native network.")
    _one_MJ_reference(source)
    heat = [
        (i, e)
        for i, e in enumerate(source["exchanges"])
        if e["type"] == "technosphere" and identity(e) == identity(review["heat_input"])
    ]
    network = [
        e
        for e in source["exchanges"]
        if e["type"] == "technosphere"
        and identity(e) == identity(review["network_input"])
    ]
    if len(heat) != 1 or len(network) != 1:
        raise ValueError("Native district heat/network boundary disagrees with review.")
    if any(
        isinstance(e["amount"], bool)
        or not isinstance(e["amount"], Real)
        or not math.isfinite(e["amount"])
        or e["amount"] <= 0
        for e in (heat[0][1], network[0])
    ):
        raise ValueError(
            "Native distribution heat and network inputs must be positive."
        )
    scenario = {"model": model, "pathway": pathway, "year": year}
    plans, regions = [], set()
    for target in secondary_markets:
        _one_MJ_reference(target)
        marker = target.get("premise heat", {})
        if (
            target["name"] != SECONDARY_NAME
            or target["reference product"] != SECONDARY_PRODUCT
            or marker.get("kind") != "uvek_iam_market"
            or marker.get("layer") != "secondary_supply"
            or marker.get("scenario") != scenario
            or not target.get("database")
            or not target.get("code")
        ):
            raise ValueError(
                "District supply must be this scenario's owned secondary market."
            )
        region = target["location"]
        if region in regions:
            raise ValueError(
                "Duplicate regional secondary market in district-delivery plan."
            )
        regions.add(region)
        inputs = [e for e in target["exchanges"] if e["type"] == "technosphere"]
        if (
            not inputs
            or any(not math.isfinite(e["amount"]) or e["amount"] <= 0 for e in inputs)
            or not math.isclose(
                math.fsum(e["amount"] for e in inputs), 1, rel_tol=1e-12
            )
        ):
            raise ValueError(
                "Secondary heat market must have a positive unit supplier mix."
            )
        token = json.dumps([model, pathway, year, region], separators=(",", ":"))
        code = "uvek-heat-delivery-" + hashlib.sha256(token.encode()).hexdigest()[:32]
        result = copy.deepcopy(source)
        result.update(
            name=DELIVERY_NAME,
            **{
                "reference product": DELIVERY_PRODUCT,
                "location": region,
                "database": target["database"],
                "code": code,
            },
        )
        result.pop("product", None)
        for exchange in result["exchanges"]:
            if "output" in exchange:
                exchange["output"] = (result["database"], code)
            if exchange["type"] == "production":
                exchange.pop("reference product", None)
                exchange.update(
                    name=DELIVERY_NAME,
                    product=DELIVERY_PRODUCT,
                    location=region,
                    input=(result["database"], code),
                )
        index, original = heat[0]
        replacement = result["exchanges"][index]
        replacement.pop("reference product", None)
        replacement.update(
            name=target["name"],
            product=target["reference product"],
            location=region,
            input=(target["database"], target["code"]),
        )
        result["comment"] = (
            "Scenario-owned copy: retain native district losses/network inventory; "
            "replace only plant heat with the regional IAM secondary mix. "
            "The following source description refers to the historical prototype. "
            + source.get("comment", "")
        )
        result["premise heat"] = {
            "kind": "uvek_native_district_delivery",
            "scenario": scenario,
            "source": identity(source),
            "secondary_market": identity(target),
            "substitution": {
                "original_index": index,
                "source_exchange": copy.deepcopy(original),
                "target_amount": replacement["amount"],
                "basis": "unchanged signed MJ and native distribution boundary",
            },
            "limitations": copy.deepcopy(review["limitations"]),
            "review": "premise/data/heat/uvek2026_distribution_review.json",
        }
        plans.append(result)
    return plans
