"""
Phase out ozone-depleting substances according to Montreal Protocol schedules.
"""

from __future__ import annotations

import copy
import re
from functools import lru_cache
from typing import Any, Optional

import yaml

from .filesystem_constants import DATA_DIR
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    List,
    redefine_uncertainty_params,
)

OZONE_DATA_DIR = DATA_DIR / "ozone"
PHASEOUT_SCHEDULES = OZONE_DATA_DIR / "phaseout_schedules.yaml"
ARTICLE5_PARTIES = OZONE_DATA_DIR / "article5_parties.yaml"
ODS_SUBSTANCES = OZONE_DATA_DIR / "ods_substances.yaml"
SUBSTITUTES = OZONE_DATA_DIR / "substitutes.yaml"

logger = create_logger("ozone")


def _update_ozone(scenario, version, system_model):
    """
    Update ozone-depleting substances in a scenario database.
    """

    ozone = OzoneDepletingSubstances(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    ozone.update_database()

    scenario["database"] = ozone.database
    scenario["cache"] = ozone.cache
    scenario["index"] = ozone.index

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["ozone"] = ozone.summary

    return scenario


@lru_cache
def fetch_mapping(filepath: str) -> dict:
    """Return a dictionary from a YAML file."""

    with open(filepath, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(text).lower())


def _contains_alias(text: str, alias: str) -> bool:
    text_tokens = _tokens(text)
    alias_tokens = _tokens(alias)

    if not alias_tokens:
        return False

    if len(alias_tokens) == 1:
        return alias_tokens[0] in text_tokens

    size = len(alias_tokens)
    return any(
        text_tokens[i : i + size] == alias_tokens
        for i in range(len(text_tokens))
    )


def _contains_keyword(text: str, keyword: str) -> bool:
    return str(keyword).lower() in str(text).lower()


def _clamp_allowed_fraction(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


class OzoneDepletingSubstances(BaseTransformation):
    """
    Replace or scale ozone-depleting substances according to Montreal Protocol
    control schedules.
    """

    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        cache: dict = None,
        index: dict = None,
    ) -> None:
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
            cache,
            index,
        )

        self.schedules = fetch_mapping(str(PHASEOUT_SCHEDULES))
        self.party_groups = fetch_mapping(str(ARTICLE5_PARTIES))
        self.ods_config = fetch_mapping(str(ODS_SUBSTANCES))
        self.substitute_config = fetch_mapping(str(SUBSTITUTES))

        self.article5_locations = set(self.party_groups.get("article5_iso2", []))
        self.non_article5_locations = set(
            self.party_groups.get("non_article5_locations", [])
        )
        self.mixed_locations = set(self.party_groups.get("mixed_locations", []))
        self.mixed_location_fallback = self.party_groups.get(
            "mixed_location_fallback", "article5"
        )

        self.substances = self.ods_config.get("substances", {})
        self.exemption_keywords = self.ods_config.get("exemption_keywords", {})
        self.applications = self.substitute_config.get("applications", {})
        self.summary = {
            "technosphere substituted": 0,
            "technosphere unresolved": 0,
            "biosphere scaled": 0,
            "exempt": 0,
        }

    def update_database(self) -> None:
        """Update all datasets in the database in place."""

        for dataset in self.database:
            self.update_dataset(dataset)

    def update_dataset(self, dataset: dict) -> None:
        """Update ODS exchanges in a dataset."""

        new_exchanges = []
        dataset_changed = False

        for exchange in dataset.get("exchanges", []):
            exchange_type = exchange.get("type")

            if exchange_type == "technosphere":
                updated_exchanges, changed = self.update_technosphere_exchange(
                    dataset, exchange
                )
                new_exchanges.extend(updated_exchanges)
                dataset_changed = dataset_changed or changed

            elif exchange_type == "biosphere":
                changed = self.update_biosphere_exchange(dataset, exchange)
                new_exchanges.append(exchange)
                dataset_changed = dataset_changed or changed

            else:
                new_exchanges.append(exchange)

        if dataset_changed:
            dataset["exchanges"] = new_exchanges

    def update_technosphere_exchange(
        self, dataset: dict, exchange: dict
    ) -> tuple[list[dict], bool]:
        """Replace the phased-out share of a technosphere ODS exchange."""

        substance = self.find_substance(exchange)
        if substance is None:
            return [exchange], False

        if self.is_exempt(dataset, exchange, substance):
            self.summary["exempt"] += 1
            self.write_log(dataset, exchange, substance, status="exempt")
            return [exchange], False

        application = self.infer_application(dataset, exchange, substance)
        allowed_fraction = self.get_allowed_fraction(
            group=substance["group"],
            location=exchange.get("location") or dataset.get("location"),
            application=application,
        )

        if allowed_fraction >= 1.0:
            return [exchange], False

        amount = float(exchange.get("amount", 0.0))
        replacement_amount = amount * (1.0 - allowed_fraction)
        residual_amount = amount * allowed_fraction

        if replacement_amount == 0.0:
            return [exchange], False

        candidate, provider = self.find_substitute_provider(
            application=application,
            substance=substance,
            dataset=dataset,
            exchange=exchange,
        )

        if provider is None:
            self.summary["technosphere unresolved"] += 1
            self.write_log(
                dataset,
                exchange,
                substance,
                application=application,
                allowed_fraction=allowed_fraction,
                status="unresolved substitute",
            )
            return [exchange], False

        updated_exchanges = []

        if abs(residual_amount) > 0:
            updated_exchanges.append(
                self.copy_exchange_with_amount(exchange, residual_amount)
            )

        replacement_ratio = float(candidate.get("replacement_ratio", 1.0))
        substitute = self.copy_exchange_with_amount(
            exchange, replacement_amount * replacement_ratio
        )
        substitute["name"] = provider["name"]
        substitute["product"] = provider["reference product"]
        substitute["location"] = provider["location"]
        substitute["unit"] = provider["unit"]
        substitute.pop("input", None)

        updated_exchanges.append(substitute)

        self.summary["technosphere substituted"] += 1
        self.write_log(
            dataset,
            exchange,
            substance,
            application=application,
            allowed_fraction=allowed_fraction,
            substitute=substitute,
            status="substituted",
        )

        return updated_exchanges, True

    def update_biosphere_exchange(self, dataset: dict, exchange: dict) -> bool:
        """Scale biosphere ODS emissions according to the phase-out schedule."""

        substance = self.find_substance(exchange)
        if substance is None:
            return False

        if self.is_exempt(dataset, exchange, substance):
            self.summary["exempt"] += 1
            self.write_log(dataset, exchange, substance, status="exempt")
            return False

        application = self.infer_application(dataset, exchange, substance)
        allowed_fraction = self.get_allowed_fraction(
            group=substance["group"],
            location=dataset.get("location"),
            application=application,
        )

        if allowed_fraction >= 1.0:
            return False

        old_amount = float(exchange.get("amount", 0.0))
        if old_amount == 0.0:
            return False

        updated = self.copy_exchange_with_amount(
            exchange, old_amount * allowed_fraction
        )
        exchange.clear()
        exchange.update(updated)

        self.summary["biosphere scaled"] += 1
        self.write_log(
            dataset,
            exchange,
            substance,
            application=application,
            allowed_fraction=allowed_fraction,
            status="biosphere scaled",
        )

        return True

    def copy_exchange_with_amount(self, exchange: dict, amount: float) -> dict:
        """Copy an exchange and rescale compatible uncertainty fields."""

        new_exchange = copy.deepcopy(exchange)
        new_exchange["amount"] = amount

        if amount == 0.0:
            new_exchange["uncertainty type"] = 0
            for key in ["loc", "scale", "minimum", "maximum", "negative"]:
                new_exchange.pop(key, None)
            return new_exchange

        if exchange.get("uncertainty type", 0) == 0:
            return new_exchange

        loc, scale, minimum, maximum, negative = redefine_uncertainty_params(
            exchange, new_exchange
        )

        for key, value in {
            "loc": loc,
            "scale": scale,
            "minimum": minimum,
            "maximum": maximum,
            "negative": negative,
        }.items():
            if value is not None:
                new_exchange[key] = value

        return new_exchange

    def find_substance(self, exchange: dict) -> Optional[dict[str, Any]]:
        """Find an ODS substance matching an exchange name or product."""

        text = " ".join(
            str(exchange.get(field, ""))
            for field in (
                "name",
                "product",
                "reference product",
                "categories",
            )
        )
        matches = []

        for substance_id, substance in self.substances.items():
            for alias in substance.get("aliases", []):
                if _contains_alias(text, alias):
                    matches.append((len(alias), substance_id, substance))

        if not matches:
            return None

        _, substance_id, substance = max(matches, key=lambda item: item[0])
        return {"id": substance_id, **substance}

    def is_exempt(self, dataset: dict, exchange: dict, substance: dict) -> bool:
        """Return True if the exchange context matches protocol exemptions."""

        text = self.context_text(dataset, exchange)
        keywords = list(self.exemption_keywords.get("global", []))
        keywords.extend(self.exemption_keywords.get(substance["group"], []))
        keywords.extend(self.exemption_keywords.get(substance["id"], []))

        return any(_contains_keyword(text, keyword) for keyword in keywords)

    def infer_application(self, dataset: dict, exchange: dict, substance: dict) -> str:
        """Infer the ODS end-use application from dataset and exchange text."""

        text = self.context_text(dataset, exchange)

        for application, config in self.applications.items():
            if any(
                _contains_keyword(text, keyword)
                for keyword in config.get("keywords", [])
            ):
                return application

        return substance.get("default_application", "default")

    @staticmethod
    def context_text(dataset: dict, exchange: dict) -> str:
        """Collect user-facing text fields for classification."""

        fields = [
            dataset.get("name", ""),
            dataset.get("reference product", ""),
            dataset.get("location", ""),
            dataset.get("comment", ""),
            exchange.get("name", ""),
            exchange.get("product", ""),
            exchange.get("location", ""),
            exchange.get("comment", ""),
        ]

        return " ".join(str(field) for field in fields)

    def get_party_group(self, location: Optional[str]) -> str:
        """Return the Montreal Protocol party group for an ecoinvent location."""

        if not location:
            return self.mixed_location_fallback

        location = str(location)
        if location in self.mixed_locations:
            return self.mixed_location_fallback

        prefix = location.split("-")[0]

        if location in self.article5_locations or prefix in self.article5_locations:
            return "article5"

        if (
            location in self.non_article5_locations
            or prefix in self.non_article5_locations
        ):
            return "non_article5"

        if len(location) == 2:
            return "non_article5"

        return self.mixed_location_fallback

    def get_allowed_fraction(
        self, group: str, location: Optional[str], application: Optional[str] = None
    ) -> float:
        """Return remaining allowed ODS fraction for a group, location, and year."""

        party_group = self.get_party_group(location)
        group_schedule = self.schedules["groups"][group][party_group]
        allowed_fraction = 1.0

        for step in sorted(
            group_schedule.get("steps", []), key=lambda item: item["year"]
        ):
            if self.year >= int(step["year"]):
                allowed_fraction = float(step["allowed_fraction"])

        for allowance in group_schedule.get("servicing_allowances", []):
            applies_to_application = application in allowance.get("applications", [])
            if (
                applies_to_application
                and int(allowance["start_year"])
                <= self.year
                < int(allowance["end_year"])
            ):
                allowed_fraction = max(
                    allowed_fraction, float(allowance["allowed_fraction"])
                )

        return _clamp_allowed_fraction(allowed_fraction)

    def find_substitute_provider(
        self,
        application: str,
        substance: dict,
        dataset: dict,
        exchange: dict,
    ) -> tuple[Optional[dict], Optional[dict]]:
        """Return the first configured substitute candidate found in the database."""

        app_config = self.applications.get(application, {})
        substitutes = app_config.get("substitutes", {})
        candidates = (
            substitutes.get(substance["id"])
            or substitutes.get(substance["group"])
            or substitutes.get("default")
            or []
        )

        for candidate in candidates:
            provider = self.find_provider(candidate, dataset, exchange)
            if provider is not None:
                return candidate, provider

        return None, None

    def find_provider(
        self, candidate: dict, dataset: dict, exchange: dict
    ) -> Optional[dict]:
        """Find the best provider dataset for a substitute candidate."""

        reference_product = candidate.get("reference_product") or candidate.get(
            "reference product"
        )
        key = (candidate["name"], reference_product)
        providers = list(self.index.get(key, []))

        if not providers:
            providers = [
                {
                    "name": ds["name"],
                    "reference product": ds["reference product"],
                    "location": ds["location"],
                    "unit": ds["unit"],
                }
                for ds in self.database
                if ds.get("name") == candidate["name"]
                and ds.get("reference product") == reference_product
            ]

        if not providers:
            return None

        if candidate.get("location"):
            for provider in providers:
                if provider["location"] == candidate["location"]:
                    return provider

        preferred_locations = [
            dataset.get("location"),
            exchange.get("location"),
            "GLO",
            "RoW",
            "World",
        ]

        for location in preferred_locations:
            for provider in providers:
                if provider["location"] == location:
                    return provider

        return providers[0]

    def write_log(
        self,
        dataset: dict,
        exchange: dict,
        substance: dict,
        status: str,
        application: Optional[str] = None,
        allowed_fraction: Optional[float] = None,
        substitute: Optional[dict] = None,
    ) -> None:
        """Write one ozone transformation log line."""

        substitute_label = ""
        if substitute is not None:
            substitute_label = (
                f"{substitute.get('name', '')}|"
                f"{substitute.get('product', '')}|"
                f"{substitute.get('location', '')}"
            )

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset.get('name', '')}|{dataset.get('reference product', '')}|"
            f"{dataset.get('location', '')}|{exchange.get('name', '')}|"
            f"{exchange.get('product', '')}|{exchange.get('location', '')}|"
            f"{substance.get('id', '')}|{substance.get('group', '')}|"
            f"{application or ''}|"
            f"{'' if allowed_fraction is None else allowed_fraction}|"
            f"{substitute_label}"
        )
