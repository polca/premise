"""Read annual service shares for construction timing in TRAILS."""

import math
from fnmatch import fnmatchcase
from functools import cached_property
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .geomap import Geomap


class VintageFleets:
    """Build-year profiles indexed by asset, IAM region, and year of use."""

    def __init__(self, package, links_path, model, pathway, years):
        self.model = model.lower()
        self._locations = {}
        with links_path.open(encoding="utf-8") as stream:
            mapping = yaml.safe_load(stream)
        self.links = mapping["construction_profiles"]
        self.exchanges = mapping.get("construction_exchanges", [])
        assets = {choice["asset"] for choice in self.exchanges}
        assets.update(
            choice["fallback"] for choice in self.exchanges if "fallback" in choice
        )
        for link in self.links.values():
            if isinstance(link, str):
                assets.add(link)
            else:
                assets.update(choice["asset"] for choice in link)
        self.shares, self.zero_stock = self._read_shares(
            package, self.model, pathway, years, assets
        )
        self.regions = {region for _, region, _ in self.shares}
        self.regions.update(region for _, region, _ in self.zero_stock)

    @staticmethod
    def _read_shares(package, model, pathway, years, assets):
        """Read service shares and explicit zero stock for the exported years."""
        if not assets or not years:
            return {}, set()
        # Read the member once because repeated seeks in a ZIP member can reread it.
        with ZipFile(package) as archive:
            parquet = pa.BufferReader(archive.read("vintages.parquet"))
        selection = [("year", "in", list(years)), ("asset", "in", sorted(assets))]
        table = pq.read_table(
            parquet,
            columns=[
                "model",
                "scenario",
                "region",
                "asset",
                "year",
                "cohort_year",
                "metric",
                "value",
                "unit",
            ],
            filters=[
                [*selection, ("metric", "=", "service_share")],
                [*selection, ("metric", "=", "stock"), ("value", "=", 0)],
            ],
        ).to_pandas()
        if not (
            table["model"].str.lower().eq(model) & table["scenario"].eq(pathway)
        ).all():
            raise ValueError(
                "Vintage service shares have a different model or scenario."
            )

        keys = ["asset", "region", "year"]
        zeros = table.loc[
            table["metric"].eq("stock") & table["cohort_year"].eq(table["year"])
        ]
        zero_stock = set(zeros[keys].itertuples(index=False, name=None))
        profiles = {}
        shares = table.loc[table["metric"].eq("service_share")]
        for key, values in shares.groupby(keys, sort=False):
            asset, region, year = key
            by_cohort = values.sort_values("cohort_year")
            cohorts = by_cohort["cohort_year"].tolist()
            weights = by_cohort["value"].tolist()
            if (
                not by_cohort["unit"].eq("1").all()
                or len(set(cohorts)) != len(cohorts)
                or any(
                    not isinstance(cohort, int) or cohort > year for cohort in cohorts
                )
                or any(not math.isfinite(weight) or weight < 0 for weight in weights)
                or not math.isclose(math.fsum(weights), 1, rel_tol=0, abs_tol=1e-9)
            ):
                raise ValueError(
                    f"Invalid vintage service shares for {asset}/{region}/{year}."
                )
            offsets = [float(cohort - year) for cohort in cohorts]
            profiles[key] = (offsets, weights)
        if zero_stock.intersection(profiles):
            raise ValueError("A fleet with zero stock cannot have service shares.")
        return profiles, zero_stock

    @cached_property
    def geography(self):
        """Load the standard IAM geography map only when a location needs it."""
        return Geomap(self.model)

    def _region(self, location):
        """Use an explicit IAM region or an unambiguous premise geographic match."""
        if location == "GLO":
            return "World"
        if not location or location in self.regions:
            return location
        if location not in self._locations:
            if location in self.geography.iam_regions:
                self._locations[location] = location
            else:
                try:
                    regions = set(self.geography.map_ecoinvent_to_iam(location))
                except (KeyError, ValueError):
                    regions = set()
                regions.discard("World")
                self._locations[location] = (
                    next(iter(regions)) if len(regions) == 1 else None
                )
        return self._locations[location]

    def _asset(self, profile_id, supplier_name, caller_name):
        """Match the existing construction profile, with extra checks for mixed fleets."""
        link = self.links.get(profile_id)
        if link is None or isinstance(link, str):
            return link
        for choice in link:
            if all(
                not choice.get(field)
                or any(fnmatchcase(name, pattern) for pattern in choice[field])
                for field, name in (
                    ("suppliers", supplier_name.lower()),
                    ("callers", caller_name.lower()),
                )
            ):
                return choice["asset"]
        return None

    def _exchange_assets(self, supplier, caller):
        """List the fleet and any fallback for an operating activity's construction input."""
        name = (supplier.get("name") or "").strip().lower()
        product = (
            (supplier.get("reference product") or supplier.get("product") or "")
            .strip()
            .lower()
        )
        unit = (supplier.get("unit") or "").strip().lower()
        caller_name = (caller.get("name") or "").strip().lower()
        for choice in self.exchanges:
            if (
                unit == choice["unit"]
                and fnmatchcase(product, choice["product"])
                and any(fnmatchcase(name, pattern) for pattern in choice["suppliers"])
                and any(
                    fnmatchcase(caller_name, pattern) for pattern in choice["callers"]
                )
            ):
                assets = (choice["asset"],)
                if "fallback" in choice:
                    assets += (choice["fallback"],)
                return assets
        return ()

    def construction(self, profile_id, supplier, caller, year, defaults):
        """Use the fleet's service weights for a matching construction exchange."""
        assets = self._exchange_assets(supplier, caller)
        profile_asset = self._asset(
            profile_id, supplier.get("name") or "", caller.get("name") or ""
        )
        if profile_asset is not None:
            assets += (profile_asset,)
        if not assets:
            return defaults
        region = self._region(caller.get("location"))
        for asset in assets:
            key = (asset, region, year)
            if key in self.zero_stock:
                return defaults
            matching = self.shares.get(key)
            if matching is not None:
                break
        else:
            return defaults
        offsets, weights = matching
        return {
            **(defaults or {}),
            "temporal_distribution": 6,
            "temporal_loc": None,
            "temporal_scale": None,
            "temporal_min": None,
            "temporal_max": None,
            "temporal_offsets": offsets.copy(),
            "temporal_weights": weights.copy(),
            "iam_vintage_asset": asset,
            "iam_vintage_region": region,
        }
