"""Keep original-model provenance separate from final scenario specifications."""

import copy
import json
from functools import lru_cache

from .vehicle_energy import vehicle_energy_exchange

SPEC_KEY = "vehicle inventory provenance"
STATE_KEY = "premise vehicle provenance"


@lru_cache(maxsize=512)
def _decode(value):
    return json.loads(value)


def record_vehicle_provenance(transport):
    """Record actual final context without altering baseline metadata or exchanges."""
    for ds in transport.database:
        if SPEC_KEY not in ds:
            continue
        spec = ds[SPEC_KEY]
        spec = _decode(spec) if isinstance(spec, str) else spec
        if spec["family"] != transport.vehicle_type:
            continue
        log = ds.get("log parameters", {})
        battery = ds.get("premise vehicle battery sizing")
        fuels = [
            {
                k: e.get(k, e.get("reference product"))
                for k in ("name", "product", "location", "unit", "amount")
            }
            for e in ds["exchanges"]
            if vehicle_energy_exchange(e)
        ]
        output = sum(e["amount"] for e in ds["exchanges"] if e["type"] == "production")
        forwarder = ds.get("emptied", False) or (
            spec["role"] == "transport service"
            and not fuels
            and any(
                e["type"] == "technosphere"
                and e.get("product", e.get("reference product"))
                == ds["reference product"]
                and e["unit"] == ds["unit"]
                for e in ds["exchanges"]
            )
        )
        state = {
            "model": transport.model,
            "scenario": transport.scenario,
            "scenario year": transport.year,
            "location": ds["location"],
            "reference unit": ds["unit"],
            "reference production": output,
            "baseline model year": spec["baseline year"],
            "role": "forwarding service" if forwarder else spec["role"],
            "original model": spec["original model"],
            "operating energy inputs": (
                fuels if spec["role"] == "transport service" else []
            ),
        }
        if spec["role"] == "transport service" and not forwarder:
            state.update(
                {
                    "IAM energy multiplier": log.get("efficiency change", 1),
                    "car energy floor multiplier": log.get("car energy floor", {}).get(
                        "floor correction factor", 1
                    ),
                    "IAM efficiency source": copy.deepcopy(
                        log.get("IAM efficiency source", {"kind": "unavailable"})
                    ),
                }
            )
        if (
            spec["role"] in {"transport service", "vehicle production"}
            and not forwarder
        ):
            state["hardware basis"] = (
                "Scenario battery sizing; combustion/hydrogen tanks retain baseline design"
                if battery
                else "2020 baseline hardware; no scenario battery resizing recorded"
            )
            state["scenario battery specifications"] = copy.deepcopy(battery)
        ds[STATE_KEY] = state
