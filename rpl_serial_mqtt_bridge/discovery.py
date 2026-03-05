from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass(frozen=True)
class DiscoveryConfig:
    prefix: str  # "homeassistant"
    base_topic: str  # "rpl"
    manufacturer: str = "PlantHub"
    model: str = "Plant Hub"


def _cfg_topic(prefix: str, component: str, object_id: str) -> str:
    return f"{prefix}/{component}/{object_id}/config"


def publish_sensor(
    pub,
    *,
    cfg: DiscoveryConfig,
    object_id: str,
    unique_id: str,
    name: str,
    state_topic: str,
    availability_topic: Optional[str] = None,
    device_ident: str,
    device_name: str,
    device_class: Optional[str] = None,
    unit: Optional[str] = None,
    state_class: Optional[str] = "measurement",
) -> None:
    payload: Dict[str, Any] = {
        "name": name,
        "unique_id": unique_id,
        "state_topic": state_topic,
        "device": {
            "identifiers": [device_ident],
            "name": device_name,
            "manufacturer": cfg.manufacturer,
            "model": cfg.model,
        },
    }
    if availability_topic:
        payload["availability_topic"] = availability_topic
        payload["payload_available"] = "online"
        payload["payload_not_available"] = "offline"
    if device_class:
        payload["device_class"] = device_class
    if unit:
        payload["unit_of_measurement"] = unit
    if state_class:
        payload["state_class"] = state_class

    pub.publish_json(_cfg_topic(cfg.prefix, "sensor", object_id), payload, retain=True)