from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

# ----------------- Discovery configuration -----------------

@dataclass(frozen=True)
class DiscoveryConfig:
    """Global configuration for Home Assistant MQTT Discovery."""
    prefix: str

@dataclass(frozen=True)
class DeviceInfo:
    """Metadata describing a device in Home Assistant."""
    identifiers: list[str]
    name: str
    manufacturer: str
    model: str

# ----------------- Topic helpers -----------------

def _config_topic(prefix: str, component: str, object_id: str) -> str:
    """Return the Home Assistant MQTT Discovery config topic."""
    return f"{prefix}/{component}/{object_id}/config"

# ----------------- Entity publishers -----------------

def publish_sensor(
    pub,
    *,
    cfg: DiscoveryConfig,
    object_id: str,
    unique_id: str,
    name: str,
    state_topic: str,
    device: DeviceInfo,
    availability_topic: Optional[str] = None,
    device_class: Optional[str] = None,
    unit: Optional[str] = None,
    state_class: Optional[str] = "measurement",
) -> None:
    """
    Publish a Home Assistant MQTT Discovery configuration for a sensor entity.
    """
    payload: Dict[str, Any] = {
        "name": name,
        "unique_id": unique_id,
        "state_topic": state_topic,
        "device": {
            "identifiers": device.identifiers,
            "name": device.name,
            "manufacturer": device.manufacturer,
            "model": device.model,
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

    pub.publish_json(_config_topic(cfg.prefix, "sensor", object_id), payload, retain=True)