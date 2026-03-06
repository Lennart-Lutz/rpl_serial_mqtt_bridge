from __future__ import annotations

from typing import Dict, Any, Tuple, Protocol

from discovery import DiscoveryConfig, DeviceInfo, publish_sensor
from mqtt_client import Publisher

# ---------------- Message type constants ----------------

TYPE_PLANT_HUB = 0xA3

# ---------------- Parser interface ----------------

class MessageParser(Protocol):
    """Interface for a parser bound to a specific (type, version) pair."""

    def validate(self, msg: Dict[str, Any]) -> Tuple[bool, str]:
        ...

    def handle(self, msg: Dict[str, Any], pub: Publisher, log_cfg, logger) -> None:
        ...

# ---------------- Parser implementations ----------------

def ensure_planthub_v1_discovery(pub: Publisher, node_id: int) -> None:
    """
    Publish Home Assistant MQTT Discovery entities for a Plant Hub v1 device once.

    Discovery is keyed by message family and node ID and persisted through
    the discovery store.
    """
    if not pub.discovery or not pub.seen:
        return

    discovery_key = f"planthub_v1:{node_id}"
    if pub.seen.has(discovery_key):
        return

    cfg: DiscoveryConfig = pub.discovery
    base = pub.base

    # Discovery information
    device_ident = f"plant_hub_{node_id}"
    device_name = f"Plant Hub {node_id}"
    device_availability_topic = f"{base}/plant_hub/{node_id}/availability" # Global device availability

    device = DeviceInfo(
        identifiers=[device_ident],
        name=device_name,
        manufacturer="Lennart Lutz",
        model="Plant Hub",
    )

    # ---------------- Port sensors ----------------

    for port_index in range(1, 13):
        port_availability_topic = f"{base}/plant_hub/{node_id}/port{port_index}/availability" # Per-port availability

        publish_sensor(
            pub,
            cfg=cfg,
            object_id=f"{device_ident}_port{port_index}",
            unique_id=f"{device_ident}_port{port_index}",
            name=f"Port {port_index}",
            state_topic=f"{base}/plant_hub/{node_id}/port{port_index}",
            availability_topic=port_availability_topic,
            device=device,
            unit=None,
            device_class=None,
            state_class="measurement",
        )

    # ---------------- Device-level sensors ----------------

    publish_sensor(
        pub,
        cfg=cfg,
        object_id=f"{device_ident}_conmask",
        unique_id=f"{device_ident}_conmask",
        name=f"ConMask",
        state_topic=f"{base}/plant_hub/{node_id}/conmask",
        availability_topic=device_availability_topic,
        device=device,
        state_class=None,
    )

    publish_sensor(
        pub,
        cfg=cfg,
        object_id=f"{device_ident}_calmask",
        unique_id=f"{device_ident}_calmask",
        name=f"CalMask",
        state_topic=f"{base}/plant_hub/{node_id}/calmask",
        availability_topic=device_availability_topic,
        device=device,
        state_class=None,
    )

    publish_sensor(
        pub,
        cfg=cfg,
        object_id=f"{device_ident}_rank",
        unique_id=f"{device_ident}_rank",
        name=f"RPL Rank",
        state_topic=f"{base}/stats/{node_id}/rank",
        availability_topic=device_availability_topic,
        device=device,
        state_class="measurement",
    )

    # Mark the overall device as online once discovery is published.
    pub.publish_str(device_availability_topic, "online", retain=True)
    pub.seen.add(discovery_key)

class PlantHubV1Parser:
    """
    Parser for Plant Hub message type 0xA3, version 1.

    Expected payload example:
    {
      "type": 163,
      "version": 1,
      "id": 17580,
      "rank": 512,
      "scon_bitmap": 0,
      "scal_bitmap": 1,
      "sensor_values": [12 integer values]
    }

    MQTT topic schema for Plant Hub v1:
    - rpl/plant_hub/{node_id}/port{1..12}   -> raw sensor value
    - rpl/plant_hub/{node_id}/conmask       -> connection bitmap
    - rpl/plant_hub/{node_id}/calmask       -> calibration bitmap
    - rpl/stats/{node_id}/rank              -> RPL rank
    """

    REQUIRED_KEYS = (
        "type",
        "version",
        "id",
        "rank",
        "scon_bitmap",
        "scal_bitmap",
        "sensor_values",
    )

    def validate(self, msg: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate message shape and basic field semantics."""
        for key in self.REQUIRED_KEYS:
            if key not in msg:
                return False, f"missing key '{key}'"

        if int(msg["type"]) != TYPE_PLANT_HUB:
            return False, f"unexpected type {msg['type']}"

        if int(msg["version"]) != 1:
            return False, f"unsupported version {msg['version']}"

        values = msg["sensor_values"]
        if not isinstance(values, list) or len(values) != 12:
            return False, "sensor_values must be a list of length 12"

        try:
            _ = [int(v) for v in values]
        except Exception:
            return False, "sensor_values contains non-integer values"

        return True, "ok"

    def handle(self, msg: Dict[str, Any], pub: Publisher, log_cfg, logger) -> None:
        """Publish a valid Plant Hub v1 message to MQTT."""
        node_id = int(msg["id"])
        rank = int(msg["rank"])
        conmask = int(msg["scon_bitmap"])
        calmask = int(msg["scal_bitmap"])
        values = [int(v) for v in msg["sensor_values"]]

        ensure_planthub_v1_discovery(pub, node_id)

        base = pub.base

        # ---------------- Device-level state ----------------

        pub.publish_str(f"{base}/plant_hub/{node_id}/availability", "online", retain=True)

        pub.publish_int(f"{base}/stats/{node_id}/rank", rank)
        pub.publish_int(f"{base}/plant_hub/{node_id}/conmask", conmask)
        pub.publish_int(f"{base}/plant_hub/{node_id}/calmask", calmask)

        # ---------------- Port states + availability ----------------

        for index, value in enumerate(values, start=1):
            is_connected = bool(conmask & (1 << (index - 1)))
            port_availability_topic = f"{base}/plant_hub/{node_id}/port{index}/availability"
            port_state_topic = f"{base}/plant_hub/{node_id}/port{index}"

            if is_connected:
                pub.publish_str(port_availability_topic, "online", retain=True)
                pub.publish_int(port_state_topic, value)
            else:
                pub.publish_str(port_availability_topic, "offline", retain=True)

        if log_cfg.parsed_messages:
            logger(
                f"[INFO] PlantHubV1 parsed: id={node_id} rank={rank} "
                f"conmask=0x{conmask:04X} calmask=0x{calmask:04X}"
            )

# ---------------- Parser registry ----------------

PARSERS: Dict[int, Dict[int, MessageParser]] = {
    TYPE_PLANT_HUB: {
        1: PlantHubV1Parser(),
    },
}

# ---------------- Dispatch ----------------

def dispatch_message(msg: Dict[str, Any], pub: Publisher, log_cfg, logger) -> None:
    """
    Resolve the parser by message type and version, validate the message,
    and publish its contents to MQTT.
    """
    try:
        msg_type = int(msg.get("type", -1))
        msg_version = int(msg.get("version", -1))
    except Exception:
        return

    version_map = PARSERS.get(msg_type)
    if not version_map:
        if log_cfg.unknown_types:
            logger(f"[WARN] Unknown message type={msg_type}. Keys={list(msg.keys())}")
        return

    parser = version_map.get(msg_version)
    if not parser:
        logger(f"[WARN] No parser for type={msg_type} version={msg_version}")
        return

    ok, reason = parser.validate(msg)
    if not ok:
        logger(f"[WARN] Message rejected (type={msg_type} version={msg_version}): {reason}")
        return

    parser.handle(msg, pub, log_cfg, logger)