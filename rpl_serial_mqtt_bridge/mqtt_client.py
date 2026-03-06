from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

# ---------------- MQTT configuration ----------------

@dataclass(frozen=True)
class MqttConfig:
    """Runtime configuration for the MQTT connection."""
    host: str
    port: int
    username: str
    password: str
    topic_base: str
    retain: bool

# ---------------- MQTT connection ----------------

def mqtt_connect(cfg: MqttConfig) -> mqtt.Client:
    """
    Connect to MQTT, start the background loop, and publish bridge status.

    A Last Will and Testament (LWT) is configured so consumers can detect
    an unexpected bridge disconnect.
    """
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    if cfg.username:
        client.username_pw_set(cfg.username, cfg.password)

    status_topic = f"{cfg.topic_base}/bridge/status"
    client.will_set(status_topic, payload="offline", qos=0, retain=True)

    client.connect(cfg.host, cfg.port, keepalive=60)
    client.loop_start()

    client.publish(status_topic, payload="online", qos=0, retain=True)
    client.publish(
        f"{cfg.topic_base}/bridge/last_start",
        payload=str(int(time.time())),
        qos=0,
        retain=True,
    )

    return client

# ---------------- MQTT publisher wrapper ----------------

class Publisher:
    """
    Thin wrapper around the MQTT client.

    Topic naming is intentionally owned by the parser layer.
    """

    def __init__(self, client: mqtt.Client, cfg: MqttConfig, log_cfg, discovery=None, seen=None, logger=None) -> None:

        self._client = client
        self._cfg = cfg
        self._log_cfg = log_cfg
        self.discovery = discovery
        self.seen = seen
        self._logger = logger or (lambda msg: None)

    @property
    def base(self) -> str:
        """Return the configured MQTT topic base."""
        return self._cfg.topic_base

    @property
    def retain(self) -> bool:
        """Return the default retain flag for state publishes."""
        return self._cfg.retain

    def publish_str(self, topic: str, payload: str, retain: Optional[bool] = None) -> None:
        """Publish a string payload."""
        effective_retain = self._cfg.retain if retain is None else retain
        self._client.publish(topic, payload=payload, qos=0, retain=effective_retain)

        if self._log_cfg.mqtt_publishes:
            self._logger(
                f"[DEBUG] MQTT pub topic='{topic}' payload='{payload}' retain={effective_retain}"
            )

    def publish_int(self, topic: str, payload: int, retain: Optional[bool] = None) -> None:
        """Publish an integer payload as string."""
        self.publish_str(topic, str(int(payload)), retain=retain)

    def publish_json(self, topic: str, payload_obj: Dict[str, Any], retain: Optional[bool] = None) -> None:
        """Publish a compact JSON payload."""

        self.publish_str(topic, json.dumps(payload_obj, separators=(",", ":")), retain=retain)