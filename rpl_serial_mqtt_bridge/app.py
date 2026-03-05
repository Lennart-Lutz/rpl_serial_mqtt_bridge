"""
RPL Serial -> MQTT Bridge

Reads newline-delimited JSON messages from a single RPL sink as serial device,
dispatches parsing by (type, version), and publishes to MQTT.

Topic schema:

Type 0xA3 (163) = Plant Hub v1:
- rpl/plant_hub/{ID}/port{1-12}      -> raw sensor value (int as string)
- rpl/plant_hub/{ID}/conmask         -> scon_bitmap
- rpl/plant_hub/{ID}/calmask         -> scal_bitmap

General:
- rpl/stats/{ID}/rank                -> RPL rank (diagnostics)

Extensibility:
- Add new message types by registering a new parser in PARSERS.
- Add new versions by adding another entry in the version map.
"""

from __future__ import annotations

import json
import time
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Protocol

import serial
from serial.tools import list_ports
import paho.mqtt.client as mqtt

from discovery import DiscoveryConfig, publish_sensor
from discovery_store import DiscoveryStore

DISCOVERY_SEEN_FILE = "/data/seen.json"
OPTIONS_FILE = "/data/options.json"

# Message types
TYPE_PLANT_HUB = 0xA3  # 163


# --------------------------
# Logging
# --------------------------

@dataclass(frozen=True)
class LogConfig:
    raw_lines: bool
    parsed_messages: bool
    unknown_types: bool
    mqtt_publishes: bool
    port_pick: bool

def log(msg: str) -> None:
    # Single place for printing (HA add-on log)
    print(msg, flush=True)


# --------------------------
# Options / configuration
# --------------------------

def load_options() -> Dict[str, Any]:
    with open(OPTIONS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_hex_auto(s: str) -> Optional[int]:
    """
    Converts VID/PID user strings to int.
    Accepts: "auto", "0x1234", "1234" (decimal), "ABCD" (hex).
    """
    s = (s or "").strip().lower()
    if not s or s == "auto":
        return None
    if s.startswith("0x"):
        return int(s, 16)
    if re.fullmatch(r"\d+", s):
        return int(s, 10)
    return int(s, 16)

def pick_serial_port(
    port_opt: str,
    vid_opt: Optional[int],
    pid_opt: Optional[int],
    *,
    log_cfg: LogConfig,
) -> Optional[str]:
    """
    Picks the most likely serial device.

    Strategy:
    - If user configured a fixed port (serial_port != "auto"), use it only if present.
    - Else enumerate ports, optionally filter by VID/PID, and pick best match using heuristics:
        * Prefer CDC ACM (ttyACM) devices
        * Prefer USB serial (ttyUSB)
        * Avoid Bluetooth
    """
    ports = list(list_ports.comports())
    if not ports:
        return None

    if log_cfg.port_pick:
        for p in ports:
            log(f"[DEBUG] Port candidate: {p.device} desc='{p.description}' vid={p.vid} pid={p.pid}")

    # If user explicitly configured a port, only accept if present.
    if port_opt and port_opt.lower() != "auto":
        for p in ports:
            if p.device == port_opt:
                if log_cfg.port_pick:
                    log(f"[DEBUG] Using explicitly configured serial_port='{port_opt}'")
                return p.device
        if log_cfg.port_pick:
            log(f"[WARN] Configured serial_port='{port_opt}' not found among available ports")
        return None

    # Filter by VID/PID if provided
    candidates = []
    for p in ports:
        if vid_opt is not None and (p.vid is None or int(p.vid) != int(vid_opt)):
            continue
        if pid_opt is not None and (p.pid is None or int(p.pid) != int(pid_opt)):
            continue
        candidates.append(p)

    if not candidates:
        candidates = ports

    def score(p) -> int:
        dev = (p.device or "").lower()
        desc = (p.description or "").lower()
        s = 0
        if "bluetooth" in desc:
            return -999

        if "cdc" in desc or "acm" in desc:
            s += 60
        if dev.startswith("/dev/ttyacm"):
            s += 50
        if dev.startswith("/dev/ttyusb"):
            s += 30
        if "usb" in desc:
            s += 10
        return s

    candidates.sort(key=score, reverse=True)

    chosen = candidates[0].device if candidates else None
    if log_cfg.port_pick and chosen:
        log(f"[DEBUG] Auto-picked serial port: {chosen}")
    return chosen

# --------------------------
# MQTT
# --------------------------

@dataclass(frozen=True)
class MqttConfig:
    host: str
    port: int
    username: str
    password: str
    topic_base: str  # e.g. "rpl"
    retain: bool

def mqtt_connect(cfg: MqttConfig) -> mqtt.Client:
    """
    Connects to MQTT and starts loop thread. Sets LWT for bridge status.
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

class Publisher:
    """
    Thin MQTT wrapper.
    Topic formatting is intentionally NOT done here (parser-owned).
    """

    def __init__(self, client: mqtt.Client, cfg: MqttConfig, log_cfg: LogConfig, discovery=None, seen=None):
        self._client = client
        self._cfg = cfg
        self._log_cfg = log_cfg
        self.discovery = discovery
        self.seen = seen

    @property
    def base(self) -> str:
        return self._cfg.topic_base

    @property
    def retain(self) -> bool:
        return self._cfg.retain

    def publish_str(self, topic: str, payload: str, retain: Optional[bool] = None) -> None:
        self._client.publish(
            topic,
            payload=payload,
            qos=0,
            retain=self._cfg.retain if retain is None else retain,
        )
        if self._log_cfg.mqtt_publishes:
            log(f"[DEBUG] MQTT pub topic='{topic}' payload='{payload}' retain={self._cfg.retain if retain is None else retain}")

    def publish_int(self, topic: str, payload: int, retain: Optional[bool] = None) -> None:
        self.publish_str(topic, str(int(payload)), retain=retain)

    def publish_json(self, topic: str, payload_obj: Dict[str, Any], retain: Optional[bool] = None) -> None:
        self.publish_str(topic, json.dumps(payload_obj, separators=(",", ":")), retain=retain)

# --------------------------
# Parser interface + implementations
# --------------------------

class MessageParser(Protocol):
    """
    Parser interface for a specific (type, version) tuple.
    Parsers own their MQTT topic schema.
    """

    def validate(self, msg: Dict[str, Any]) -> Tuple[bool, str]:
        ...

    def handle(self, msg: Dict[str, Any], pub: Publisher, log_cfg: LogConfig) -> None:
        ...

def ensure_planthub_v1_discovery(pub: Publisher, device_id: int) -> None:
    if not pub.discovery or not pub.seen:
        return

    key = f"planthub_v1:{device_id}"
    if pub.seen.has(key):
        return

    cfg: DiscoveryConfig = pub.discovery
    base = pub.base

    device_ident = f"plant_hub_{device_id}"
    device_name = f"Plant Hub {device_id}"

    # ein availability topic für ALLE Entities dieses Devices
    avail_topic = f"{base}/plant_hub/{device_id}/availability"

    # 12 ports
    for i in range(1, 13):
        publish_sensor(
            pub,
            cfg=cfg,
            object_id=f"{device_ident}_port{i}",
            unique_id=f"{device_ident}_port{i}",
            name=f"{device_name} Port {i}",
            state_topic=f"{base}/plant_hub/{device_id}/port{i}",
            device_ident=device_ident,
            device_name=device_name,
            availability_topic=avail_topic,   # <---
            unit=None,
            device_class=None,
            state_class="measurement",
        )

    # masks + rank
    publish_sensor(
        pub,
        cfg=cfg,
        object_id=f"{device_ident}_conmask",
        unique_id=f"{device_ident}_conmask",
        name=f"{device_name} ConMask",
        state_topic=f"{base}/plant_hub/{device_id}/conmask",
        device_ident=device_ident,
        device_name=device_name,
        availability_topic=avail_topic,      # <---
        state_class=None,
    )
    publish_sensor(
        pub,
        cfg=cfg,
        object_id=f"{device_ident}_calmask",
        unique_id=f"{device_ident}_calmask",
        name=f"{device_name} CalMask",
        state_topic=f"{base}/plant_hub/{device_id}/calmask",
        device_ident=device_ident,
        device_name=device_name,
        availability_topic=avail_topic,      # <---
        state_class=None,
    )
    publish_sensor(
        pub,
        cfg=cfg,
        object_id=f"{device_ident}_rank",
        unique_id=f"{device_ident}_rank",
        name=f"{device_name} RPL Rank",
        state_topic=f"{base}/stats/{device_id}/rank",
        device_ident=device_ident,
        device_name=device_name,
        availability_topic=avail_topic,      # <---
        state_class="measurement",
    )

    # Device ist "da": availability online (retain!)
    pub.publish_str(avail_topic, "online", retain=True)

    pub.seen.add(key)

class PlantHubV1Parser:
    """
    Plant Hub message v1.

    Example:
      {
        "type": 163,
        "version": 1,
        "id": 17580,
        "rank": 512,
        "scon_bitmap": 0,
        "scal_bitmap": 1,
        "sensor_values": [ ... 12 ints ... ]
      }
    """

    REQUIRED_KEYS = (
        "type", "version", "id", "rank",
        "scon_bitmap", "scal_bitmap", "sensor_values"
    )

    def validate(self, msg: Dict[str, Any]) -> Tuple[bool, str]:
        for k in self.REQUIRED_KEYS:
            if k not in msg:
                return False, f"missing key '{k}'"

        if int(msg["type"]) != TYPE_PLANT_HUB:
            return False, f"unexpected type {msg['type']}"

        if int(msg["version"]) != 1:
            return False, f"unsupported version {msg['version']}"

        vals = msg["sensor_values"]
        if not isinstance(vals, list) or len(vals) != 12:
            return False, "sensor_values must be a list of length 12"

        try:
            _ = [int(v) for v in vals]
        except Exception:
            return False, "sensor_values contains non-integers"

        return True, "ok"

    def handle(self, msg: Dict[str, Any], pub: Publisher, log_cfg: LogConfig) -> None:
        device_id = int(msg["id"])
        rank = int(msg["rank"])
        conmask = int(msg["scon_bitmap"])
        calmask = int(msg["scal_bitmap"])
        values = [int(v) for v in msg["sensor_values"]]

        ensure_planthub_v1_discovery(pub, device_id)

        # ---- Topic building (parser-owned) ----
        base = pub.base

        # Diagnostics
        pub.publish_int(f"{base}/stats/{device_id}/rank", rank)

        # Masks
        pub.publish_int(f"{base}/plant_hub/{device_id}/conmask", conmask)
        pub.publish_int(f"{base}/plant_hub/{device_id}/calmask", calmask)

        # Ports (1..12)
        for i, v in enumerate(values, start=1):
            pub.publish_int(f"{base}/plant_hub/{device_id}/port{i}", v)

        if log_cfg.parsed_messages:
            log(f"[INFO] PlantHubV1 parsed: id={device_id} rank={rank} conmask=0x{conmask:04X} calmask=0x{calmask:04X}")

# Registry: TYPE -> VERSION -> parser instance
PARSERS: Dict[int, Dict[int, MessageParser]] = {
    TYPE_PLANT_HUB: {1: PlantHubV1Parser()},
}

def dispatch_message(msg: Dict[str, Any], pub: Publisher, log_cfg: LogConfig) -> None:
    """
    Finds parser by msg['type'] and msg['version'] and publishes via that parser.
    """
    try:
        msg_type = int(msg.get("type", -1))
        msg_version = int(msg.get("version", -1))
    except Exception:
        return

    type_map = PARSERS.get(msg_type)
    if not type_map:
        if log_cfg.unknown_types:
            log(f"[WARN] Unknown message type={msg_type}. Keys={list(msg.keys())}")
        return

    parser = type_map.get(msg_version)
    if not parser:
        log(f"[WARN] No parser for type={msg_type} version={msg_version}")
        return

    ok, reason = parser.validate(msg)
    if not ok:
        log(f"[WARN] Message rejected (type={msg_type} version={msg_version}): {reason}")
        return

    parser.handle(msg, pub, log_cfg)

# --------------------------
# Serial reader loop
# --------------------------

def main() -> None:
    opts = load_options()

    # Logging flags
    log_cfg = LogConfig(
        raw_lines=bool(opts.get("log_raw_lines", False)),
        parsed_messages=bool(opts.get("log_parsed_messages", True)),
        unknown_types=bool(opts.get("log_unknown_types", True)),
        mqtt_publishes=bool(opts.get("log_mqtt_publishes", False)),
        port_pick=bool(opts.get("log_port_pick", False)),
    )

    # Serial
    baudrate = int(opts.get("baudrate", 115200))
    port_opt = str(opts.get("serial_port", "auto"))
    vid_opt = normalize_hex_auto(str(opts.get("serial_vid", "auto")))
    pid_opt = normalize_hex_auto(str(opts.get("serial_pid", "auto")))
    timeout_s = float(opts.get("serial_timeout_s", 2.0))

    # MQTT
    mqtt_cfg = MqttConfig(
        host=str(opts.get("mqtt_host", "core-mosquitto")),
        port=int(opts.get("mqtt_port", 1883)),
        username=str(opts.get("mqtt_username", "")),
        password=str(opts.get("mqtt_password", "")),
        topic_base=str(opts.get("mqtt_topic_base", "rpl")).rstrip("/"),
        retain=bool(opts.get("mqtt_retain", True)),
    )

    discovery_enable = bool(opts.get("mqtt_discovery_enable", True))
    discovery_prefix = str(opts.get("mqtt_discovery_prefix", "homeassistant")).rstrip("/")

    seen = DiscoveryStore.load(DISCOVERY_SEEN_FILE) if discovery_enable else None
    discovery_cfg = DiscoveryConfig(prefix=discovery_prefix, base_topic=mqtt_cfg.topic_base) if discovery_enable else None

    log(f"[INFO] Starting bridge (baud={baudrate}, port={port_opt}, timeout={timeout_s}s)")
    if vid_opt is not None or pid_opt is not None:
        log(f"[INFO] Serial filter enabled: VID={vid_opt} PID={pid_opt}")
    log(f"[INFO] MQTT: {mqtt_cfg.host}:{mqtt_cfg.port} base='{mqtt_cfg.topic_base}' retain={mqtt_cfg.retain}")

    mqtt_client = mqtt_connect(mqtt_cfg)
    pub = Publisher(mqtt_client, mqtt_cfg, log_cfg, discovery=discovery_cfg, seen=seen)

    while True:
        dev = pick_serial_port(port_opt, vid_opt, pid_opt, log_cfg=log_cfg)
        if not dev:
            log("[WARN] No serial devices found. Retrying in 2s...")
            time.sleep(2)
            continue

        log(f"[INFO] Using serial device: {dev}")

        try:
            with serial.Serial(dev, baudrate=baudrate, timeout=timeout_s) as ser:
                while True:
                    line = ser.readline()
                    if not line:
                        continue

                    s = line.decode("utf-8", errors="replace").strip()
                    if not s:
                        continue

                    if log_cfg.raw_lines:
                        log(f"[RAW] {s}")

                    try:
                        msg = json.loads(s)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(msg, dict):
                        dispatch_message(msg, pub, log_cfg)

        except serial.SerialException as e:
            log(f"[WARN] Serial error on {dev}: {e}. Reconnecting in 2s...")
            time.sleep(2)
        except Exception as e:
            log(f"[ERROR] Unexpected error: {e}. Reconnecting in 2s...")
            time.sleep(2)

if __name__ == "__main__":
    main()