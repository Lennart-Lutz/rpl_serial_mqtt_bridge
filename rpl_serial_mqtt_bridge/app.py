"""
RPL Serial -> MQTT Bridge

Reads newline-delimited JSON messages from a single RPL sink as serial device,
dispatches parsing by (type, version), and publishes to MQTT using a stable topic
schema.

Topic schema:

Type 0xA3 (163) = Plant Hub v1:
- rpl/plant_hub/{ID}/port{1-12}      -> raw sensor value as string/int
- rpl/plant_hub/{ID}/conmask         -> scon_bitmap
- rpl/plant_hub/{ID}/calmask         -> scal_bitmap

General:
- rpl/stats/{ID}/rank                -> RPL rank (for diagnostics)

Extensibility:
- Add new message types by registering a new parser class in PARSERS.
- Add new versions by adding another entry in the version map.
"""

from __future__ import annotations

import json
import time
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List, Protocol

import serial
from serial.tools import list_ports
import paho.mqtt.client as mqtt

OPTIONS_FILE = "/data/options.json"

# Message types
TYPE_PLANT_HUB = 0xA3  # 163

# --------------------------
# Utility / configuration
# --------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

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

def pick_serial_port(port_opt: str, vid_opt: Optional[int], pid_opt: Optional[int]) -> Optional[str]:
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

    # If user explicitly configured a port, only accept if present.
    if port_opt and port_opt.lower() != "auto":
        for p in ports:
            if p.device == port_opt:
                return p.device
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

        # Best signals first
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
    return candidates[0].device if candidates else None

# --------------------------
# MQTT publishing
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
    A small wrapper to keep all topic formatting in one place.
    """

    def __init__(self, client: mqtt.Client, cfg: MqttConfig):
        self._client = client
        self._cfg = cfg

    def publish_str(self, topic: str, payload: str, retain: Optional[bool] = None) -> None:
        self._client.publish(
            topic,
            payload=payload,
            qos=0,
            retain=self._cfg.retain if retain is None else retain,
        )

    def publish_int(self, topic: str, payload: int, retain: Optional[bool] = None) -> None:
        self.publish_str(topic, str(int(payload)), retain=retain)

    # ---- Topic schema ----

    def plant_hub_port(self, device_id: int, port_1based: int) -> str:
        return f"{self._cfg.topic_base}/plant_hub/{device_id}/port{port_1based}"

    def plant_hub_conmask(self, device_id: int) -> str:
        return f"{self._cfg.topic_base}/plant_hub/{device_id}/conmask"

    def plant_hub_calmask(self, device_id: int) -> str:
        return f"{self._cfg.topic_base}/plant_hub/{device_id}/calmask"

    def stats_rank(self, device_id: int) -> str:
        return f"{self._cfg.topic_base}/stats/{device_id}/rank"

# --------------------------
# Parser interface + implementations
# --------------------------

class ParseResult(Protocol):
    """
    Marker protocol; parser implementations can return any structured result they want.
    For now we publish directly in the parser (simple + explicit topics).
    """
    pass

class MessageParser(Protocol):
    """
    Parser interface for a specific (type, version) tuple.
    """

    def validate(self, msg: Dict[str, Any]) -> Tuple[bool, str]:
        ...

    def handle(self, msg: Dict[str, Any], pub: Publisher) -> None:
        ...


class PlantHubV1Parser:
    """
    Plant Hub message v1:

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

    def handle(self, msg: Dict[str, Any], pub: Publisher) -> None:
        device_id = int(msg["id"])
        rank = int(msg["rank"])
        conmask = int(msg["scon_bitmap"])
        calmask = int(msg["scal_bitmap"])
        values = [int(v) for v in msg["sensor_values"]]

        # Stats
        pub.publish_int(pub.stats_rank(device_id), rank)

        # Masks
        pub.publish_int(pub.plant_hub_conmask(device_id), conmask)
        pub.publish_int(pub.plant_hub_calmask(device_id), calmask)

        # Ports (1..12)
        for i, v in enumerate(values, start=1):
            pub.publish_int(pub.plant_hub_port(device_id, i), v)

# Registry: TYPE -> VERSION -> parser instance
PARSERS: Dict[int, Dict[int, MessageParser]] = {
    TYPE_PLANT_HUB: {
        1: PlantHubV1Parser(),
    }
}

def dispatch_message(msg: Dict[str, Any], pub: Publisher) -> None:
    """
    Finds parser by msg['type'] and msg['version'] and publishes via that parser.
    Unknown types/versions are ignored (but you could log or publish raw if desired).
    """
    try:
        msg_type = int(msg.get("type", -1))
        msg_version = int(msg.get("version", -1))
    except Exception:
        return

    type_map = PARSERS.get(msg_type)
    if not type_map:
        # Unknown type: ignore for now (easy to change later)
        return

    parser = type_map.get(msg_version)
    if not parser:
        log(f"[WARN] No parser for type={msg_type} version={msg_version}")
        return

    ok, reason = parser.validate(msg)
    if not ok:
        log(f"[WARN] Message rejected (type={msg_type} version={msg_version}): {reason}")
        return

    parser.handle(msg, pub)

# --------------------------
# Serial reader loop
# --------------------------

def main() -> None:
    opts = load_options()

    # Serial
    baudrate = int(opts.get("baudrate", 115200))
    port_opt = str(opts.get("serial_port", "auto"))
    vid_opt = normalize_hex_auto(str(opts.get("serial_vid", "auto")))
    pid_opt = normalize_hex_auto(str(opts.get("serial_pid", "auto")))
    timeout_s = float(opts.get("json_timeout_s", 2.0))
    log_raw = bool(opts.get("log_raw_lines", False))

    # MQTT
    mqtt_cfg = MqttConfig(
        host=str(opts.get("mqtt_host", "core-mosquitto")),
        port=int(opts.get("mqtt_port", 1883)),
        username=str(opts.get("mqtt_username", "")),
        password=str(opts.get("mqtt_password", "")),
        topic_base=str(opts.get("mqtt_topic_base", "rpl")).rstrip("/"),
        retain=bool(opts.get("mqtt_retain", True)),
    )

    log(f"[INFO] Starting bridge (baud={baudrate}, port={port_opt}, timeout={timeout_s}s)")
    if vid_opt is not None or pid_opt is not None:
        log(f"[INFO] Serial filter enabled: VID={vid_opt} PID={pid_opt}")
    log(f"[INFO] MQTT: {mqtt_cfg.host}:{mqtt_cfg.port} base='{mqtt_cfg.topic_base}' retain={mqtt_cfg.retain}")

    mqtt_client = mqtt_connect(mqtt_cfg)
    pub = Publisher(mqtt_client, mqtt_cfg)

    while True:
        dev = pick_serial_port(port_opt, vid_opt, pid_opt)
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

                    if log_raw:
                        log(f"[RAW] {s}")

                    # Your sink prints JSON. We treat each line as a JSON object.
                    try:
                        msg = json.loads(s)
                    except json.JSONDecodeError:
                        # Ignore non-JSON noise / partial lines
                        continue

                    if isinstance(msg, dict):
                        dispatch_message(msg, pub)

        except serial.SerialException as e:
            log(f"[WARN] Serial error on {dev}: {e}. Reconnecting in 2s...")
            time.sleep(2)
        except Exception as e:
            log(f"[ERROR] Unexpected error: {e}. Reconnecting in 2s...")
            time.sleep(2)

if __name__ == "__main__":
    main()