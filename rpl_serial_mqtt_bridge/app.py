"""
RPL Serial -> MQTT Bridge

This add-on reads newline-delimited JSON messages from a serial-connected RPL sink,
dispatches them by (type, version), and publishes parsed values to MQTT.

Design goals:
- MQTT Discovery is created automatically per device type and node ID
- Discovery information is persisted so it is only published once per device
- Each message type/version is handled by its own parser
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import serial
from serial.tools import list_ports

from discovery import DiscoveryConfig
from discovery_store import DiscoveryStore
from mqtt_client import MqttConfig, Publisher, mqtt_connect
from parsers import dispatch_message

# ---------------- Constants -----------------

DISCOVERY_SEEN_FILE = "/data/seen.json"
OPTIONS_FILE = "/data/options.json"

# ---------------- Logging ----------------

@dataclass(frozen=True)
class LogConfig:
    raw_lines: bool
    parsed_messages: bool
    unknown_types: bool
    mqtt_publishes: bool
    port_pick: bool

def log(msg: str) -> None:
    """Write a log line to the Home Assistant add-on log output."""
    print(msg, flush=True)

# ---------------- Add-on options ----------------

def load_options() -> Dict[str, Any]:
    """Load add-on options from the Home Assistant options file."""
    with open(OPTIONS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_hex_auto(value: str) -> Optional[int]:
    """
    Convert a user-supplied VID/PID string to an integer.

    Accepted input:
    - "auto"    -> None
    - "0x1234"  -> hex
    - "1234"    -> decimal
    - "ABCD"    -> hex
    """
    s = (value or "").strip().lower()
    if not s or s == "auto":
        return None
    if s.startswith("0x"):
        return int(s, 16)
    if re.fullmatch(r"\d+", s):
        return int(s, 10)
    return int(s, 16)

# ---------------- Serial port selection ----------------

def pick_serial_port(configured_port: str, configured_vid: Optional[int], configured_pid: Optional[int], *, log_cfg: LogConfig) -> Optional[str]:
    """
    Pick the most likely serial device.

    Strategy:
    - If a fixed serial port is configured, use it only if it exists
    - Otherwise enumerate all serial devices
    - Optionally filter by VID/PID
    - Rank remaining candidates using simple heuristics
    """
    ports = list(list_ports.comports())
    if not ports:
        return None

    if log_cfg.port_pick:
        for port in ports:
            log(
                f"[DEBUG] Port candidate: {port.device} "
                f"desc='{port.description}' vid={port.vid} pid={port.pid}"
            )

    if configured_port and configured_port.lower() != "auto":
        for port in ports:
            if port.device == configured_port:
                if log_cfg.port_pick:
                    log(f"[DEBUG] Using explicitly configured serial_port='{configured_port}'")
                return port.device

        if log_cfg.port_pick:
            log(f"[WARN] Configured serial_port='{configured_port}' not found")
        return None

    candidates = []
    for port in ports:
        if configured_vid is not None and (port.vid is None or int(port.vid) != int(configured_vid)):
            continue
        if configured_pid is not None and (port.pid is None or int(port.pid) != int(configured_pid)):
            continue
        candidates.append(port)

    if not candidates:
        candidates = ports

    def score(port: Any) -> int:
        dev = (port.device or "").lower()
        desc = (port.description or "").lower()

        if "bluetooth" in desc:
            return -999

        value = 0
        if "cdc" in desc or "acm" in desc:
            value += 60
        if dev.startswith("/dev/ttyacm"):
            value += 50
        if dev.startswith("/dev/ttyusb"):
            value += 30
        if "usb" in desc:
            value += 10
        return value

    candidates.sort(key=score, reverse=True)

    selected = candidates[0].device if candidates else None
    if log_cfg.port_pick and selected:
        log(f"[DEBUG] Auto-picked serial port: {selected}")

    return selected

# ---------------- Main loop ----------------

def main() -> None:
    """Main application entry point."""
    opts = load_options()

    log_cfg = LogConfig(
        raw_lines=bool(opts.get("log_raw_lines", False)),
        parsed_messages=bool(opts.get("log_parsed_messages", True)),
        unknown_types=bool(opts.get("log_unknown_types", True)),
        mqtt_publishes=bool(opts.get("log_mqtt_publishes", False)),
        port_pick=bool(opts.get("log_port_pick", False)),
    )

    baudrate = int(opts.get("baudrate", 115200))
    configured_port = str(opts.get("serial_port", "auto"))
    configured_vid = normalize_hex_auto(str(opts.get("serial_vid", "auto")))
    configured_pid = normalize_hex_auto(str(opts.get("serial_pid", "auto")))
    timeout_s = float(opts.get("serial_timeout_s", 2.0))

    mqtt_cfg = MqttConfig(
        host=str(opts.get("mqtt_host", "core-mosquitto")),
        port=int(opts.get("mqtt_port", 1883)),
        username=str(opts.get("mqtt_username", "")),
        password=str(opts.get("mqtt_password", "")),
        topic_base=str(opts.get("mqtt_topic_base", "rpl")).rstrip("/"),
        retain=bool(opts.get("mqtt_retain", True)),
    )

    discovery_enabled = bool(opts.get("mqtt_discovery_enable", True))
    discovery_prefix = str(opts.get("mqtt_discovery_prefix", "homeassistant")).rstrip("/")

    seen = DiscoveryStore.load(DISCOVERY_SEEN_FILE) if discovery_enabled else None
    discovery_cfg = (
        DiscoveryConfig(prefix=discovery_prefix)
        if discovery_enabled
        else None
    )

    log(f"[INFO] Starting bridge (baud={baudrate}, port={configured_port}, timeout={timeout_s}s)")
    if configured_vid is not None or configured_pid is not None:
        log(f"[INFO] Serial filter enabled: VID={configured_vid} PID={configured_pid}")
    log(f"[INFO] MQTT: {mqtt_cfg.host}:{mqtt_cfg.port} base='{mqtt_cfg.topic_base}' retain={mqtt_cfg.retain}")

    mqtt_client = mqtt_connect(mqtt_cfg)
    publisher = Publisher(
        mqtt_client,
        mqtt_cfg,
        log_cfg,
        discovery=discovery_cfg,
        seen=seen,
        logger=log,
    )

    while True:
        device = pick_serial_port(
            configured_port,
            configured_vid,
            configured_pid,
            log_cfg=log_cfg,
        )

        if not device:
            log("[WARN] No serial devices found. Reconnecting in 2 seconds...")
            time.sleep(2)
            continue

        log(f"[INFO] Using serial device: {device}")

        try:
            with serial.Serial(device, baudrate=baudrate, timeout=timeout_s) as ser:
                while True:
                    line = ser.readline()
                    if not line:
                        continue

                    text = line.decode("utf-8", errors="replace").strip()
                    if not text:
                        continue

                    if log_cfg.raw_lines:
                        log(f"[RAW] {text}")

                    try:
                        msg = json.loads(text)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(msg, dict):
                        dispatch_message(msg, publisher, log_cfg, log)

        except serial.SerialException as exc:
            log(f"[WARN] Serial error on {device}: {exc}. Reconnecting in 2 seconds...")
            time.sleep(2)
        except Exception as exc:
            log(f"[ERROR] Unexpected error: {exc}. Reconnecting in 2 seconds...")
            time.sleep(2)

if __name__ == "__main__":
    main()