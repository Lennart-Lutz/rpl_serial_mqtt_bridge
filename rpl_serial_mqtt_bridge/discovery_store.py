from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Set

# ----------------- Discovery persistence -----------------

@dataclass
class DiscoveryStore:
    """
    Persistent store for already-published MQTT Discovery objects.

    This prevents publishing duplicate discovery payloads after every restart.
    """
    path: Path
    seen: Set[str]

    @classmethod
    def load(cls, path: str) -> "DiscoveryStore":
        """Load the discovery store from disk."""
        file_path = Path(path)

        try:
            raw = json.loads(file_path.read_text(encoding="utf-8"))
            items = raw.get("seen", [])
            return cls(path=file_path, seen={str(item) for item in items})
        except FileNotFoundError:
            return cls(path=file_path, seen=set())
        except Exception:
            # If the file is corrupted, start with an empty store.
            return cls(path=file_path, seen=set())

    def save(self) -> None:
        """Atomically write the discovery store to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps({"seen": sorted(self.seen)}, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.path)

    def has(self, key: str) -> bool:
        """Return whether a discovery key is already known."""
        return key in self.seen

    def add(self, key: str) -> None:
        """Add a new discovery key and persist the updated store."""
        if key not in self.seen:
            self.seen.add(key)
            self.save()