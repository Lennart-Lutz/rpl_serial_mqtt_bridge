from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Set


@dataclass
class DiscoveryStore:
    path: Path
    seen: Set[str]

    @classmethod
    def load(cls, path: str) -> "DiscoveryStore":
        p = Path(path)
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            items = raw.get("seen", [])
            return cls(path=p, seen=set(str(x) for x in items))
        except FileNotFoundError:
            return cls(path=p, seen=set())
        except Exception:
            # corrupted -> start fresh
            return cls(path=p, seen=set())

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"seen": sorted(self.seen)}, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def has(self, key: str) -> bool:
        return key in self.seen

    def add(self, key: str) -> None:
        if key not in self.seen:
            self.seen.add(key)
            self.save()