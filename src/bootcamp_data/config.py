from dataclasses import dataclass
from pathlib import Path
import json
import time
import httpx
import pandas as pd


@dataclass(frozen=True)
class Paths:
    root: Path
    raw: Path
    cache: Path
    processed: Path
    external: Path

def make_paths(root: Path) -> Paths:
    data = root / "data"
    return Paths(
        root=root,
        raw=data / "raw",
        cache=data / "cache",
        processed=data / "processed",
        external=data / "external",
    )


def fetch_json_cached(url: str, cache_path: Path, *, ttl_s: int | None = None) -> dict:
    """Offline-first JSON fetch with optional TTL."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Offline-first default: if cache exists, use it (unless TTL says it's too old)
    if cache_path.exists():
        age_s = time.time() - cache_path.stat().st_mtime
        if ttl_s is None or age_s < ttl_s:
            return json.loads(cache_path.read_text(encoding="utf-8"))

        # Otherwise: fetch and overwrite cache
    with httpx.Client(timeout=20.0) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.json()

    cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


df = pd.read_csv("C:\\Users\\rashe\\OneDrive\\سطح المكتب\\SDAIA_Ai\\AiWeekTow\\orders.csv",
                 dtype= {
                     "user_id":str
                 })
print(df)