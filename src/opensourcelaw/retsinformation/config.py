from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .models import Source

RETSINFO_BASE_URL = "https://www.retsinformation.dk"
RETSINFO_SITEMAP_URL = f"{RETSINFO_BASE_URL}/eli/sitemap.xml"


def _env_int(name: str, default: int | None) -> int | None:
    value = os.environ.get(name)
    if value is None:
        return default
    if value.strip().lower() in {"", "none", "null"}:
        return None
    return int(value)


def default_sources() -> list[Source]:
    return [
        Source(
            id="retsinformation_eli",
            name="Retsinformation ELI sitemap",
            type="retsinformation_sitemap",
            enabled=True,
            config={
                "base_url": RETSINFO_BASE_URL,
                "sitemap_url": RETSINFO_SITEMAP_URL,
                "sitemap_pages": _env_int("OPENSOURCELAW_RETSINFO_SITEMAP_PAGES", 1),
                "max_items": _env_int("OPENSOURCELAW_RETSINFO_MAX_ITEMS", 25),
                "sitemap_delay_seconds": 2.0,
                "request_delay_seconds": 1.0,
                "timeout_seconds": 30,
                "retries": 3,
                "retry_backoff_seconds": 10.0,
                "retry_status_codes": [429, 500, 502, 503],
                "user_agent": "opensourcelaw-retsinformation-ingest/0.1",
            },
        )
    ]


def load_sources(path: str | Path | None = None) -> list[Source]:
    configured_path = path or os.environ.get("OPENSOURCELAW_SOURCES_FILE")
    if not configured_path:
        return [source for source in default_sources() if source.enabled]

    payload = json.loads(Path(configured_path).read_text(encoding="utf-8"))
    raw_sources: Any = payload.get("sources") if isinstance(payload, dict) else payload
    if not isinstance(raw_sources, list):
        raise ValueError("source config must be a list or an object with a 'sources' list")

    sources = [Source.from_mapping(item) for item in raw_sources]
    return [source for source in sources if source.enabled]
