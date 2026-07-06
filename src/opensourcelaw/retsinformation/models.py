from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class Source:
    id: str
    name: str
    type: str
    enabled: bool = True
    config: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "Source":
        known_fields = {"id", "name", "type", "enabled", "config"}
        config = dict(value.get("config") or {})
        for key, item in value.items():
            if key not in known_fields:
                config.setdefault(key, item)
        return cls(
            id=str(value["id"]),
            name=str(value.get("name") or value["id"]),
            type=str(value["type"]),
            enabled=bool(value.get("enabled", True)),
            config=config,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "enabled": self.enabled,
            "config": self.config,
        }


@dataclass
class SitemapPage:
    source_id: str
    page_number: int
    url: str
    fetched_at: str
    status_code: int | None
    content_type: str | None
    content_hash: str | None
    raw_uri: str | None
    error: str | None
    content: bytes | None = field(default=None, repr=False)

    def to_dict(self, *, include_content: bool = False) -> dict[str, Any]:
        data: dict[str, Any] = {
            "source_id": self.source_id,
            "page_number": self.page_number,
            "url": self.url,
            "fetched_at": self.fetched_at,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "content_hash": self.content_hash,
            "raw_uri": self.raw_uri,
            "error": self.error,
            "content_bytes": len(self.content) if self.content is not None else None,
        }
        if include_content:
            data["content"] = self.content.decode("utf-8", errors="replace") if self.content else None
        return data


@dataclass
class DiscoveredItem:
    source_id: str
    external_id: str
    url: str | None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "external_id": self.external_id,
            "url": self.url,
            "metadata": self.metadata,
        }


@dataclass
class RawFetch:
    source_id: str
    external_id: str
    fetched_at: str
    status_code: int | None
    content_type: str | None
    etag: str | None
    last_modified: str | None
    content_hash: str | None
    raw_uri: str | None
    error: str | None
    url: str | None
    content_length: int | None
    run_id: str | None = None

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "RawFetch":
        return cls(
            source_id=str(value["source_id"]),
            external_id=str(value["external_id"]),
            fetched_at=str(value["fetched_at"]),
            status_code=value.get("status_code"),
            content_type=value.get("content_type"),
            etag=value.get("etag"),
            last_modified=value.get("last_modified"),
            content_hash=value.get("content_hash"),
            raw_uri=value.get("raw_uri"),
            error=value.get("error"),
            url=value.get("url"),
            content_length=value.get("content_length"),
            run_id=value.get("run_id"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "external_id": self.external_id,
            "fetched_at": self.fetched_at,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "content_hash": self.content_hash,
            "raw_uri": self.raw_uri,
            "error": self.error,
            "url": self.url,
            "content_length": self.content_length,
            "run_id": self.run_id,
        }


@dataclass
class ChangedRawFetch:
    source_id: str
    external_id: str
    fetched_at: str
    status: str
    content_hash: str | None
    previous_content_hash: str | None
    raw_uri: str | None
    run_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "external_id": self.external_id,
            "fetched_at": self.fetched_at,
            "status": self.status,
            "content_hash": self.content_hash,
            "previous_content_hash": self.previous_content_hash,
            "raw_uri": self.raw_uri,
            "run_id": self.run_id,
            "metadata": self.metadata,
        }
