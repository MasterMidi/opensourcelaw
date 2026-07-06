from __future__ import annotations

import json
import os
import re
import tempfile
import urllib.parse
from pathlib import Path
from typing import Any, Iterable

from .models import ChangedRawFetch, DiscoveredItem, RawFetch, SitemapPage, Source


class FilesystemIngestStore:
    def __init__(self, root_path: str | Path):
        self.root_path = Path(root_path)

    def write_sources(self, sources: list[Source], run_id: str) -> Path:
        return self.write_run_artifact(run_id, "sources", [source.to_dict() for source in sources])

    def write_sitemap_page(self, page: SitemapPage) -> str | None:
        if not page.content or not page.content_hash:
            return None
        extension = extension_for(page.content_type, page.url)
        path = (
            self.root_path
            / "discovery"
            / "sitemap_pages"
            / _safe_segment(page.source_id)
            / f"page-{page.page_number}-{_safe_timestamp(page.fetched_at)}-{page.content_hash[:16]}{extension}"
        )
        _atomic_write_bytes(path, page.content)
        page.raw_uri = str(path)
        return str(path)

    def write_sitemap_pages(self, pages: list[SitemapPage], run_id: str) -> Path:
        return self.write_run_artifact(run_id, "retsinformation_sitemap_pages", [page.to_dict() for page in pages])

    def write_discovered_items(self, items: list[DiscoveredItem], run_id: str) -> Path:
        return self.write_run_artifact(run_id, "discovered_items", [item.to_dict() for item in items])

    def write_raw_content(
        self,
        *,
        source_id: str,
        external_id: str,
        fetched_at: str,
        content_hash: str,
        content_type: str | None,
        url: str | None,
        content: bytes,
    ) -> str:
        extension = extension_for(content_type, url)
        path = self.root_path / "raw" / _safe_segment(source_id)
        for part in external_id_to_path_parts(external_id):
            path /= part
        path /= f"{_safe_timestamp(fetched_at)}-{content_hash[:16]}{extension}"
        _atomic_write_bytes(path, content)
        return str(path)

    def record_raw_fetches(self, fetches: list[RawFetch], run_id: str) -> Path:
        rows = [fetch.to_dict() for fetch in fetches]
        self._append_jsonl(self.root_path / "metadata" / "raw_fetches.jsonl", rows)
        return self.write_run_artifact(run_id, "raw_fetches", rows)

    def read_raw_fetches(self, *, exclude_run_id: str | None = None) -> list[RawFetch]:
        path = self.root_path / "metadata" / "raw_fetches.jsonl"
        if not path.exists():
            return []
        fetches: list[RawFetch] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            fetch = RawFetch.from_mapping(json.loads(line))
            if exclude_run_id and fetch.run_id == exclude_run_id:
                continue
            fetches.append(fetch)
        return fetches

    def record_changed_raw_fetches(self, changes: list[ChangedRawFetch], run_id: str) -> Path:
        rows = [change.to_dict() for change in changes]
        self._append_jsonl(self.root_path / "metadata" / "changed_raw_fetches.jsonl", rows)
        return self.write_run_artifact(run_id, "changed_raw_fetches", rows)

    def write_run_artifact(self, run_id: str, name: str, data: Any) -> Path:
        path = self.root_path / "runs" / _safe_segment(run_id) / f"{_safe_segment(name)}.json"
        _atomic_write_text(path, json.dumps(data, indent=2, ensure_ascii=False))
        return path

    def _append_jsonl(self, path: Path, rows: Iterable[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False))
                file.write("\n")


def external_id_to_path_parts(external_id: str) -> list[str]:
    parts = [part for part in external_id.split("/") if part]
    if not parts:
        parts = [external_id]
    return [_safe_segment(part) for part in parts]


def extension_for(content_type: str | None, url: str | None) -> str:
    content_type = (content_type or "").lower()
    url = (url or "").lower()
    if "xml" in content_type or url.endswith("/xml") or url.endswith(".xml"):
        return ".xml"
    if "json" in content_type or url.endswith(".json"):
        return ".json"
    if "html" in content_type or url.endswith(".html") or url.endswith(".htm"):
        return ".html"
    if "pdf" in content_type or url.endswith(".pdf"):
        return ".pdf"
    if "text" in content_type or url.endswith(".txt"):
        return ".txt"
    return ".bin"


def _safe_segment(value: str) -> str:
    return urllib.parse.quote(str(value), safe="-_.")


def _safe_timestamp(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "", value)


def _atomic_write_text(path: Path, content: str) -> None:
    _atomic_write(path, content.encode("utf-8"))


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    _atomic_write(path, content)


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(content)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
