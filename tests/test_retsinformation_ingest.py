from __future__ import annotations

import hashlib

from opensourcelaw.retsinformation.change_detection import classify_raw_fetches
from opensourcelaw.retsinformation.discovery import discover_items_from_sitemap_pages, sitemap_page_urls
from opensourcelaw.retsinformation.models import RawFetch, SitemapPage, Source
from opensourcelaw.retsinformation.storage import FilesystemIngestStore


def test_sitemap_discovery_extracts_eli_xml_urls() -> None:
    source = Source(
        id="retsinformation_eli",
        name="Retsinformation",
        type="retsinformation_sitemap",
        config={"base_url": "https://www.retsinformation.dk", "max_items": 2},
    )
    page = SitemapPage(
        source_id=source.id,
        page_number=1,
        url="https://www.retsinformation.dk/eli/sitemap.xml?page=1",
        fetched_at="2026-07-06T12:00:00Z",
        status_code=200,
        content_type="application/xml",
        content_hash="hash",
        raw_uri=None,
        error=None,
        content=b"""
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://www.retsinformation.dk/eli/lta/2024/460</loc></url>
          <url><loc>https://www.retsinformation.dk/eli/mt/2023/10</loc></url>
          <url><loc>https://www.retsinformation.dk/not-eli</loc></url>
        </urlset>
        """,
    )

    items = discover_items_from_sitemap_pages(source, [page])

    assert [item.external_id for item in items] == ["lta/2024/460", "mt/2023/10"]
    assert items[0].url == "https://www.retsinformation.dk/eli/lta/2024/460/xml"
    assert items[0].metadata["source_url"] == "https://www.retsinformation.dk/eli/lta/2024/460"


def test_sitemap_page_urls_preserve_existing_query() -> None:
    source = Source(
        id="retsinformation_eli",
        name="Retsinformation",
        type="retsinformation_sitemap",
        config={"sitemap_url": "https://example.test/sitemap.xml?lang=da", "sitemap_pages": 2},
    )

    assert sitemap_page_urls(source) == [
        (1, "https://example.test/sitemap.xml?lang=da&page=1"),
        (2, "https://example.test/sitemap.xml?lang=da&page=2"),
    ]


def test_filesystem_store_writes_raw_content_and_metadata(tmp_path) -> None:
    store = FilesystemIngestStore(tmp_path)
    content = b"<Dokument>raw</Dokument>"
    content_hash = hashlib.sha256(content).hexdigest()

    raw_uri = store.write_raw_content(
        source_id="retsinformation_eli",
        external_id="lta/2024/460",
        fetched_at="2026-07-06T12:00:00Z",
        content_hash=content_hash,
        content_type="application/xml",
        url="https://www.retsinformation.dk/eli/lta/2024/460/xml",
        content=content,
    )

    assert (tmp_path / "raw" / "retsinformation_eli" / "lta" / "2024" / "460").exists()
    assert raw_uri.endswith(".xml")

    fetch = RawFetch(
        source_id="retsinformation_eli",
        external_id="lta/2024/460",
        fetched_at="2026-07-06T12:00:00Z",
        status_code=200,
        content_type="application/xml",
        etag="abc",
        last_modified=None,
        content_hash=content_hash,
        raw_uri=raw_uri,
        error=None,
        url="https://www.retsinformation.dk/eli/lta/2024/460/xml",
        content_length=len(content),
        run_id="run-1",
    )
    store.record_raw_fetches([fetch], "run-1")

    assert store.read_raw_fetches()[0].content_hash == content_hash
    assert store.read_raw_fetches(exclude_run_id="run-1") == []


def test_change_detection_classifies_new_changed_unchanged_and_failed() -> None:
    historical = [
        _raw_fetch("lta/2024/1", "2026-07-05T12:00:00Z", "old", run_id="run-1"),
        _raw_fetch("lta/2024/2", "2026-07-05T12:00:00Z", "same", run_id="run-1"),
    ]
    current = [
        _raw_fetch("lta/2024/1", "2026-07-06T12:00:00Z", "new", run_id="run-2"),
        _raw_fetch("lta/2024/2", "2026-07-06T12:00:00Z", "same", run_id="run-2"),
        _raw_fetch("lta/2024/3", "2026-07-06T12:00:00Z", "first", run_id="run-2"),
        _raw_fetch("lta/2024/4", "2026-07-06T12:00:00Z", None, error="timeout", run_id="run-2"),
    ]

    statuses = {change.external_id: change.status for change in classify_raw_fetches(current, historical)}

    assert statuses == {
        "lta/2024/1": "changed",
        "lta/2024/2": "unchanged",
        "lta/2024/3": "new",
        "lta/2024/4": "failed",
    }


def _raw_fetch(
    external_id: str,
    fetched_at: str,
    content_hash: str | None,
    *,
    error: str | None = None,
    run_id: str,
) -> RawFetch:
    return RawFetch(
        source_id="retsinformation_eli",
        external_id=external_id,
        fetched_at=fetched_at,
        status_code=None if error else 200,
        content_type="application/xml" if not error else None,
        etag=None,
        last_modified=None,
        content_hash=content_hash,
        raw_uri=f"raw/{external_id}.xml" if content_hash else None,
        error=error,
        url=f"https://www.retsinformation.dk/eli/{external_id}/xml",
        content_length=1 if content_hash else None,
        run_id=run_id,
    )
