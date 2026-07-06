from __future__ import annotations

import hashlib

from .http import HttpClient, client_from_config, header_value
from .models import DiscoveredItem, RawFetch, Source, utc_now_iso
from .storage import FilesystemIngestStore


def fetch_discovered_item(
    item: DiscoveredItem,
    source: Source,
    store: FilesystemIngestStore,
    *,
    run_id: str,
    http_client: HttpClient | None = None,
) -> RawFetch:
    fetched_at = utc_now_iso()
    if not item.url:
        return RawFetch(
            source_id=item.source_id,
            external_id=item.external_id,
            fetched_at=fetched_at,
            status_code=None,
            content_type=None,
            etag=None,
            last_modified=None,
            content_hash=None,
            raw_uri=None,
            error="discovered item has no URL",
            url=None,
            content_length=None,
            run_id=run_id,
        )

    client = http_client or client_from_config(source.config, delay_key="request_delay_seconds")
    response = client.get(item.url)
    content_type = header_value(response.headers, "Content-Type")
    etag = header_value(response.headers, "ETag")
    last_modified = header_value(response.headers, "Last-Modified")

    if response.error:
        return RawFetch(
            source_id=item.source_id,
            external_id=item.external_id,
            fetched_at=fetched_at,
            status_code=response.status_code,
            content_type=content_type,
            etag=etag,
            last_modified=last_modified,
            content_hash=None,
            raw_uri=None,
            error=response.error,
            url=item.url,
            content_length=len(response.content) if response.content else None,
            run_id=run_id,
        )

    content_hash = hashlib.sha256(response.content).hexdigest()
    raw_uri = store.write_raw_content(
        source_id=item.source_id,
        external_id=item.external_id,
        fetched_at=fetched_at,
        content_hash=content_hash,
        content_type=content_type,
        url=item.url,
        content=response.content,
    )
    return RawFetch(
        source_id=item.source_id,
        external_id=item.external_id,
        fetched_at=fetched_at,
        status_code=response.status_code,
        content_type=content_type,
        etag=etag,
        last_modified=last_modified,
        content_hash=content_hash,
        raw_uri=raw_uri,
        error=None,
        url=item.url,
        content_length=len(response.content),
        run_id=run_id,
    )
