from __future__ import annotations

from collections import Counter, defaultdict

from dagster import MetadataValue, asset

from opensourcelaw.resources import IngestStoreResource
from opensourcelaw.retsinformation.change_detection import classify_raw_fetches
from opensourcelaw.retsinformation.config import load_sources
from opensourcelaw.retsinformation.discovery import discover_items_from_sitemap_pages, fetch_sitemap_pages
from opensourcelaw.retsinformation.fetch import fetch_discovered_item
from opensourcelaw.retsinformation.http import client_from_config
from opensourcelaw.retsinformation.models import ChangedRawFetch, DiscoveredItem, RawFetch, SitemapPage, Source

GROUP_NAME = "retsinformation_raw"


@asset(group_name=GROUP_NAME, compute_kind="python")
def sources(context, ingest_store: IngestStoreResource) -> list[Source]:
    loaded_sources = load_sources()
    store = ingest_store.create_store()
    artifact_path = store.write_sources(loaded_sources, context.run_id)
    context.add_output_metadata(
        {
            "source_count": len(loaded_sources),
            "source_ids": ", ".join(source.id for source in loaded_sources),
            "artifact_path": MetadataValue.path(str(artifact_path)),
        }
    )
    return loaded_sources


@asset(group_name=GROUP_NAME, compute_kind="python")
def retsinformation_sitemap_pages(
    context,
    sources: list[Source],
    ingest_store: IngestStoreResource,
) -> list[SitemapPage]:
    store = ingest_store.create_store()
    pages: list[SitemapPage] = []
    for source in _sources_by_type(sources, "retsinformation_sitemap"):
        source_pages = fetch_sitemap_pages(source)
        for page in source_pages:
            store.write_sitemap_page(page)
        pages.extend(source_pages)

    artifact_path = store.write_sitemap_pages(pages, context.run_id)
    context.add_output_metadata(
        {
            "page_count": len(pages),
            "successful_pages": sum(1 for page in pages if not page.error),
            "failed_pages": sum(1 for page in pages if page.error),
            "artifact_path": MetadataValue.path(str(artifact_path)),
        }
    )
    return pages


@asset(group_name=GROUP_NAME, compute_kind="python")
def discovered_items(
    context,
    sources: list[Source],
    retsinformation_sitemap_pages: list[SitemapPage],
    ingest_store: IngestStoreResource,
) -> list[DiscoveredItem]:
    store = ingest_store.create_store()
    source_by_id = {source.id: source for source in sources}
    pages_by_source: dict[str, list[SitemapPage]] = defaultdict(list)
    for page in retsinformation_sitemap_pages:
        pages_by_source[page.source_id].append(page)

    items: list[DiscoveredItem] = []
    for source_id, source_pages in pages_by_source.items():
        source = source_by_id.get(source_id)
        if source is None:
            continue
        items.extend(discover_items_from_sitemap_pages(source, source_pages))

    artifact_path = store.write_discovered_items(items, context.run_id)
    by_source = Counter(item.source_id for item in items)
    context.add_output_metadata(
        {
            "item_count": len(items),
            "source_counts": dict(by_source),
            "artifact_path": MetadataValue.path(str(artifact_path)),
        }
    )
    return items


@asset(group_name=GROUP_NAME, compute_kind="python")
def raw_fetches(
    context,
    sources: list[Source],
    discovered_items: list[DiscoveredItem],
    ingest_store: IngestStoreResource,
) -> list[RawFetch]:
    store = ingest_store.create_store()
    source_by_id = {source.id: source for source in sources}
    items_by_source: dict[str, list[DiscoveredItem]] = defaultdict(list)
    for item in discovered_items:
        items_by_source[item.source_id].append(item)

    fetches: list[RawFetch] = []
    for source_id, source_items in items_by_source.items():
        source = source_by_id.get(source_id)
        if source is None:
            continue
        client = client_from_config(source.config, delay_key="request_delay_seconds")
        for item in source_items:
            fetches.append(fetch_discovered_item(item, source, store, run_id=context.run_id, http_client=client))

    artifact_path = store.record_raw_fetches(fetches, context.run_id)
    context.add_output_metadata(
        {
            "fetch_count": len(fetches),
            "successful_fetches": sum(1 for fetch in fetches if not fetch.error),
            "failed_fetches": sum(1 for fetch in fetches if fetch.error),
            "artifact_path": MetadataValue.path(str(artifact_path)),
        }
    )
    return fetches


@asset(group_name=GROUP_NAME, compute_kind="python")
def changed_raw_fetches(
    context,
    raw_fetches: list[RawFetch],
    ingest_store: IngestStoreResource,
) -> list[ChangedRawFetch]:
    store = ingest_store.create_store()
    historical_fetches = store.read_raw_fetches(exclude_run_id=context.run_id)
    changes = classify_raw_fetches(raw_fetches, historical_fetches)
    artifact_path = store.record_changed_raw_fetches(changes, context.run_id)
    by_status = Counter(change.status for change in changes)
    context.add_output_metadata(
        {
            "change_count": len(changes),
            "status_counts": dict(by_status),
            "artifact_path": MetadataValue.path(str(artifact_path)),
        }
    )
    return changes


def _sources_by_type(sources: list[Source], source_type: str) -> list[Source]:
    return [source for source in sources if source.type == source_type]


RETSINFORMATION_RAW_ASSETS = [
    sources,
    retsinformation_sitemap_pages,
    discovered_items,
    raw_fetches,
    changed_raw_fetches,
]
