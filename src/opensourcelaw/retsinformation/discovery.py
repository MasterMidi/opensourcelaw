from __future__ import annotations

import hashlib
import re
import urllib.parse
import xml.etree.ElementTree as ET

from .config import RETSINFO_BASE_URL, RETSINFO_SITEMAP_URL
from .http import HttpClient, client_from_config, header_value
from .models import DiscoveredItem, SitemapPage, Source, utc_now_iso

ELI_RE = re.compile(r"/eli/([a-z]+)/(\d{4})/(\d+)")
TYPE_PRIORITY = {"lta": 0, "mt": 1, "ft": 2}


def sitemap_page_urls(source: Source) -> list[tuple[int, str]]:
    config = source.config
    sitemap_url = str(config.get("sitemap_url") or RETSINFO_SITEMAP_URL)
    page_count = int(config.get("sitemap_pages") or 1)
    return [(page, _with_query_param(sitemap_url, "page", str(page))) for page in range(1, page_count + 1)]


def fetch_sitemap_pages(source: Source, http_client: HttpClient | None = None) -> list[SitemapPage]:
    client = http_client or client_from_config(source.config, delay_key="sitemap_delay_seconds")
    pages: list[SitemapPage] = []
    for page_number, url in sitemap_page_urls(source):
        fetched_at = utc_now_iso()
        response = client.get(url)
        content_hash = hashlib.sha256(response.content).hexdigest() if response.content and not response.error else None
        pages.append(
            SitemapPage(
                source_id=source.id,
                page_number=page_number,
                url=url,
                fetched_at=fetched_at,
                status_code=response.status_code,
                content_type=header_value(response.headers, "Content-Type"),
                content_hash=content_hash,
                raw_uri=None,
                error=response.error,
                content=response.content if not response.error else None,
            )
        )
    return pages


def discover_items_from_sitemap_pages(source: Source, pages: list[SitemapPage]) -> list[DiscoveredItem]:
    config = source.config
    allowed_types = {str(item) for item in config.get("types", config.get("item_types", []))}
    allowed_years = {int(item) for item in config.get("years", [])}
    max_items = config.get("max_items")
    max_items = int(max_items) if max_items is not None else None
    base_url = str(config.get("base_url") or RETSINFO_BASE_URL).rstrip("/")

    items: list[DiscoveredItem] = []
    seen: set[str] = set()
    for page in pages:
        if page.error or not page.content:
            continue
        try:
            root = ET.fromstring(page.content)
        except ET.ParseError:
            continue
        for url in _loc_values(root):
            match = ELI_RE.search(url)
            if not match:
                continue
            eli_type = match.group(1)
            year = int(match.group(2))
            number = int(match.group(3))
            if allowed_types and eli_type not in allowed_types:
                continue
            if allowed_years and year not in allowed_years:
                continue
            external_id = f"{eli_type}/{year}/{number}"
            if external_id in seen:
                continue
            seen.add(external_id)
            source_url = f"{base_url}/eli/{eli_type}/{year}/{number}"
            xml_url = f"{source_url}/xml"
            items.append(
                DiscoveredItem(
                    source_id=source.id,
                    external_id=external_id,
                    url=xml_url,
                    metadata={
                        "eli_type": eli_type,
                        "year": year,
                        "number": number,
                        "eli_uri": f"/eli/{eli_type}/{year}/{number}",
                        "source_url": source_url,
                        "xml_url": xml_url,
                        "sitemap_url": url,
                    },
                )
            )
            if max_items is not None and len(items) >= max_items:
                return _sorted_items(items)
    return _sorted_items(items)


def _loc_values(root: ET.Element) -> list[str]:
    values: list[str] = []
    for element in root.iter():
        tag = str(element.tag)
        if tag == "loc" or tag.endswith("}loc"):
            if element.text and element.text.strip():
                values.append(element.text.strip())
    return values


def _sorted_items(items: list[DiscoveredItem]) -> list[DiscoveredItem]:
    return sorted(
        items,
        key=lambda item: (
            TYPE_PRIORITY.get(str(item.metadata.get("eli_type")), 99),
            -int(item.metadata.get("year", 0)),
            -int(item.metadata.get("number", 0)),
        ),
    )


def _with_query_param(url: str, key: str, value: str) -> str:
    parts = urllib.parse.urlsplit(url)
    query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(query), parts.fragment))
