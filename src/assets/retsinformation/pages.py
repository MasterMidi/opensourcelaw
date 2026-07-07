from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import httpx
from dagster import AssetExecutionContext, StaticPartitionsDefinition, asset
from defusedxml import ElementTree

from src.assets.retsinformation.sitemap import SitemapPageRef

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


@dataclass
class SitemapEntry:
    url: str
    lastmod: str


retsinfo_sitemap_page_partitions = StaticPartitionsDefinition(
    [str(page) for page in range(1, 22)]
)


@asset(group_name="retsinformation", partitions_def=retsinfo_sitemap_page_partitions)
def retsinfo_sitemap_page(
    context: AssetExecutionContext, retsinfo_sitemap_index: list[SitemapPageRef]
) -> list[SitemapEntry]:
    page = context.partition_key

    response = httpx.get(
        next(x for x in retsinfo_sitemap_index if x.page == page).url,
        timeout=30.0,
        follow_redirects=True,
        headers={"User-Agent": "opensourcelaw/0.1"},
    )

    response.raise_for_status()

    root = ElementTree.fromstring(response.content)

    entries = []

    for sitemap_element in root.findall("sm:url", SITEMAP_NS):
        loc_element = sitemap_element.find("sm:loc", SITEMAP_NS)
        lastmod_element = sitemap_element.find("sm:lastmod", SITEMAP_NS)

        if loc_element is None or loc_element.text is None:
            continue

        if lastmod_element is None or lastmod_element.text is None:
            continue

        modified = lastmod_element.text.strip()
        url_str = loc_element.text.strip()
        # url = urlparse(url_str)
        # split_path = url.path.rsplit(sep="/", maxsplit=4)
        # id = split_path[0]
        # year = split_path[1]
        # doc_type = split_path[2]

        entry = SitemapEntry(url=url_str, lastmod=modified)
        context.log.debug(f"Found entry. URL: {entry.url}")
        entries.append(entry)

    context.add_output_metadata(
        {
            "entry_count": len(entries),
        }
    )

    return entries
