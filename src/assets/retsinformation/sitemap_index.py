from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

from dagster import AssetExecutionContext, asset
from defusedxml import ElementTree

from src.resources import RetsinformationHttpResource

RETSINFO_ELI_SITEMAP_URL = "https://www.retsinformation.dk/eli/sitemap.xml"
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


@dataclass
class SitemapPageRef:
    page: str
    url: str


@asset(group_name="retsinformation", pool="retsinformation_http")
def retsinfo_sitemap_index(
    context: AssetExecutionContext,
    retsinformation_http: RetsinformationHttpResource,
) -> list[SitemapPageRef]:
    root = ElementTree.fromstring(
        retsinformation_http.get_bytes(RETSINFO_ELI_SITEMAP_URL)
    )

    refs = []

    for sitemap_element in root.findall("sm:sitemap", SITEMAP_NS):
        loc_element = sitemap_element.find("sm:loc", SITEMAP_NS)

        if loc_element is None or loc_element.text is None:
            continue

        url = loc_element.text.strip()
        query = parse_qs(urlparse(url).query)
        page = query["page"][0]
        ref = SitemapPageRef(page=page, url=url)
        context.log.debug(f"Found page {ref.page}. URL: {ref.url}")
        refs.append(ref)

    context.add_output_metadata(
        {
            "ref_count": len(refs),
        }
    )

    return refs
