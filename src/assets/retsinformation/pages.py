from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlparse

from dagster import AssetExecutionContext, StaticPartitionsDefinition, asset
from defusedxml import ElementTree

from src.assets.retsinformation.sitemap import SitemapPageRef
from src.resources import RetsinformationHttpResource

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class DocumentType(StrEnum):
    FC = "fc"
    FOB = "fob"
    ILT = "ilt"
    LTA = "lta"
    LTB = "ltb"
    LTC = "ltc"
    MT = "mt"
    RETSINFO = "retsinfo"


DOCUMENT_TYPES_WITH_ID_PREFIX_YEAR = {DocumentType.FC}


@dataclass(frozen=True)
class EliDocumentUrlParts:
    id: str
    year: str
    type: DocumentType


@dataclass(frozen=True)
class SitemapEntry:
    url: str
    lastmod: str
    id: str
    year: str
    type: DocumentType


def parse_eli_document_url(url: str) -> EliDocumentUrlParts | None:
    parts = [part for part in urlparse(url).path.split("/") if part]

    if len(parts) not in {3, 4} or parts[0] != "eli":
        return None

    try:
        doc_type = DocumentType(parts[1])
    except ValueError:
        return None

    if len(parts) == 3:
        document_id = parts[2]

        if doc_type not in DOCUMENT_TYPES_WITH_ID_PREFIX_YEAR:
            return None

        year = document_id[:4]
    else:
        year, document_id = parts[2:]

    if len(year) != 4 or not year.isdigit():
        return None

    return EliDocumentUrlParts(id=document_id, year=year, type=doc_type)


retsinfo_sitemap_page_partitions = StaticPartitionsDefinition(
    [str(page) for page in range(1, 22)]
)


@asset(group_name="retsinformation", partitions_def=retsinfo_sitemap_page_partitions)
def retsinfo_sitemap_page(
    context: AssetExecutionContext,
    retsinfo_sitemap_index: list[SitemapPageRef],
    retsinformation_http: RetsinformationHttpResource,
) -> list[SitemapEntry]:
    page = context.partition_key

    response = retsinformation_http.get(
        next(x for x in retsinfo_sitemap_index if x.page == page).url,
        follow_redirects=True,
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

        url_str = loc_element.text.strip()
        url_parts = parse_eli_document_url(url_str)

        if url_parts is None:
            context.log.warning(f"Skipping unrecognized ELI URL: {url_str}")
            continue

        entry = SitemapEntry(
            url=url_str,
            lastmod=lastmod_element.text.strip(),
            id=url_parts.id,
            year=url_parts.year,
            type=url_parts.type,
        )
        context.log.debug(
            f"Found {entry.type} entry {entry.year}/{entry.id}. URL: {entry.url}"
        )
        entries.append(entry)

    type_counts = Counter(entry.type.value for entry in entries)

    context.add_output_metadata(
        {
            "entry_count": len(entries),
            "type_counts": dict(sorted(type_counts.items())),
        }
    )

    return entries
