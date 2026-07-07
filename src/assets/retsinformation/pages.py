from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from io import BytesIO
from time import perf_counter

from dagster import AssetExecutionContext, asset
from defusedxml import ElementTree

from src.assets.retsinformation.sitemap import SitemapPageRef
from src.resources import RetsinformationHttpResource

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
SITEMAP_URL_TAG = "{http://www.sitemaps.org/schemas/sitemap/0.9}url"
SITEMAP_LOC_TAG = "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
SITEMAP_LASTMOD_TAG = "{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod"


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


@dataclass(frozen=True)
class ParsedSitemapPage:
    entries: list[SitemapEntry]
    skipped_count: int


def parse_eli_document_url(url: str) -> EliDocumentUrlParts | None:
    _prefix, separator, path = url.partition("/eli/")

    if not separator:
        return None

    path = path.split("?", 1)[0].split("#", 1)[0].strip("/")
    parts = path.split("/")

    if len(parts) not in {2, 3}:
        return None

    try:
        doc_type = DocumentType(parts[0])
    except ValueError:
        return None

    if len(parts) == 2:
        document_id = parts[1]

        if doc_type not in DOCUMENT_TYPES_WITH_ID_PREFIX_YEAR:
            return None

        year = document_id[:4]
    else:
        year, document_id = parts[1:]

    if len(year) != 4 or not year.isdigit():
        return None

    return EliDocumentUrlParts(id=document_id, year=year, type=doc_type)


def parse_eli_sitemap_page(xml_content: bytes) -> ParsedSitemapPage:
    entries: list[SitemapEntry] = []
    skipped_count = 0

    for _event, element in ElementTree.iterparse(BytesIO(xml_content), events=("end",)):
        if element.tag != SITEMAP_URL_TAG:
            continue

        loc_text = element.findtext(SITEMAP_LOC_TAG)
        lastmod_text = element.findtext(SITEMAP_LASTMOD_TAG)

        if loc_text is None or lastmod_text is None:
            skipped_count += 1
            element.clear()
            continue

        url = loc_text.strip()
        url_parts = parse_eli_document_url(url)

        if url_parts is None:
            skipped_count += 1
            element.clear()
            continue

        entries.append(
            SitemapEntry(
                url=url,
                lastmod=lastmod_text.strip(),
                id=url_parts.id,
                year=url_parts.year,
                type=url_parts.type,
            )
        )
        element.clear()

    return ParsedSitemapPage(entries=entries, skipped_count=skipped_count)


@asset(group_name="retsinformation")
def retsinfo_sitemap_page(
    context: AssetExecutionContext,
    retsinfo_sitemap_index: list[SitemapPageRef],
    retsinformation_http: RetsinformationHttpResource,
) -> list[SitemapEntry]:
    entries = []
    skipped_count = 0
    total_fetch_seconds = 0.0
    total_parse_seconds = 0.0

    for page_ref in sorted(retsinfo_sitemap_index, key=lambda ref: int(ref.page)):
        context.log.info(f"Fetching sitemap page {page_ref.page}: {page_ref.url}")
        fetch_start = perf_counter()
        response = retsinformation_http.get(page_ref.url, follow_redirects=True)

        response.raise_for_status()
        fetch_seconds = perf_counter() - fetch_start
        total_fetch_seconds += fetch_seconds

        parse_start = perf_counter()
        parsed_page = parse_eli_sitemap_page(response.content)
        parse_seconds = perf_counter() - parse_start
        total_parse_seconds += parse_seconds

        entries.extend(parsed_page.entries)
        skipped_count += parsed_page.skipped_count

        context.log.info(
            f"Loaded {len(parsed_page.entries)} entries from sitemap page {page_ref.page} "
            f"in {parse_seconds:.2f}s after {fetch_seconds:.2f}s fetch "
            f"({len(entries)} total, {parsed_page.skipped_count} skipped on page)"
        )

    type_counts = Counter(entry.type.value for entry in entries)
    year_counts = Counter(entry.year for entry in entries)

    context.add_output_metadata(
        {
            "sitemap_page_count": len(retsinfo_sitemap_index),
            "entry_count": len(entries),
            "skipped_count": skipped_count,
            "type_counts": dict(sorted(type_counts.items())),
            "year_count": len(year_counts),
            "fetch_seconds": round(total_fetch_seconds, 3),
            "parse_seconds": round(total_parse_seconds, 3),
        }
    )

    return entries
