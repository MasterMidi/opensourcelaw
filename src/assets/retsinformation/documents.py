import json
from collections import Counter
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from urllib.parse import urlparse, urlunparse

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from src.assets.retsinformation.pages import DocumentType, SitemapEntry
from src.resources import RetsinformationHttpResource


document_year_partitions = StaticPartitionsDefinition(
    [str(year) for year in range(1985, date.today().year + 1)]
)


class DocumentContentSource(StrEnum):
    XML_ENDPOINT = "xml_endpoint"
    API_DOCUMENT = "api_document"


@dataclass(frozen=True)
class DocumentRefSet:
    document_type: DocumentType
    year: str
    entries: list[SitemapEntry]


@dataclass(frozen=True)
class DocumentPage:
    entry: SitemapEntry
    source_url: str
    source: DocumentContentSource
    status_code: int
    content_type: str
    body: str
    bytes_downloaded: int


@dataclass(frozen=True)
class DocumentFetchFailure:
    entry: SitemapEntry
    source_url: str
    source: DocumentContentSource
    status_code: int
    reason: str


@dataclass(frozen=True)
class DocumentPageBatch:
    document_type: DocumentType
    year: str
    pages: list[DocumentPage]
    failures: list[DocumentFetchFailure]


def document_xml_url(url: str) -> str:
    base_url = url.rstrip("/")

    if base_url.endswith("/xml"):
        return base_url

    return f"{base_url}/xml"


def document_api_url(url: str) -> str:
    parsed_url = urlparse(url.rstrip("/"))
    path = parsed_url.path

    if path.endswith("/xml"):
        path = path[: -len("/xml")]

    return urlunparse(
        parsed_url._replace(path=f"/api/document{path}", query="", fragment="")
    )


def api_document_response_has_document(body: str) -> bool:
    try:
        documents = json.loads(body)
    except json.JSONDecodeError:
        return False

    if not isinstance(documents, list) or not documents:
        return False

    first_document = documents[0]

    if not isinstance(first_document, dict):
        return False

    return first_document.get("id") != -1


def _build_document_refs(
    *,
    context: AssetExecutionContext,
    entries: list[SitemapEntry],
    document_type: DocumentType,
) -> DocumentRefSet:
    year = context.partition_key
    refs = [
        entry for entry in entries if entry.type == document_type and entry.year == year
    ]

    context.log.info(
        f"Found {len(refs)} {document_type.value} document refs for year {year}"
    )

    metadata = {
        "document_type": document_type.value,
        "year": year,
        "ref_count": len(refs),
    }

    if refs:
        metadata["first_url"] = MetadataValue.url(refs[0].url)

    context.add_output_metadata(metadata)

    return DocumentRefSet(document_type=document_type, year=year, entries=refs)


def _fetch_document_pages(
    *,
    context: AssetExecutionContext,
    refs: DocumentRefSet,
    retsinformation_http: RetsinformationHttpResource,
) -> DocumentPageBatch:
    context.log.info(
        f"Fetching {len(refs.entries)} {refs.document_type.value} documents "
        f"for year {refs.year}"
    )

    pages: list[DocumentPage] = []
    failures: list[DocumentFetchFailure] = []

    for index, entry in enumerate(refs.entries, start=1):
        xml_url = document_xml_url(entry.url)
        api_url = document_api_url(entry.url)
        context.log.info(
            f"Fetching {refs.document_type.value} document {index}/"
            f"{len(refs.entries)}: {entry.year}/{entry.id}"
        )

        response = retsinformation_http.get(xml_url)

        if response.status_code == 404:
            context.log.warning(
                f"Primary XML endpoint returned 404 for {refs.document_type.value} "
                f"{entry.year}/{entry.id}; trying API fallback: {api_url}"
            )

            response = retsinformation_http.post_json(
                api_url,
                json={"isRawHtml": False},
            )

            if response.status_code == 404:
                failures.append(
                    DocumentFetchFailure(
                        entry=entry,
                        source_url=api_url,
                        source=DocumentContentSource.API_DOCUMENT,
                        status_code=response.status_code,
                        reason=getattr(response, "reason_phrase", "not found"),
                    )
                )
                context.log.warning(
                    f"Skipping dead {refs.document_type.value} link "
                    f"{entry.year}/{entry.id}: "
                    f"API fallback returned 404"
                )

                continue

            response.raise_for_status()

            if not api_document_response_has_document(response.text):
                failures.append(
                    DocumentFetchFailure(
                        entry=entry,
                        source_url=api_url,
                        source=DocumentContentSource.API_DOCUMENT,
                        status_code=response.status_code,
                        reason="API fallback returned no document",
                    )
                )
                context.log.warning(
                    f"Skipping dead {refs.document_type.value} link "
                    f"{entry.year}/{entry.id}: "
                    f"API fallback returned no document"
                )

                continue

            pages.append(
                DocumentPage(
                    entry=entry,
                    source_url=api_url,
                    source=DocumentContentSource.API_DOCUMENT,
                    status_code=response.status_code,
                    content_type=response.headers.get("content-type", ""),
                    body=response.text,
                    bytes_downloaded=len(response.content),
                )
            )

            continue

        response.raise_for_status()

        pages.append(
            DocumentPage(
                entry=entry,
                source_url=xml_url,
                source=DocumentContentSource.XML_ENDPOINT,
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                body=response.text,
                bytes_downloaded=len(response.content),
            )
        )

    source_counts = Counter(page.source.value for page in pages)

    metadata = {
        "document_type": refs.document_type.value,
        "year": refs.year,
        "available_ref_count": len(refs.entries),
        "fetched_count": len(pages),
        "source_counts": dict(sorted(source_counts.items())),
        "failed_count": len(failures),
        "not_found_count": sum(
            1 for failure in failures if failure.status_code == 404
        ),
        "bytes_downloaded": sum(page.bytes_downloaded for page in pages),
    }

    if refs.entries:
        metadata["first_xml_url"] = MetadataValue.url(
            document_xml_url(refs.entries[0].url)
        )
        metadata["first_api_url"] = MetadataValue.url(
            document_api_url(refs.entries[0].url)
        )

    context.add_output_metadata(metadata)

    return DocumentPageBatch(
        document_type=refs.document_type,
        year=refs.year,
        pages=pages,
        failures=failures,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def fc_document_refs(
    context: AssetExecutionContext,
    retsinfo_sitemap_page: list[SitemapEntry],
) -> DocumentRefSet:
    return _build_document_refs(
        context=context,
        entries=retsinfo_sitemap_page,
        document_type=DocumentType.FC,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def ilt_document_refs(
    context: AssetExecutionContext,
    retsinfo_sitemap_page: list[SitemapEntry],
) -> DocumentRefSet:
    return _build_document_refs(
        context=context,
        entries=retsinfo_sitemap_page,
        document_type=DocumentType.ILT,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def retsinfo_document_refs(
    context: AssetExecutionContext,
    retsinfo_sitemap_page: list[SitemapEntry],
) -> DocumentRefSet:
    return _build_document_refs(
        context=context,
        entries=retsinfo_sitemap_page,
        document_type=DocumentType.RETSINFO,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def fc_document_pages(
    context: AssetExecutionContext,
    fc_document_refs: DocumentRefSet,
    retsinformation_http: RetsinformationHttpResource,
) -> DocumentPageBatch:
    return _fetch_document_pages(
        context=context,
        refs=fc_document_refs,
        retsinformation_http=retsinformation_http,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def ilt_document_pages(
    context: AssetExecutionContext,
    ilt_document_refs: DocumentRefSet,
    retsinformation_http: RetsinformationHttpResource,
) -> DocumentPageBatch:
    return _fetch_document_pages(
        context=context,
        refs=ilt_document_refs,
        retsinformation_http=retsinformation_http,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def retsinfo_document_pages(
    context: AssetExecutionContext,
    retsinfo_document_refs: DocumentRefSet,
    retsinformation_http: RetsinformationHttpResource,
) -> DocumentPageBatch:
    return _fetch_document_pages(
        context=context,
        refs=retsinfo_document_refs,
        retsinformation_http=retsinformation_http,
    )
