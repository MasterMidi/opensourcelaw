from dataclasses import dataclass
from datetime import date
from time import sleep

from dagster import (
    AssetExecutionContext,
    Config,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from src.assets.retsinformation.pages import DocumentType, SitemapEntry
from src.resources import RetsinformationHttpResource


document_year_partitions = StaticPartitionsDefinition(
    [str(year) for year in range(1985, date.today().year + 1)]
)


@dataclass(frozen=True)
class DocumentRefSet:
    document_type: DocumentType
    year: str
    entries: list[SitemapEntry]


@dataclass(frozen=True)
class DocumentPage:
    entry: SitemapEntry
    status_code: int
    content_type: str
    html: str
    bytes_downloaded: int


@dataclass(frozen=True)
class DocumentPageBatch:
    document_type: DocumentType
    year: str
    pages: list[DocumentPage]


class DocumentFetchConfig(Config):
    max_documents: int | None = 25
    request_delay_seconds: float = 0.0


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
    config: DocumentFetchConfig,
) -> DocumentPageBatch:
    selected_entries = (
        refs.entries[: config.max_documents]
        if config.max_documents is not None
        else refs.entries
    )

    context.log.info(
        f"Fetching {len(selected_entries)} of {len(refs.entries)} "
        f"{refs.document_type.value} documents for year {refs.year}"
    )

    pages: list[DocumentPage] = []

    for index, entry in enumerate(selected_entries, start=1):
        context.log.info(
            f"Fetching {refs.document_type.value} document {index}/"
            f"{len(selected_entries)}: {entry.year}/{entry.id}"
        )

        response = retsinformation_http.get(entry.url)
        response.raise_for_status()

        pages.append(
            DocumentPage(
                entry=entry,
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                html=response.text,
                bytes_downloaded=len(response.content),
            )
        )

        if config.request_delay_seconds > 0 and index < len(selected_entries):
            sleep(config.request_delay_seconds)

    metadata = {
        "document_type": refs.document_type.value,
        "year": refs.year,
        "available_ref_count": len(refs.entries),
        "fetched_count": len(pages),
        "bytes_downloaded": sum(page.bytes_downloaded for page in pages),
        "max_documents": config.max_documents
        if config.max_documents is not None
        else "all",
    }

    if selected_entries:
        metadata["first_url"] = MetadataValue.url(selected_entries[0].url)

    context.add_output_metadata(metadata)

    return DocumentPageBatch(
        document_type=refs.document_type,
        year=refs.year,
        pages=pages,
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
    config: DocumentFetchConfig,
    fc_document_refs: DocumentRefSet,
    retsinformation_http: RetsinformationHttpResource,
) -> DocumentPageBatch:
    return _fetch_document_pages(
        context=context,
        refs=fc_document_refs,
        retsinformation_http=retsinformation_http,
        config=config,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def ilt_document_pages(
    context: AssetExecutionContext,
    config: DocumentFetchConfig,
    ilt_document_refs: DocumentRefSet,
    retsinformation_http: RetsinformationHttpResource,
) -> DocumentPageBatch:
    return _fetch_document_pages(
        context=context,
        refs=ilt_document_refs,
        retsinformation_http=retsinformation_http,
        config=config,
    )


@asset(group_name="retsinformation", partitions_def=document_year_partitions)
def retsinfo_document_pages(
    context: AssetExecutionContext,
    config: DocumentFetchConfig,
    retsinfo_document_refs: DocumentRefSet,
    retsinformation_http: RetsinformationHttpResource,
) -> DocumentPageBatch:
    return _fetch_document_pages(
        context=context,
        refs=retsinfo_document_refs,
        retsinformation_http=retsinformation_http,
        config=config,
    )
