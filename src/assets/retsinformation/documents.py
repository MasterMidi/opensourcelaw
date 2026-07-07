from dataclasses import dataclass
from time import sleep

from dagster import (
    AssetExecutionContext,
    AssetIn,
    Config,
    IdentityPartitionMapping,
    MetadataValue,
    asset,
)

from src.assets.retsinformation.pages import (
    DocumentType,
    SitemapEntry,
    retsinfo_sitemap_page_partitions,
)
from src.resources import RetsinformationHttpResource


@dataclass(frozen=True)
class DocumentPage:
    entry: SitemapEntry
    status_code: int
    content_type: str
    html: str
    bytes_downloaded: int


class DocumentFetchConfig(Config):
    max_documents: int | None = 25
    request_delay_seconds: float = 0.0


def _fetch_document_pages(
    *,
    context: AssetExecutionContext,
    entries: list[SitemapEntry],
    document_type: DocumentType,
    retsinformation_http: RetsinformationHttpResource,
    config: DocumentFetchConfig,
) -> list[DocumentPage]:
    matching_entries = [entry for entry in entries if entry.type == document_type]
    selected_entries = (
        matching_entries[: config.max_documents]
        if config.max_documents is not None
        else matching_entries
    )

    context.log.info(
        f"Fetching {len(selected_entries)} of {len(matching_entries)} {document_type} "
        f"documents from sitemap partition {context.partition_key}"
    )

    pages: list[DocumentPage] = []

    for index, entry in enumerate(selected_entries, start=1):
        context.log.info(
            f"Fetching {document_type} document {index}/{len(selected_entries)}: "
            f"{entry.year}/{entry.id}"
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
        "document_type": document_type,
        "available_entry_count": len(matching_entries),
        "fetched_count": len(pages),
        "bytes_downloaded": sum(page.bytes_downloaded for page in pages),
        "max_documents": config.max_documents
        if config.max_documents is not None
        else "all",
    }

    if selected_entries:
        metadata["first_url"] = MetadataValue.url(selected_entries[0].url)

    context.add_output_metadata(metadata)

    return pages


@asset(
    group_name="retsinformation",
    partitions_def=retsinfo_sitemap_page_partitions,
    ins={
        "retsinfo_sitemap_page": AssetIn(partition_mapping=IdentityPartitionMapping())
    },
)
def fc_document_pages(
    context: AssetExecutionContext,
    config: DocumentFetchConfig,
    retsinfo_sitemap_page: list[SitemapEntry],
    retsinformation_http: RetsinformationHttpResource,
) -> list[DocumentPage]:
    return _fetch_document_pages(
        context=context,
        entries=retsinfo_sitemap_page,
        document_type=DocumentType.FC,
        retsinformation_http=retsinformation_http,
        config=config,
    )


@asset(
    group_name="retsinformation",
    partitions_def=retsinfo_sitemap_page_partitions,
    ins={
        "retsinfo_sitemap_page": AssetIn(partition_mapping=IdentityPartitionMapping())
    },
)
def ilt_document_pages(
    context: AssetExecutionContext,
    config: DocumentFetchConfig,
    retsinfo_sitemap_page: list[SitemapEntry],
    retsinformation_http: RetsinformationHttpResource,
) -> list[DocumentPage]:
    return _fetch_document_pages(
        context=context,
        entries=retsinfo_sitemap_page,
        document_type=DocumentType.ILT,
        retsinformation_http=retsinformation_http,
        config=config,
    )


@asset(
    group_name="retsinformation",
    partitions_def=retsinfo_sitemap_page_partitions,
    ins={
        "retsinfo_sitemap_page": AssetIn(partition_mapping=IdentityPartitionMapping())
    },
)
def retsinfo_document_pages(
    context: AssetExecutionContext,
    config: DocumentFetchConfig,
    retsinfo_sitemap_page: list[SitemapEntry],
    retsinformation_http: RetsinformationHttpResource,
) -> list[DocumentPage]:
    return _fetch_document_pages(
        context=context,
        entries=retsinfo_sitemap_page,
        document_type=DocumentType.RETSINFO,
        retsinformation_http=retsinformation_http,
        config=config,
    )
