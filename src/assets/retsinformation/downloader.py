from dagster import AssetExecutionContext, MetadataValue, asset

from src.assets.retsinformation.documents import DocumentRefSet
from src.assets.retsinformation.pages import SitemapEntry


@asset(group_name="retsinformation2")
def retsinfo_downloader(
    context: AssetExecutionContext, retsinfo_sitemap_page: list[SitemapEntry]
) -> list[DocumentRefSet]:
    year = context.partition_key
    refs = [
        entry for entry in retsinfo_sitemap_page if entry.type == document_type and entry.year == year
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
