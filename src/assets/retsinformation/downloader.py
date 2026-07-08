import shutil
from collections.abc import Mapping
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    PipesSubprocessClient,
    asset,
    file_relative_path,
)

from src.assets.retsinformation.documents import (
    DocumentRefSet,
    document_year_partitions,
)
from src.assets.retsinformation.pages import DocumentType, SitemapEntry
from src.resources import DotnetScriptResource

RETSINFO_DOWNLOADER_TOOL = (
    Path(__file__).resolve().parents[3] / "tools" / "retsinformation_downloader.cs"
)


def _required_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"dotnet downloader output field {field!r} must be an object")

    return value


def _required_str(value: Mapping[str, object], field: str) -> str:
    field_value = value.get(field)

    if not isinstance(field_value, str):
        raise ValueError(f"dotnet downloader output field {field!r} must be a string")

    return field_value


def _entry_payload(entry: SitemapEntry) -> dict[str, str]:
    return {
        "url": entry.url,
        "lastmod": entry.lastmod,
        "id": entry.id,
        "year": entry.year,
        "type": entry.type.value,
    }


def _load_entries(value: object) -> list[SitemapEntry]:
    if not isinstance(value, list):
        raise ValueError("dotnet downloader output field 'entries' must be a list")

    entries: list[SitemapEntry] = []

    for item in value:
        entry = _required_mapping(item, "entries[]")
        entries.append(
            SitemapEntry(
                url=_required_str(entry, "url"),
                lastmod=_required_str(entry, "lastmod"),
                id=_required_str(entry, "id"),
                year=_required_str(entry, "year"),
                type=DocumentType(_required_str(entry, "type")),
            )
        )

    return entries


def _load_document_ref_set(value: object) -> DocumentRefSet:
    result = _required_mapping(value, "root")

    return DocumentRefSet(
        document_type=DocumentType(_required_str(result, "documentType")),
        year=_required_str(result, "year"),
        entries=_load_entries(result.get("entries")),
    )


@asset(group_name="retsinformation2", partitions_def=document_year_partitions)
def retsinfo_downloader(
    context: AssetExecutionContext,
    retsinfo_sitemap_page: list[SitemapEntry],
    pipes_subprocess_client: PipesSubprocessClient,
) -> DocumentRefSet:
    year = context.partition_key
    document_type = DocumentType.RETSINFO

    cmd = [
        shutil.which("curl"),
        "-L",
        f"{retsinfo_sitemap_page[0].url}/xml",
    ]

    refs = []
    for x in retsinfo_sitemap_page:
        refs.append(pipes_subprocess_client.run(
            command=cmd,
            context=context,
        ).get_materialize_result())
    

    




    

    payload = {
        "documentType": document_type.value,
        "year": year,
        "retsinfoSitemapPage": [
            _entry_payload(entry) for entry in retsinfo_sitemap_page
        ],
    }

    context.log.info(
        f"Building {document_type.value} document refs for year {year} with dotnet: "
        f"{RETSINFO_DOWNLOADER_TOOL}"
    )

    metadata = {
        "document_type": refs.document_type.value,
        "year": refs.year,
        "ref_count": len(refs.entries),
    }

    if refs.entries:
        metadata["first_url"] = MetadataValue.url(refs.entries[0].url)

    context.add_output_metadata(metadata)

    return refs

