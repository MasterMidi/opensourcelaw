import os
from datetime import date
from pathlib import Path
from typing import Any, cast

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)

from src.assets.retsinformation.sitemap_pages import (
    RETSINFO_PAGE_REQUEST_TIMEOUT_SECONDS,
    RETSINFO_USER_AGENT,
    PubMedia,
    SitemapEntry,
)
from src.resources import DotnetScriptResource

REPO_ROOT = Path(__file__).resolve().parents[3]
RETSINFO_DOWNLOADER_TOOL = REPO_ROOT / "tools" / "retsinformation_downloader.cs"
document_partitions = MultiPartitionsDefinition(
    {
        "document_type": StaticPartitionsDefinition(
            [document_type.value for document_type in PubMedia]
        ),
        "year": StaticPartitionsDefinition(
            [str(year) for year in range(1985, date.today().year + 1)]
        ),
    }
)


def _entry_payload(entry: SitemapEntry) -> dict[str, str]:
    return {
        "url": entry.url,
        "lastmod": entry.lastmod,
        "id": entry.id,
        "year": entry.year,
        "type": entry.type.value,
    }


@asset(
    group_name="retsinformation",
    partitions_def=document_partitions,
    pool="retsinformation_dotnet",
)
def retsinfo_documents(
    context: AssetExecutionContext,
    retsinfo_sitemap_pages: list[SitemapEntry],
    dotnet_script: DotnetScriptResource,
) -> MaterializeResult:
    keys = cast(Any, context.partition_key).keys_by_dimension
    document_type = PubMedia(keys["document_type"])
    year = keys["year"]
    ingest_root = Path(
        os.environ.get("OPENSOURCELAW_INGEST_ROOT", REPO_ROOT / "data" / "ingest")
    )
    # ponytail: local files are enough until this runs across machines.
    output_dir = (
        ingest_root / "raw" / "retsinformation_documents" / document_type / year
    )

    context.log.info(
        f"Downloading {document_type.value}/{year} XML documents with dotnet: "
        f"{RETSINFO_DOWNLOADER_TOOL}"
    )

    result = cast(
        dict[str, Any],
        dotnet_script.run_json(
            RETSINFO_DOWNLOADER_TOOL,
            {
                "documentType": document_type.value,
                "year": year,
                "outputDir": str(output_dir.resolve()),
                "userAgent": RETSINFO_USER_AGENT,
                "timeoutSeconds": RETSINFO_PAGE_REQUEST_TIMEOUT_SECONDS,
                "retsinfoSitemapPage": [
                    _entry_payload(entry) for entry in retsinfo_sitemap_pages
                ],
            },
            context.log,
        ),
    )

    return MaterializeResult(
        metadata={key: value for key, value in result.items() if value is not None}
    )
