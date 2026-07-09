from datetime import date
from pathlib import Path
from typing import Any, cast

from dagster import (
    AssetExecutionContext,
    DataVersion,
    MaterializeResult,
    MultiPartitionsDefinition,
    RetryPolicy,
    StaticPartitionsDefinition,
    asset,
)

from src.assets.retsinformation.sitemap_pages import (
    RETSINFO_PAGE_REQUEST_TIMEOUT_SECONDS,
    RETSINFO_USER_AGENT,
    PubMedia,
    SitemapEntry,
)
from src.resources import DotnetScriptResource, S3ObjectStoreResource

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


def _stable_result_metadata(result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in result.items()
        if value is not None
        and key not in {"objects", "outputDir"}
        and not key.endswith("Path")
        and not key.endswith("DirectoryPath")
    }


@asset(
    group_name="retsinformation",
    partitions_def=document_partitions,
    pool="retsinformation_dotnet",
    retry_policy=RetryPolicy(max_retries=2),
)
def retsinfo_documents(
    context: AssetExecutionContext,
    retsinfo_sitemap_pages: list[SitemapEntry],
    dotnet_script: DotnetScriptResource,
    raw_object_store: S3ObjectStoreResource,
) -> MaterializeResult:
    keys = cast(Any, context.partition_key).keys_by_dimension
    document_type = PubMedia(keys["document_type"])
    year = keys["year"]
    context.log.info(
        f"Downloading {document_type.value}/{year} XML documents with dotnet: "
        f"{RETSINFO_DOWNLOADER_TOOL}"
    )
    bucket = raw_object_store.resolved_bucket()
    prefix = f"raw/retsinformation_documents/{document_type.value}/{year}"

    raw_object_store.ensure_bucket()
    result = cast(
        dict[str, Any],
        dotnet_script.run_json(
            context,
            RETSINFO_DOWNLOADER_TOOL,
            {
                "documentType": document_type.value,
                "year": year,
                "userAgent": RETSINFO_USER_AGENT,
                "timeoutSeconds": RETSINFO_PAGE_REQUEST_TIMEOUT_SECONDS,
                "retsinfoSitemapPage": [
                    _entry_payload(entry) for entry in retsinfo_sitemap_pages
                ],
                "s3": {
                    "bucket": bucket,
                    "endpointUrl": raw_object_store.endpoint_url,
                    "region": raw_object_store.region,
                    "accessKeyId": raw_object_store.access_key_id,
                    "secretAccessKey": raw_object_store.secret_access_key,
                    "prefix": prefix,
                    "maxAttempts": raw_object_store.max_attempts,
                },
            },
        ),
    )
    raw_data_version = result.get("dataVersion")
    raw_prefix = result.get("rawPrefix")
    raw_latest_key = result.get("rawLatestKey")
    raw_manifest_key = result.get("rawManifestKey")
    objects = result.get("objects")

    if not isinstance(raw_data_version, str):
        raise ValueError("dotnet downloader output field 'dataVersion' must be a string")
    if not isinstance(raw_prefix, str):
        raise ValueError("dotnet downloader output field 'rawPrefix' must be a string")
    if not isinstance(raw_latest_key, str):
        raise ValueError("dotnet downloader output field 'rawLatestKey' must be a string")
    if not isinstance(raw_manifest_key, str):
        raise ValueError("dotnet downloader output field 'rawManifestKey' must be a string")
    if not isinstance(objects, list):
        raise ValueError("dotnet downloader output field 'objects' must be a list")

    raw_object_store.put_json(
        raw_latest_key,
        {
            "bucket": bucket,
            "prefix": raw_prefix,
            "data_version": raw_data_version,
            "manifest_key": raw_manifest_key,
            "objects": objects,
        },
    )
    storage_metadata = {
        "raw_bucket": bucket,
        "raw_prefix": raw_prefix,
        "raw_latest_key": raw_latest_key,
        "raw_manifest_key": raw_manifest_key,
        "raw_uploaded_object_count": len(objects),
    }

    return MaterializeResult(
        data_version=DataVersion(raw_data_version),
        metadata=_stable_result_metadata(result)
        | {"raw_data_version": raw_data_version}
        | storage_metadata,
    )
