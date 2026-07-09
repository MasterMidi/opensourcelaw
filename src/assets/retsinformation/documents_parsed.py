import os
import tempfile
from pathlib import Path
from typing import Any, Protocol, cast

from dagster import AssetExecutionContext, MaterializeResult, asset

from src.assets.retsinformation.documents_raw import (
    REPO_ROOT,
    document_partitions,
    retsinfo_documents,
)
from src.assets.retsinformation.sitemap_pages import PubMedia
from src.resources import DotnetScriptResource, S3ObjectStoreResource

RETSINFO_XML_PARSER_TOOL = REPO_ROOT / "tools" / "retsinformation_xml_parser.cs"


class RawObjectReader(Protocol):
    def get_json(self, key: str) -> object: ...

    def get_object(self, key: str) -> bytes: ...


def _download_latest_raw_documents(
    document_type: PubMedia,
    year: str,
    raw_object_store: RawObjectReader,
    target_dir: Path,
) -> dict[str, Any]:
    latest_key = f"raw/retsinformation_documents/{document_type.value}/{year}/latest.json"
    latest = raw_object_store.get_json(latest_key)

    if not isinstance(latest, dict) or not isinstance(latest.get("objects"), list):
        raise ValueError(f"raw S3 pointer is invalid: {latest_key}")

    for item in latest["objects"]:
        if not isinstance(item, dict):
            raise ValueError(f"raw S3 pointer object entry is invalid: {latest_key}")

        key = item.get("key")
        relative_path = item.get("path")
        if not isinstance(key, str) or not isinstance(relative_path, str):
            raise ValueError(f"raw S3 pointer object entry is invalid: {latest_key}")

        path = target_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw_object_store.get_object(key))

    return latest


@asset(
    group_name="retsinformation",
    partitions_def=document_partitions,
    deps=[retsinfo_documents],
    pool="retsinformation_dotnet",
)
def retsinfo_parsed_documents(
    context: AssetExecutionContext,
    dotnet_script: DotnetScriptResource,
    raw_object_store: S3ObjectStoreResource,
) -> MaterializeResult:
    keys = cast(Any, context.partition_key).keys_by_dimension
    document_type = PubMedia(keys["document_type"])
    year = keys["year"]
    ingest_root = Path(
        os.environ.get("OPENSOURCELAW_INGEST_ROOT", REPO_ROOT / "data" / "ingest")
    )
    output_dir = ingest_root / "parsed" / "retsinformation_documents" / document_type / year

    context.log.info(
        f"Parsing {document_type.value}/{year} XML documents with dotnet: "
        f"{RETSINFO_XML_PARSER_TOOL}"
    )

    with tempfile.TemporaryDirectory(prefix="opensourcelaw-retsinfo-raw-") as temp_dir:
        input_dir = Path(temp_dir) / document_type.value / year
        latest = _download_latest_raw_documents(
            document_type,
            year,
            raw_object_store,
            input_dir,
        )
        result = cast(
            dict[str, Any],
            dotnet_script.run_json(
                context,
                RETSINFO_XML_PARSER_TOOL,
                {
                    "documentType": document_type.value,
                    "year": year,
                    "inputDir": str(input_dir.resolve()),
                    "outputDir": str(output_dir.resolve()),
                },
            ),
        )

    return MaterializeResult(
        metadata={key: value for key, value in result.items() if value is not None}
        | {"raw_data_version": latest.get("data_version")}
    )
