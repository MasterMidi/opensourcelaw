import hashlib
import mimetypes
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Protocol, cast

from dagster import (
    AssetExecutionContext,
    DataVersion,
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


class RawObjectStore(Protocol):
    def resolved_bucket(self) -> str: ...

    def ensure_bucket(self) -> None: ...

    def put_file(self, key: str, path: Path, content_type: str) -> None: ...

    def put_json(self, key: str, value: object) -> None: ...


def _entry_payload(entry: SitemapEntry) -> dict[str, str]:
    return {
        "url": entry.url,
        "lastmod": entry.lastmod,
        "id": entry.id,
        "year": entry.year,
        "type": entry.type.value,
    }


def _raw_document_data_version(output_dir: Path) -> str:
    hasher = hashlib.sha256()

    for path in _raw_document_payload_paths(output_dir):
        hasher.update(path.relative_to(output_dir).as_posix().encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(hashlib.sha256(path.read_bytes()).hexdigest().encode("ascii"))
        hasher.update(b"\0")

    return hasher.hexdigest()


def _raw_document_payload_paths(output_dir: Path) -> list[Path]:
    paths: list[Path] = []

    for directory in [output_dir / "xml", output_dir / "jsonld"]:
        if directory.exists():
            paths.extend(path for path in directory.rglob("*") if path.is_file())

    failures_path = output_dir / "failures.jsonl"
    if failures_path.exists():
        paths.append(failures_path)

    return sorted(paths, key=lambda path: path.relative_to(output_dir).as_posix())


def _upload_raw_documents(
    output_dir: Path,
    document_type: PubMedia,
    year: str,
    data_version: str,
    raw_object_store: RawObjectStore,
) -> dict[str, Any]:
    bucket = raw_object_store.resolved_bucket()
    prefix = f"raw/retsinformation_documents/{document_type.value}/{year}/{data_version}"
    latest_key = f"raw/retsinformation_documents/{document_type.value}/{year}/latest.json"
    uploaded_count = 0
    objects: list[dict[str, str]] = []

    raw_object_store.ensure_bucket()

    for path in sorted(output_dir.rglob("*")):
        if not path.is_file():
            continue

        relative_path = path.relative_to(output_dir).as_posix()
        key = f"{prefix}/{relative_path}"
        raw_object_store.put_file(key, path, _content_type(path))
        objects.append({"key": key, "path": relative_path})
        uploaded_count += 1

    raw_object_store.put_json(
        latest_key,
        {
            "bucket": bucket,
            "prefix": prefix,
            "data_version": data_version,
            "manifest_key": f"{prefix}/manifest.json",
            "objects": objects,
        },
    )

    return {
        "raw_bucket": bucket,
        "raw_prefix": prefix,
        "raw_latest_key": latest_key,
        "raw_uploaded_object_count": uploaded_count,
    }


def _content_type(path: Path) -> str:
    if path.suffix == ".jsonl":
        return "application/x-ndjson"

    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def _stable_result_metadata(result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in result.items()
        if value is not None
        and key not in {"outputDir"}
        and not key.endswith("Path")
        and not key.endswith("DirectoryPath")
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
    raw_object_store: S3ObjectStoreResource,
) -> MaterializeResult:
    keys = cast(Any, context.partition_key).keys_by_dimension
    document_type = PubMedia(keys["document_type"])
    year = keys["year"]
    context.log.info(
        f"Downloading {document_type.value}/{year} XML documents with dotnet: "
        f"{RETSINFO_DOWNLOADER_TOOL}"
    )

    with tempfile.TemporaryDirectory(prefix="opensourcelaw-retsinfo-raw-") as temp_dir:
        output_dir = Path(temp_dir) / document_type.value / year
        result = cast(
            dict[str, Any],
            dotnet_script.run_json(
                context,
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
            ),
        )
        raw_data_version = _raw_document_data_version(output_dir)
        storage_metadata = _upload_raw_documents(
            output_dir,
            document_type,
            year,
            raw_data_version,
            raw_object_store,
        )

    return MaterializeResult(
        data_version=DataVersion(raw_data_version),
        metadata=_stable_result_metadata(result)
        | {"raw_data_version": raw_data_version}
        | storage_metadata,
    )
