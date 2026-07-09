import os
from pathlib import Path
from typing import Any, cast

from dagster import AssetExecutionContext, MaterializeResult, asset

from src.assets.retsinformation.documents_raw import (
    REPO_ROOT,
    document_partitions,
    retsinfo_documents,
)
from src.assets.retsinformation.sitemap_pages import PubMedia
from src.resources import DotnetScriptResource

RETSINFO_XML_PARSER_TOOL = REPO_ROOT / "tools" / "retsinformation_xml_parser.cs"


@asset(
    group_name="retsinformation",
    partitions_def=document_partitions,
    deps=[retsinfo_documents],
    pool="retsinformation_dotnet",
)
def retsinfo_parsed_documents(
    context: AssetExecutionContext,
    dotnet_script: DotnetScriptResource,
) -> MaterializeResult:
    keys = cast(Any, context.partition_key).keys_by_dimension
    document_type = PubMedia(keys["document_type"])
    year = keys["year"]
    ingest_root = Path(
        os.environ.get("OPENSOURCELAW_INGEST_ROOT", REPO_ROOT / "data" / "ingest")
    )
    input_dir = ingest_root / "raw" / "retsinformation_documents" / document_type / year
    output_dir = ingest_root / "parsed" / "retsinformation_documents" / document_type / year

    context.log.info(
        f"Parsing {document_type.value}/{year} XML documents with dotnet: "
        f"{RETSINFO_XML_PARSER_TOOL}"
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
    )
