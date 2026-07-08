from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from dagster import AssetExecutionContext, asset

from src.assets.retsinformation.sitemap_index import SitemapPageRef
from src.resources import DotnetScriptResource

RETSINFO_SITEMAP_PAGES_TOOL = (
    Path(__file__).resolve().parents[3] / "tools" / "retsinformation_sitemap_pages.cs"
)
RETSINFO_USER_AGENT = "opensourcelaw-retsinformation-ingest/0.1"
RETSINFO_PAGE_REQUEST_TIMEOUT_SECONDS = 30.0


class PubMedia(StrEnum):
    FT = "ft"  # Folketingstidende as the official medium of publication of documents of the Danish Parliament. These documents are also published in a special section in Retsinformation.
    FOB = "fob"  # A special section in Retsinformation where the Danish Parliamentary Ombudsman publishes his views and decisions.
    LTA = "lta"  # Lovtidende A as the official medium of publication of promulgated documents.
    LTB = "ltb"  # Lovtidende B as the official medium of publication of promulgated documents.
    LTC = "ltc"  # Lovtidende C as the official medium of publication of promulgated documents.
    MT = "mt"  # Ministerialtidende as the official medium of publication of the document. Since the date 01-01-2013 documents are no longer published in Ministerialtidende.
    RETSINFO = "retsinfo"  # Retsinformation as the official medium of publication of non-promulgated documents, e.g. documents classified in Retsinformation as decisions.


@dataclass(frozen=True)
class SitemapEntry:
    url: str
    lastmod: str
    id: str
    year: str
    type: PubMedia


def _required_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(
            f"dotnet sitemap page output field {field!r} must be an object"
        )

    return value


def _required_str(value: Mapping[str, object], field: str) -> str:
    field_value = value.get(field)

    if not isinstance(field_value, str):
        raise ValueError(f"dotnet sitemap page output field {field!r} must be a string")

    return field_value


def _required_int(value: Mapping[str, object], field: str) -> int:
    field_value = value.get(field)

    if isinstance(field_value, bool) or not isinstance(field_value, int):
        raise ValueError(f"dotnet sitemap page output field {field!r} must be an int")

    return field_value


def _required_float(value: Mapping[str, object], field: str) -> float:
    field_value = value.get(field)

    if isinstance(field_value, bool) or not isinstance(field_value, (int, float)):
        raise ValueError(f"dotnet sitemap page output field {field!r} must be numeric")

    return float(field_value)


def _required_str_int_dict(value: object, field: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ValueError(
            f"dotnet sitemap page output field {field!r} must be an object"
        )

    counts: dict[str, int] = {}

    for key, count in value.items():
        if (
            not isinstance(key, str)
            or isinstance(count, bool)
            or not isinstance(count, int)
        ):
            raise ValueError(
                f"dotnet sitemap page output field {field!r} must map strings to ints"
            )

        counts[key] = count

    return counts


def _load_entries(value: object) -> list[SitemapEntry]:
    if not isinstance(value, list):
        raise ValueError("dotnet sitemap page output field 'entries' must be a list")

    entries: list[SitemapEntry] = []

    for item in value:
        entry = _required_mapping(item, "entries[]")
        entries.append(
            SitemapEntry(
                url=_required_str(entry, "url"),
                lastmod=_required_str(entry, "lastmod"),
                id=_required_str(entry, "id"),
                year=_required_str(entry, "year"),
                type=PubMedia(_required_str(entry, "type")),
            )
        )

    return entries


def _log_page_results(
    context: AssetExecutionContext,
    page_results: object,
) -> None:
    if not isinstance(page_results, list):
        raise ValueError("dotnet sitemap page output field 'pages' must be a list")

    for item in page_results:
        page = _required_mapping(item, "pages[]")
        context.log.info(
            f"Loaded {_required_int(page, 'entryCount')} entries from sitemap page "
            f"{_required_str(page, 'page')} in "
            f"{_required_float(page, 'parseSeconds'):.2f}s after "
            f"{_required_float(page, 'fetchSeconds'):.2f}s fetch "
            f"({_required_int(page, 'totalEntryCount')} total, "
            f"{_required_int(page, 'skippedCount')} skipped on page)"
        )


@asset(group_name="retsinformation", pool="retsinformation_dotnet")
def retsinfo_sitemap_pages(
    context: AssetExecutionContext,
    retsinfo_sitemap_index: list[SitemapPageRef],
    dotnet_script: DotnetScriptResource,
) -> list[SitemapEntry]:
    page_refs = sorted(retsinfo_sitemap_index, key=lambda ref: int(ref.page))
    payload = {
        "userAgent": RETSINFO_USER_AGENT,
        "timeoutSeconds": RETSINFO_PAGE_REQUEST_TIMEOUT_SECONDS,
        "pages": [
            {
                "page": page_ref.page,
                "url": page_ref.url,
            }
            for page_ref in page_refs
        ],
    }

    context.log.info(
        f"Fetching and parsing {len(page_refs)} sitemap pages with dotnet: "
        f"{RETSINFO_SITEMAP_PAGES_TOOL}"
    )
    raw_result = dotnet_script.run_json(RETSINFO_SITEMAP_PAGES_TOOL, payload)
    result = _required_mapping(raw_result, "root")
    entries = _load_entries(result.get("entries"))
    entry_count = _required_int(result, "entryCount")

    if entry_count != len(entries):
        raise ValueError(
            "dotnet sitemap page output entryCount does not match entries length"
        )

    _log_page_results(context, result.get("pages"))

    context.add_output_metadata(
        {
            "sitemap_page_count": _required_int(result, "sitemapPageCount"),
            "entry_count": entry_count,
            "skipped_count": _required_int(result, "skippedCount"),
            "type_counts": _required_str_int_dict(
                result.get("typeCounts"), "typeCounts"
            ),
            "year_count": _required_int(result, "yearCount"),
            "fetch_seconds": round(_required_float(result, "fetchSeconds"), 3),
            "parse_seconds": round(_required_float(result, "parseSeconds"), 3),
        }
    )

    return entries
