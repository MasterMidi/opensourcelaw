import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Iterator

from dagster import (
    AllPartitionMapping,
    AssetDep,
    AssetExecutionContext,
    MaterializeResult,
    asset,
)

from src.assets.retsinformation.documents_parsed import retsinfo_parsed_documents
from src.assets.retsinformation.documents_raw import REPO_ROOT


DOCUMENT_COLUMNS = [
    "payload_id",
    "file_name",
    "eli_uri",
    "source_url",
    "xml_url",
    "accession_number",
    "document_type",
    "title",
    "short_title",
    "popular_title",
    "year",
    "number",
    "pub_media",
    "document_id",
    "unique_document_id",
    "administrative_authority",
    "ressort",
    "announced_in",
    "signed",
    "effective",
    "end_date",
    "published",
    "status",
    "parser_specialist_key",
    "parser_specialist_family",
    "parser_chunking_profile",
    "text_sha256",
]

TEXT_COLUMNS = ["payload_id", "eli_uri", "extracted_text", "text_sha256"]

UNIT_COLUMNS = [
    "payload_id",
    "unit_id",
    "parent_unit_id",
    "provision_type",
    "unit_type",
    "label",
    "number",
    "heading",
    "text",
    "source_path",
    "canonical_path",
    "eli_fragment",
    "sort_order",
    "depth",
    "specialist_key",
    "source_position_id",
]

SOURCE_POSITION_COLUMNS = [
    "position_id",
    "payload_id",
    "source_anchor",
    "char_start",
    "char_end",
    "section_path",
    "unit_id",
]

SOURCE_REFERENCE_COLUMNS = [
    "payload_id",
    "raw_text",
    "normalized_text",
    "reference_kind",
    "target_identifier_scheme",
    "target_identifier_value",
    "target_unit_anchor",
    "confidence",
]

FAILURE_COLUMNS = ["file_name", "xml_path", "error_message"]


def build_retsinfo_sqlite(parsed_root: Path, db_path: Path) -> dict[str, Any]:
    tmp_path = db_path.with_suffix(db_path.suffix + ".tmp")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.unlink(missing_ok=True)

    counts = {
        "documents": 0,
        "parse_stats": 0,
        "eli_metadata": 0,
        "texts": 0,
        "units": 0,
        "source_positions": 0,
        "source_references": 0,
        "failures": 0,
    }
    partition_dirs = sorted({path.parent for path in parsed_root.rglob("documents.jsonl")})

    with sqlite3.connect(tmp_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        _create_schema(conn)

        for path in _files(partition_dirs, "documents.jsonl"):
            for row in _read_jsonl(path):
                _insert(conn, "documents", DOCUMENT_COLUMNS, row)
                counts["documents"] += 1

                stats = row.get("stats")
                if isinstance(stats, dict):
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO parse_stats (
                            payload_id,
                            total_units,
                            text_units,
                            metadata_only_xml,
                            by_type_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            row.get("payload_id"),
                            stats.get("total_units"),
                            stats.get("text_units"),
                            stats.get("metadata_only_xml"),
                            _json(stats.get("by_type")),
                        ),
                    )
                    counts["parse_stats"] += 1

                if row.get("eli_metadata") is not None:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO eli_metadata (payload_id, raw_json)
                        VALUES (?, ?)
                        """,
                        (row.get("payload_id"), _json(row.get("eli_metadata"))),
                    )
                    counts["eli_metadata"] += 1

        counts["texts"] = _insert_jsonl_files(
            conn, "texts", TEXT_COLUMNS, _files(partition_dirs, "texts.jsonl")
        )
        counts["units"] = _insert_jsonl_files(
            conn, "units", UNIT_COLUMNS, _files(partition_dirs, "units.jsonl")
        )
        counts["source_positions"] = _insert_jsonl_files(
            conn,
            "source_positions",
            SOURCE_POSITION_COLUMNS,
            _files(partition_dirs, "source_positions.jsonl"),
        )
        counts["source_references"] = _insert_jsonl_files(
            conn,
            "source_references",
            SOURCE_REFERENCE_COLUMNS,
            _files(partition_dirs, "references.jsonl"),
        )
        counts["failures"] = _insert_jsonl_files(
            conn, "failures", FAILURE_COLUMNS, _files(partition_dirs, "failures.jsonl")
        )

    tmp_path.replace(db_path)
    return {"sqlite_path": str(db_path), "partition_count": len(partition_dirs), **counts}


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE documents (
            payload_id TEXT PRIMARY KEY NOT NULL,
            file_name TEXT,
            eli_uri TEXT,
            source_url TEXT,
            xml_url TEXT,
            accession_number TEXT,
            document_type TEXT,
            title TEXT,
            short_title TEXT,
            popular_title TEXT,
            year INTEGER,
            number INTEGER,
            pub_media TEXT,
            document_id TEXT,
            unique_document_id TEXT,
            administrative_authority TEXT,
            ressort TEXT,
            announced_in TEXT,
            signed TEXT,
            effective TEXT,
            end_date TEXT,
            published TEXT,
            status TEXT,
            parser_specialist_key TEXT,
            parser_specialist_family TEXT,
            parser_chunking_profile TEXT,
            text_sha256 TEXT
        );

        CREATE TABLE parse_stats (
            payload_id TEXT PRIMARY KEY NOT NULL REFERENCES documents(payload_id),
            total_units INTEGER,
            text_units INTEGER,
            metadata_only_xml INTEGER,
            by_type_json TEXT
        );

        CREATE TABLE eli_metadata (
            payload_id TEXT PRIMARY KEY NOT NULL REFERENCES documents(payload_id),
            raw_json TEXT NOT NULL
        );

        CREATE TABLE texts (
            payload_id TEXT PRIMARY KEY NOT NULL REFERENCES documents(payload_id),
            eli_uri TEXT,
            extracted_text TEXT,
            text_sha256 TEXT
        );

        CREATE TABLE units (
            payload_id TEXT NOT NULL REFERENCES documents(payload_id),
            unit_id TEXT NOT NULL,
            parent_unit_id TEXT,
            provision_type TEXT,
            unit_type TEXT,
            label TEXT,
            number TEXT,
            heading TEXT,
            text TEXT,
            source_path TEXT,
            canonical_path TEXT,
            eli_fragment TEXT,
            sort_order INTEGER,
            depth INTEGER,
            specialist_key TEXT,
            source_position_id TEXT,
            PRIMARY KEY (payload_id, unit_id)
        );

        CREATE TABLE source_positions (
            position_id TEXT NOT NULL,
            payload_id TEXT NOT NULL REFERENCES documents(payload_id),
            source_anchor TEXT,
            char_start INTEGER,
            char_end INTEGER,
            section_path TEXT,
            unit_id TEXT,
            PRIMARY KEY (payload_id, position_id)
        );

        CREATE TABLE source_references (
            reference_id INTEGER PRIMARY KEY,
            payload_id TEXT NOT NULL REFERENCES documents(payload_id),
            raw_text TEXT,
            normalized_text TEXT,
            reference_kind TEXT,
            target_identifier_scheme TEXT,
            target_identifier_value TEXT,
            target_unit_anchor TEXT,
            confidence REAL
        );

        CREATE TABLE failures (
            failure_id INTEGER PRIMARY KEY,
            file_name TEXT,
            xml_path TEXT,
            error_message TEXT
        );

        CREATE INDEX documents_eli_uri_idx ON documents(eli_uri);
        CREATE INDEX documents_type_year_idx ON documents(pub_media, year);
        CREATE INDEX units_parent_idx ON units(payload_id, parent_unit_id);
        CREATE INDEX units_type_idx ON units(provision_type, unit_type);
        CREATE INDEX source_positions_unit_idx ON source_positions(payload_id, unit_id);
        CREATE INDEX source_references_target_idx ON source_references(
            target_identifier_scheme,
            target_identifier_value
        );
        """
    )


def _insert_jsonl_files(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    paths: list[Path],
) -> int:
    count = 0
    for path in paths:
        for row in _read_jsonl(path):
            _insert(conn, table, columns, row)
            count += 1
    return count


def _insert(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    row: dict[str, Any],
) -> None:
    placeholders = ", ".join("?" for _ in columns)
    column_names = ", ".join(columns)
    conn.execute(
        f"INSERT OR REPLACE INTO {table} ({column_names}) VALUES ({placeholders})",
        [row.get(column) for column in columns],
    )


def _files(partition_dirs: list[Path], name: str) -> list[Path]:
    return [
        path for path in (directory / name for directory in partition_dirs) if path.exists()
    ]


def _read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as file:
        for line in file:
            if line.strip():
                yield json.loads(line)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


@asset(
    group_name="retsinformation",
    deps=[AssetDep(retsinfo_parsed_documents, partition_mapping=AllPartitionMapping())],
)
def retsinfo_documents_sqlite(context: AssetExecutionContext) -> MaterializeResult:
    ingest_root = Path(
        os.environ.get("OPENSOURCELAW_INGEST_ROOT", REPO_ROOT / "data" / "ingest")
    )
    parsed_root = ingest_root / "parsed" / "retsinformation_documents"
    db_path = ingest_root / "parsed" / "retsinformation_documents.sqlite"
    metadata = build_retsinfo_sqlite(parsed_root, db_path)

    context.log.info(
        f"Built {metadata['documents']} parsed Retsinformation documents into {db_path}"
    )
    return MaterializeResult(metadata=metadata)
