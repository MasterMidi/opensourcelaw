import json
import sqlite3

from src.assets.retsinformation.documents_sqlite import build_retsinfo_sqlite


def test_build_retsinfo_sqlite_loads_parser_jsonl(tmp_path):
    partition = tmp_path / "parsed" / "lta" / "2024"
    partition.mkdir(parents=True)

    _write_jsonl(
        partition / "documents.jsonl",
        [
            {
                "payload_id": "doc-1",
                "file_name": "doc.xml",
                "eli_uri": "/eli/lta/2024/1",
                "title": "Test law",
                "pub_media": "lta",
                "year": 2024,
                "stats": {
                    "total_units": 1,
                    "text_units": 1,
                    "metadata_only_xml": False,
                    "by_type": {"dokument": 1},
                },
                "eli_metadata": {"@id": "/eli/lta/2024/1"},
            }
        ],
    )
    _write_jsonl(
        partition / "texts.jsonl",
        [
            {
                "payload_id": "doc-1",
                "eli_uri": "/eli/lta/2024/1",
                "extracted_text": "Hello",
                "text_sha256": "abc",
            }
        ],
    )
    _write_jsonl(
        partition / "units.jsonl",
        [
            {
                "payload_id": "doc-1",
                "unit_id": "dokument",
                "provision_type": "dokument",
                "unit_type": "act",
                "text": "Hello",
            }
        ],
    )
    _write_jsonl(
        partition / "source_positions.jsonl",
        [
            {
                "position_id": "position-dokument",
                "payload_id": "doc-1",
                "unit_id": "dokument",
                "char_start": 0,
                "char_end": 5,
            }
        ],
    )
    _write_jsonl(
        partition / "references.jsonl",
        [
            {
                "payload_id": "doc-1",
                "raw_text": "section 1",
                "reference_kind": "legal_citation",
                "confidence": 0.75,
            }
        ],
    )
    _write_jsonl(partition / "failures.jsonl", [])

    db_path = tmp_path / "retsinformation.sqlite"
    metadata = build_retsinfo_sqlite(tmp_path / "parsed", db_path)

    assert metadata["documents"] == 1
    assert metadata["units"] == 1
    assert db_path.exists()

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT title FROM documents").fetchone() == ("Test law",)
        assert conn.execute("SELECT total_units FROM parse_stats").fetchone() == (1,)
        assert conn.execute("SELECT extracted_text FROM texts").fetchone() == ("Hello",)
        assert conn.execute("SELECT raw_text FROM source_references").fetchone() == (
            "section 1",
        )


def _write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
