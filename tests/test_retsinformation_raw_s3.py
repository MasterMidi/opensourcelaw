from src.assets.retsinformation.documents_raw import (
    _raw_document_data_version,
    _upload_raw_documents,
)
from src.assets.retsinformation.sitemap_pages import PubMedia


def test_raw_document_data_version_tracks_payload_not_temp_metadata(tmp_path):
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write_raw_output(left, metadata_fetched_at="2026-07-09T12:00:00Z")
    _write_raw_output(right, metadata_fetched_at="2026-07-09T13:00:00Z")

    assert _raw_document_data_version(left) == _raw_document_data_version(right)

    (right / "jsonld" / "000001_1.json").write_text('{"id":2}\n', encoding="utf-8")

    assert _raw_document_data_version(left) != _raw_document_data_version(right)


def test_upload_raw_documents_writes_latest_pointer_with_objects(tmp_path):
    root = tmp_path / "raw"
    _write_raw_output(root, metadata_fetched_at="2026-07-09T12:00:00Z")
    store = FakeObjectStore()
    data_version = _raw_document_data_version(root)

    metadata = _upload_raw_documents(root, PubMedia.LTA, "2026", data_version, store)

    assert metadata["raw_bucket"] == "opensourcelaw-raw"
    assert metadata["raw_uploaded_object_count"] == 5
    latest = store.json_objects["raw/retsinformation_documents/lta/2026/latest.json"]
    assert latest["data_version"] == data_version
    assert {item["path"] for item in latest["objects"]} == {
        "failures.jsonl",
        "jsonld/000001_1.json",
        "manifest.json",
        "metadata/000001_1.json",
        "xml/000001_1.xml",
    }


def _write_raw_output(root, metadata_fetched_at):
    (root / "xml").mkdir(parents=True)
    (root / "jsonld").mkdir()
    (root / "metadata").mkdir()
    (root / "xml" / "000001_1.xml").write_text("<xml />\n", encoding="utf-8")
    (root / "jsonld" / "000001_1.json").write_text('{"id":1}\n', encoding="utf-8")
    (root / "metadata" / "000001_1.json").write_text(
        f'{{"fetched_at":"{metadata_fetched_at}"}}\n',
        encoding="utf-8",
    )
    (root / "manifest.json").write_text(
        f'{{"outputDir":"{root}"}}\n',
        encoding="utf-8",
    )
    (root / "failures.jsonl").write_text("", encoding="utf-8")


class FakeObjectStore:
    def __init__(self):
        self.file_objects = {}
        self.json_objects = {}

    def resolved_bucket(self):
        return "opensourcelaw-raw"

    def ensure_bucket(self):
        pass

    def put_file(self, key, path, content_type):
        self.file_objects[key] = (path.read_bytes(), content_type)

    def put_json(self, key, value):
        self.json_objects[key] = value
