from src.assets.retsinformation.documents_parsed import _download_latest_raw_documents
from src.assets.retsinformation.sitemap_pages import PubMedia


def test_download_latest_raw_documents_restores_s3_objects(tmp_path):
    store = FakeObjectStore()

    latest = _download_latest_raw_documents(PubMedia.LTA, "2026", store, tmp_path)

    assert latest["data_version"] == "abc"
    assert (tmp_path / "xml" / "000001_1.xml").read_bytes() == b"<xml />\n"
    assert (tmp_path / "metadata" / "000001_1.json").read_bytes() == b"{}\n"


class FakeObjectStore:
    def get_json(self, key):
        assert key == "raw/retsinformation_documents/lta/2026/latest.json"
        return {
            "data_version": "abc",
            "objects": [
                {"key": "raw/prefix/xml/000001_1.xml", "path": "xml/000001_1.xml"},
                {
                    "key": "raw/prefix/metadata/000001_1.json",
                    "path": "metadata/000001_1.json",
                },
            ],
        }

    def get_object(self, key):
        return {
            "raw/prefix/xml/000001_1.xml": b"<xml />\n",
            "raw/prefix/metadata/000001_1.json": b"{}\n",
        }[key]
