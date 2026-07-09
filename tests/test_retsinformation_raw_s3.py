from src.assets.retsinformation.documents_raw import _stable_result_metadata


def test_stable_result_metadata_omits_large_objects_and_local_paths():
    assert _stable_result_metadata(
        {
            "dataVersion": "abc",
            "downloadedCount": 1,
            "objects": [{"key": "raw/object", "path": "xml/doc.xml"}],
            "outputDir": "/tmp/raw",
            "xmlDirectoryPath": "/tmp/raw/xml",
            "manifestPath": "/tmp/raw/manifest.json",
        }
    ) == {"dataVersion": "abc", "downloadedCount": 1}
