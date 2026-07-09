from email.message import Message
from io import BytesIO
from urllib.error import HTTPError

import pytest

import src.resources.s3 as s3
from src.resources import S3ObjectStoreResource, S3RequestError


def test_s3_resource_defaults_to_local_seaweedfs(monkeypatch):
    captured = _capture_s3_requests(monkeypatch)

    S3ObjectStoreResource().put_json("x.json", {"ok": True})

    request = captured[0]
    assert request["url"] == "http://localhost:8333/opensourcelaw-raw/x.json"
    assert request["body"] == b'{"ok":true}'
    assert request["headers"]["content-type"] == "application/json"
    assert "Credential=opensourcelaw/" in request["headers"]["authorization"]


def test_s3_resource_uses_dagster_config(monkeypatch):
    captured = _capture_s3_requests(monkeypatch)

    S3ObjectStoreResource(
        endpoint_url="http://seaweedfs:8333",
        bucket="configured-bucket",
        region="eu-west-1",
        access_key_id="configured-login",
        secret_access_key="configured-secret",
    ).put_json("x.json", {"ok": True})

    request = captured[0]
    assert request["url"] == "http://seaweedfs:8333/configured-bucket/x.json"
    assert "Credential=configured-login/" in request["headers"]["authorization"]
    assert "/eu-west-1/s3/aws4_request" in request["headers"]["authorization"]


def test_s3_resource_uploads_xml_and_empty_jsonl_files(monkeypatch, tmp_path):
    captured = _capture_s3_requests(monkeypatch)
    xml_path = tmp_path / "document.xml"
    failures_path = tmp_path / "failures.jsonl"
    xml_path.write_text("<root />\n", encoding="utf-8")
    failures_path.write_text("", encoding="utf-8")
    store = S3ObjectStoreResource()

    store.put_file("xml/document.xml", xml_path, "application/xml")
    store.put_file("failures.jsonl", failures_path, "application/x-ndjson")

    assert captured[0]["url"] == "http://localhost:8333/opensourcelaw-raw/xml/document.xml"
    assert captured[0]["body"] == b"<root />\n"
    assert captured[0]["headers"]["content-type"] == "application/xml"
    assert captured[1]["url"] == "http://localhost:8333/opensourcelaw-raw/failures.jsonl"
    assert captured[1]["body"] == b""
    assert captured[1]["headers"]["content-type"] == "application/x-ndjson"


def test_s3_resource_retries_transient_server_errors(monkeypatch):
    attempts = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            pass

        def read(self):
            return b""

    def fake_urlopen(request, timeout):
        attempts.append(request.full_url)
        if len(attempts) == 1:
            raise HTTPError(
                request.full_url,
                500,
                "Internal Server Error",
                Message(),
                BytesIO(b"<Error><Code>InternalError</Code></Error>"),
            )
        return Response()

    monkeypatch.setattr(s3, "sleep", lambda seconds: None)
    monkeypatch.setattr(s3, "urlopen", fake_urlopen)

    S3ObjectStoreResource(max_attempts=2).put_json("x.json", {"ok": True})

    assert len(attempts) == 2


def test_s3_resource_reports_server_error_body(monkeypatch):
    def fake_urlopen(request, timeout):
        raise HTTPError(
            request.full_url,
            500,
            "Internal Server Error",
            Message(),
            BytesIO(b"<Error><Code>InternalError</Code></Error>"),
        )

    monkeypatch.setattr(s3, "urlopen", fake_urlopen)

    with pytest.raises(S3RequestError, match="InternalError") as error:
        S3ObjectStoreResource(max_attempts=1).put_json("x.json", {"ok": True})

    assert error.value.status == 500


def test_s3_resource_still_creates_missing_bucket(monkeypatch):
    methods = []

    def fake_request(self, method, bucket, key, body, headers, ok_statuses):
        methods.append(method)
        if method == "HEAD":
            raise S3RequestError(404, "failed")
        return b""

    monkeypatch.setattr(S3ObjectStoreResource, "_request", fake_request)

    S3ObjectStoreResource().ensure_bucket()

    assert methods == ["HEAD", "PUT"]


def _capture_s3_requests(monkeypatch):
    captured = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            pass

        def read(self):
            return b""

    def fake_urlopen(request, timeout):
        captured.append(
            {
                "url": request.full_url,
                "body": request.data,
                "headers": {
                    name.lower(): value for name, value in request.header_items()
                },
            }
        )
        return Response()

    monkeypatch.setattr(s3, "urlopen", fake_urlopen)
    return captured
