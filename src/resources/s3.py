import hashlib
import hmac
import json
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from dagster import ConfigurableResource


class S3RequestError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class S3ObjectStoreResource(ConfigurableResource):
    bucket: str = "opensourcelaw-raw"
    endpoint_url: str = "http://localhost:8333"
    region: str = "us-east-1"
    access_key_id: str = "opensourcelaw"
    secret_access_key: str = "opensourcelaw"
    max_attempts: int = 3

    def resolved_bucket(self) -> str:
        return self.bucket

    def ensure_bucket(self) -> None:
        bucket = self.resolved_bucket()
        try:
            self._request("HEAD", bucket, "", b"", {}, {200})
            return
        except S3RequestError as error:
            if error.status != 404:
                raise

        self._request("PUT", bucket, "", b"", {}, {200, 201})

    def put_file(self, key: str, path: Path, content_type: str) -> None:
        self._request(
            "PUT",
            self.resolved_bucket(),
            key,
            path.read_bytes(),
            {"content-type": content_type},
            {200, 201},
        )

    def put_json(self, key: str, value: object) -> None:
        self._request(
            "PUT",
            self.resolved_bucket(),
            key,
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            {"content-type": "application/json"},
            {200, 201},
        )

    def get_object(self, key: str) -> bytes:
        return self._request("GET", self.resolved_bucket(), key, b"", {}, {200})

    def get_json(self, key: str) -> object:
        return json.loads(self.get_object(key).decode("utf-8"))

    def _request(
        self,
        method: str,
        bucket: str,
        key: str,
        body: bytes,
        headers: dict[str, str],
        ok_statuses: set[int],
    ) -> bytes:
        attempts = max(1, self.max_attempts)

        for attempt in range(1, attempts + 1):
            request = self._signed_request(method, bucket, key, body, headers)
            try:
                with urlopen(request, timeout=30) as response:
                    if response.status not in ok_statuses:
                        raise S3RequestError(
                            response.status,
                            f"S3 {method} s3://{bucket}/{key} returned HTTP "
                            f"{response.status}",
                        )
                    return response.read()
            except HTTPError as error:
                detail = error.read().decode("utf-8", "replace")
                if 500 <= error.code <= 599 and attempt < attempts:
                    sleep(attempt)
                    continue
                raise S3RequestError(
                    error.code,
                    f"S3 {method} s3://{bucket}/{key} failed HTTP {error.code} "
                    f"after {attempt} attempt(s): {detail}",
                ) from error

        raise AssertionError("unreachable")

    def _signed_request(
        self,
        method: str,
        bucket: str,
        key: str,
        body: bytes,
        headers: dict[str, str],
    ) -> Request:
        region = self.region
        endpoint_url = self.endpoint_url
        access_key = self.access_key_id
        secret_key = self.secret_access_key
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body).hexdigest()
        url, canonical_uri, host = _s3_url(endpoint_url, bucket, key)
        signed_headers_map = {
            name.lower(): value.strip() for name, value in headers.items()
        }
        signed_headers_map.update(
            {
                "host": host,
                "x-amz-content-sha256": payload_hash,
                "x-amz-date": amz_date,
            }
        )
        signed_header_names = sorted(signed_headers_map)
        signed_headers = ";".join(signed_header_names)
        canonical_headers = "".join(
            f"{name}:{signed_headers_map[name]}\n" for name in signed_header_names
        )
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                "",
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        scope = f"{date_stamp}/{region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = hmac.new(
            _s3_signing_key(secret_key, date_stamp, region),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed_headers_map["authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        return Request(
            url,
            data=None if method == "HEAD" else body,
            headers=signed_headers_map,
            method=method,
        )


def _s3_url(endpoint_url: str, bucket: str, key: str) -> tuple[str, str, str]:
    parsed = urlsplit(endpoint_url.rstrip("/"))
    path_parts = [part for part in parsed.path.split("/") if part]
    path_parts.append(bucket)
    path_parts.extend(part for part in key.split("/") if part)
    canonical_uri = "/" + "/".join(quote(part, safe="-_.~") for part in path_parts)
    return (
        urlunsplit((parsed.scheme, parsed.netloc, canonical_uri, "", "")),
        canonical_uri,
        parsed.netloc,
    )


def _s3_signing_key(secret_key: str, date_stamp: str, region: str) -> bytes:
    date_key = hmac.new(
        ("AWS4" + secret_key).encode("utf-8"),
        date_stamp.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    region_key = hmac.new(date_key, region.encode("utf-8"), hashlib.sha256).digest()
    service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()
