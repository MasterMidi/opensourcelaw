from __future__ import annotations

import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


class RateLimiter:
    def __init__(self, delay_seconds: float = 0.0):
        self.delay_seconds = max(0.0, delay_seconds)
        self._next_allowed = 0.0

    def wait(self) -> None:
        wait_for = self._next_allowed - time.time()
        if wait_for > 0:
            time.sleep(wait_for)
        self._next_allowed = max(time.time(), self._next_allowed) + self.delay_seconds


@dataclass
class HttpResponse:
    url: str
    status_code: int | None
    headers: dict[str, str]
    content: bytes
    error: str | None = None


class HttpClient:
    def __init__(
        self,
        *,
        timeout_seconds: float,
        headers: dict[str, str],
        retries: int,
        retry_status_codes: set[int],
        retry_backoff_seconds: float,
        limiter: RateLimiter | None = None,
        verify_ssl: bool = True,
    ):
        self.timeout_seconds = timeout_seconds
        self.headers = headers
        self.retries = max(1, retries)
        self.retry_status_codes = retry_status_codes
        self.retry_backoff_seconds = retry_backoff_seconds
        self.limiter = limiter or RateLimiter()
        self.verify_ssl = verify_ssl

    def get(self, url: str) -> HttpResponse:
        last_error: str | None = None
        for attempt in range(1, self.retries + 1):
            self.limiter.wait()
            try:
                return self._get_once(url, verify_ssl=self.verify_ssl)
            except urllib.error.HTTPError as exc:
                body = exc.read()
                headers = {key: value for key, value in exc.headers.items()}
                last_error = f"HTTP {exc.code}: {exc.reason}"
                if exc.code in self.retry_status_codes and attempt < self.retries:
                    self._sleep_before_retry(attempt)
                    continue
                return HttpResponse(url=url, status_code=exc.code, headers=headers, content=body, error=last_error)
            except urllib.error.URLError as exc:
                if self.verify_ssl and _is_ssl_error(exc):
                    try:
                        return self._get_once(url, verify_ssl=False)
                    except urllib.error.URLError as fallback_exc:
                        last_error = str(fallback_exc.reason)
                else:
                    last_error = str(exc.reason)
                if attempt < self.retries:
                    self._sleep_before_retry(attempt)
                    continue
                return HttpResponse(url=url, status_code=None, headers={}, content=b"", error=last_error)
        return HttpResponse(url=url, status_code=None, headers={}, content=b"", error=last_error or "request failed")

    def _get_once(self, url: str, *, verify_ssl: bool) -> HttpResponse:
        request = urllib.request.Request(url, headers=self.headers, method="GET")
        context = None if verify_ssl else ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=context) as response:
            content = response.read()
            headers = {key: value for key, value in response.headers.items()}
            return HttpResponse(url=url, status_code=response.status, headers=headers, content=content)

    def _sleep_before_retry(self, attempt: int) -> None:
        time.sleep(self.retry_backoff_seconds * (2 ** (attempt - 1)))


def header_value(headers: dict[str, str], name: str) -> str | None:
    lower_name = name.lower()
    for key, value in headers.items():
        if key.lower() == lower_name:
            return value
    return None


def client_from_config(config: dict[str, Any], *, delay_key: str) -> HttpClient:
    headers = {
        "User-Agent": str(config.get("user_agent") or "opensourcelaw-retsinformation-ingest/0.1"),
        "Accept": str(config.get("accept") or "*/*"),
    }
    delay = float(config.get(delay_key, config.get("request_delay_seconds", 0.0)) or 0.0)
    retry_status_codes = {int(code) for code in config.get("retry_status_codes", [429, 500, 502, 503])}
    return HttpClient(
        timeout_seconds=float(config.get("timeout_seconds", 30)),
        headers=headers,
        retries=int(config.get("retries", 3)),
        retry_status_codes=retry_status_codes,
        retry_backoff_seconds=float(config.get("retry_backoff_seconds", 10.0)),
        limiter=RateLimiter(delay),
        verify_ssl=bool(config.get("verify_ssl", True)),
    )


def _is_ssl_error(exc: urllib.error.URLError) -> bool:
    reason = exc.reason
    return isinstance(reason, ssl.SSLError) or isinstance(reason, ssl.SSLCertVerificationError)
