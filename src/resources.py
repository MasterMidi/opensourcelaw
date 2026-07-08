import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

import httpx
from dagster import ConfigurableResource


class LearningStorageResource(ConfigurableResource):
    base_dir: str = "data/learning"

    def path_for(self, filename: str) -> Path:
        output_path = Path(self.base_dir) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path


class DotnetScriptResource(ConfigurableResource):
    command: str = "dotnet"
    timeout_seconds: float = 600.0

    def run_json(self, script_path: Path, payload: object) -> object:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")

        dotnet_path = shutil.which(self.command)

        if dotnet_path is None:
            raise RuntimeError(f"{self.command} is required for DotnetScriptResource")

        if not script_path.exists():
            raise RuntimeError(f"dotnet script does not exist: {script_path}")

        env = os.environ | {
            "DOTNET_CLI_TELEMETRY_OPTOUT": "1",
            "DOTNET_NOLOGO": "1",
            "DOTNET_SKIP_FIRST_TIME_EXPERIENCE": "1",
        }

        try:
            completed = subprocess.run(
                [dotnet_path, "run", str(script_path)],
                input=json.dumps(payload, separators=(",", ":")),
                capture_output=True,
                check=False,
                cwd=script_path.parent,
                env=env,
                text=True,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"dotnet script timed out: {script_path}") from error

        if completed.returncode != 0:
            error_message = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(
                f"dotnet script failed ({script_path}): "
                f"{error_message or f'exit {completed.returncode}'}"
            )

        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                f"dotnet script returned invalid JSON ({script_path}): "
                f"{completed.stdout[:500]!r}"
            ) from error


@dataclass(frozen=True)
class CurlResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    content: bytes

    @property
    def text(self) -> str:
        encoding = _encoding_from_content_type(self.headers.get("content-type", ""))

        try:
            return self.content.decode(encoding, errors="replace")
        except LookupError:
            return self.content.decode("utf-8", errors="replace")

    @property
    def reason_phrase(self) -> str:
        try:
            return HTTPStatus(self.status_code).phrase
        except ValueError:
            return "HTTP error"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(
                f"curl GET {self.url} returned HTTP {self.status_code} "
                f"{self.reason_phrase}"
            )


def _encoding_from_content_type(content_type: str) -> str:
    for part in content_type.split(";")[1:]:
        key, separator, value = part.strip().partition("=")

        if separator and key.lower() == "charset" and value:
            return value.strip('"')

    return "utf-8"


def _parse_curl_headers(raw_headers: bytes) -> dict[str, str]:
    header_text = raw_headers.decode("iso-8859-1", errors="replace")
    blocks: list[list[str]] = []
    current_block: list[str] = []

    for line in header_text.splitlines():
        if line.startswith("HTTP/"):
            if current_block:
                blocks.append(current_block)
            current_block = [line]
            continue

        if not line.strip():
            if current_block:
                blocks.append(current_block)
                current_block = []
            continue

        if current_block:
            current_block.append(line)

    if current_block:
        blocks.append(current_block)

    final_block = blocks[-1] if blocks else []
    headers: dict[str, str] = {}

    for line in final_block[1:]:
        name, separator, value = line.partition(":")

        if separator:
            headers[name.strip().lower()] = value.strip()

    return headers


class RetsinformationCurlResource(ConfigurableResource):
    timeout_seconds: float = 30.0
    user_agent: str = "opensourcelaw-retsinformation-ingest/0.1"

    def get(self, url: str, *, follow_redirects: bool = True) -> CurlResponse:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")

        curl_path = shutil.which("curl")

        if curl_path is None:
            raise RuntimeError("curl is required for RetsinformationCurlResource")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            body_path = temp_path / "body"
            headers_path = temp_path / "headers"
            command = [
                curl_path,
                "--silent",
                "--show-error",
                "--compressed",
                "--max-time",
                str(self.timeout_seconds),
                "--user-agent",
                self.user_agent,
                "--dump-header",
                str(headers_path),
                "--output",
                str(body_path),
                "--write-out",
                "%{http_code}",
            ]

            if follow_redirects:
                command.append("--location")

            command.append(url)

            try:
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    check=False,
                    text=True,
                    timeout=self.timeout_seconds + 5,
                )
            except subprocess.TimeoutExpired as error:
                raise RuntimeError(f"curl timed out fetching {url}") from error

            if completed.returncode != 0:
                error_message = completed.stderr.strip() or f"exit {completed.returncode}"
                raise RuntimeError(f"curl failed fetching {url}: {error_message}")

            status_text = completed.stdout.strip()

            try:
                status_code = int(status_text)
            except ValueError as error:
                raise RuntimeError(
                    f"curl returned an invalid HTTP status for {url}: {status_text!r}"
                ) from error

            return CurlResponse(
                url=url,
                status_code=status_code,
                headers=_parse_curl_headers(headers_path.read_bytes()),
                content=body_path.read_bytes(),
            )


class RetsinformationHttpResource(ConfigurableResource):
    timeout_seconds: float = 30.0
    user_agent: str = "opensourcelaw-retsinformation-ingest/0.1"

    def get(self, url: str, *, follow_redirects: bool = True) -> httpx.Response:
        return httpx.get(
            url,
            timeout=self.timeout_seconds,
            follow_redirects=follow_redirects,
            headers={"User-Agent": self.user_agent},
        )

    def post_json(
        self,
        url: str,
        *,
        json: object,
        follow_redirects: bool = True,
    ) -> httpx.Response:
        return httpx.post(
            url,
            json=json,
            timeout=self.timeout_seconds,
            follow_redirects=follow_redirects,
            headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
        )
