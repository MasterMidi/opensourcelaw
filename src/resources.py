import json
import os
import subprocess
from pathlib import Path
from urllib.request import Request, urlopen

from dagster import ConfigurableResource


class LearningStorageResource(ConfigurableResource):
    base_dir: str = "data/learning"

    def path_for(self, filename: str) -> Path:
        path = Path(self.base_dir) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path


class DotnetScriptResource(ConfigurableResource):
    command: str = "dotnet"
    timeout_seconds: float = 600.0

    def run_json(self, script_path: Path, payload: object) -> object:
        completed = subprocess.run(
            [self.command, "run", str(script_path)],
            input=json.dumps(payload, separators=(",", ":")),
            capture_output=True,
            check=False,
            cwd=script_path.parent,
            env=os.environ | {
                "DOTNET_CLI_TELEMETRY_OPTOUT": "1",
                "DOTNET_NOLOGO": "1",
            },
            text=True,
            timeout=self.timeout_seconds,
        )

        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())

        return json.loads(completed.stdout)


class RetsinformationHttpResource(ConfigurableResource):
    timeout_seconds: float = 30.0
    user_agent: str = "opensourcelaw-retsinformation-ingest/0.1"

    def get_bytes(self, url: str) -> bytes:
        request = Request(url, headers={"User-Agent": self.user_agent})

        with urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read()
