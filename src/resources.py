import tempfile
from pathlib import Path
from urllib.request import Request, urlopen

from dagster import AssetExecutionContext, ConfigurableResource, PipesSubprocessClient


class LearningStorageResource(ConfigurableResource):
    base_dir: str = "data/learning"

    def path_for(self, filename: str) -> Path:
        path = Path(self.base_dir) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path


class DotnetScriptResource(ConfigurableResource):
    command: str = "dotnet"

    def run_json(
        self, context: AssetExecutionContext, script_path: Path, payload: object
    ) -> object:
        with tempfile.TemporaryDirectory(prefix="opensourcelaw-dotnet-") as artifacts_dir:
            invocation = PipesSubprocessClient(
                cwd=str(script_path.parent),
                env={
                    "DOTNET_CLI_TELEMETRY_OPTOUT": "1",
                    "DOTNET_NOLOGO": "1",
                },
            ).run(
                context=context,
                command=[
                    self.command,
                    "run",
                    "--artifacts-path",
                    artifacts_dir,
                    "--file",
                    str(script_path),
                ],
                extras={"payload": payload},
            )

        messages = invocation.get_custom_messages()

        if len(messages) != 1:
            raise RuntimeError(
                f"dotnet script reported {len(messages)} custom messages, expected 1"
            )

        return messages[0]


class RetsinformationHttpResource(ConfigurableResource):
    timeout_seconds: float = 30.0
    user_agent: str = "opensourcelaw-retsinformation-ingest/0.1"

    def get_bytes(self, url: str) -> bytes:
        request = Request(url, headers={"User-Agent": self.user_agent})

        with urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read()
