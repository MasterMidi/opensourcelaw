import tempfile
from pathlib import Path

from dagster import AssetExecutionContext, ConfigurableResource, PipesSubprocessClient


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
