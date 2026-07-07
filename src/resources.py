from pathlib import Path

import httpx
from dagster import ConfigurableResource


class LearningStorageResource(ConfigurableResource):
    base_dir: str = "data/learning"

    def path_for(self, filename: str) -> Path:
        output_path = Path(self.base_dir) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path


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
