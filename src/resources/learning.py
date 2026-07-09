from pathlib import Path

from dagster import ConfigurableResource


class LearningStorageResource(ConfigurableResource):
    base_dir: str = "data/learning"

    def path_for(self, filename: str) -> Path:
        path = Path(self.base_dir) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
