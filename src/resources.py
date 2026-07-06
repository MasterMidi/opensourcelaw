from pathlib import Path

from dagster import ConfigurableResource


class LearningStorageResource(ConfigurableResource):
    base_dir: str = "data/learning"

    def path_for(self, filename: str) -> Path:
        output_dir = Path(self.base_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / filename