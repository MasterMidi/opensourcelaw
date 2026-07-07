from pathlib import Path

from dagster import ConfigurableResource


class LearningStorageResource(ConfigurableResource):
    base_dir: str = "data/learning"

    def path_for(self, filename: str) -> Path:
        output_path = Path(self.base_dir) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path