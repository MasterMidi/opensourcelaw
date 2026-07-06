from __future__ import annotations

import os
from pathlib import Path

from dagster import ConfigurableResource

from opensourcelaw.retsinformation.storage import FilesystemIngestStore


class IngestStoreResource(ConfigurableResource):
    root_path: str = ""

    def create_store(self) -> FilesystemIngestStore:
        configured_path = self.root_path or os.environ.get("OPENSOURCELAW_INGEST_ROOT") or "data/ingest"
        return FilesystemIngestStore(Path(configured_path))
