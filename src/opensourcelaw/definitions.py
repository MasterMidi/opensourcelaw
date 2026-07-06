from dagster import Definitions

from opensourcelaw.assets.retsinformation import RETSINFORMATION_RAW_ASSETS
from opensourcelaw.resources import IngestStoreResource


defs = Definitions(
    assets=RETSINFORMATION_RAW_ASSETS,
    resources={"ingest_store": IngestStoreResource()},
)
