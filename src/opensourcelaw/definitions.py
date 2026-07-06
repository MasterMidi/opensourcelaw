from dagster import Definitions, asset


@asset
def example_asset() -> int:
    return 1


defs = Definitions(assets=[example_asset])
