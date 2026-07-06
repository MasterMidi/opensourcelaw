from dagster import Definitions

from learning import hello_dagster


defs = Definitions(
    assets=[hello_dagster],
)
