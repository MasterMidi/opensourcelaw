from dagster import Definitions

from learning import configurable_greeting, excited_hello, hello_dagster

defs = Definitions(
    assets=[hello_dagster, excited_hello, configurable_greeting],
)
