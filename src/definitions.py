from dagster import Definitions

from learning import configurable_greeting, excited_hello, greeting_file, hello_dagster

defs = Definitions(
    assets=[hello_dagster, excited_hello, configurable_greeting, greeting_file],
)
