from dagster import Definitions

from learning import excited_hello, hello_dagster

defs = Definitions(
    assets=[hello_dagster, excited_hello],
)
