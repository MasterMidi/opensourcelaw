from dagster import asset


@asset(group_name="learning")
def hello_dagster() -> str:
    return "Hello from Dagster"


@asset(group_name="learning")
def excited_hello(hello_dagster: str) -> str:
    return hello_dagster.upper() + "!"
