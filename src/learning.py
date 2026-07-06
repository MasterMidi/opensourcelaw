from dagster import asset


@asset(group_name="learning")
def hello_dagster() -> str:
    return "Hello from Dagster"
