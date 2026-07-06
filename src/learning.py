from dagster import AssetExecutionContext, asset


@asset(group_name="learning")
def hello_dagster(context: AssetExecutionContext) -> str:
    message = "Hello from Dagster"
    context.log.info("Created the hello message")
    context.add_output_metadata({"message": message, "message_length": len(message)})
    return message


@asset(group_name="learning")
def excited_hello(context: AssetExecutionContext, hello_dagster: str) -> str:
    excited_message = hello_dagster.upper() + "!"
    context.log.info("Created the excited hello message")
    context.add_output_metadata(
        {
            "original_message": hello_dagster,
            "excited_message": excited_message,
            "excited_message_length": len(excited_message),
        }
    )
    return excited_message
