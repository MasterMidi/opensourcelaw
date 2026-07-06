from dagster import AssetExecutionContext, Config, asset


class GreetingConfig(Config):
    name: str = "Dagster"
    excited: bool = True


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


@asset(group_name="learning")
def configurable_greeting(
    context: AssetExecutionContext,
    config: GreetingConfig,
) -> str:
    message = f"Hello, {config.name}"

    if config.excited:
        message = message.upper() + "!"

    context.log.info(f"Created greeting: {message}")

    context.add_output_metadata(
        {
            "name": config.name,
            "excited": config.excited,
            "message": message,
        }
    )

    return message
