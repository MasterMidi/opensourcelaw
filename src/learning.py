from pathlib import Path

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


@asset(group_name="learning")
def greeting_file(context: AssetExecutionContext, configurable_greeting: str) -> str:
    output_dir = Path("data/learning")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "greeting.txt"
    output_path.write_text(configurable_greeting, encoding="utf-8")

    context.log.info(f"Wrote greeting to {output_path}")

    context.add_output_metadata(
        {
            "output_path": str(output_path),
            "bytes_written": output_path.stat().st_size,
        }
    )

    return str(output_path)
