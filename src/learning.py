from dataclasses import dataclass
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    Config,
    DailyPartitionsDefinition,
    asset,
    asset_check,
)

from src.resources import LearningStorageResource


@dataclass
class SavedPage:
    url: str
    file_path: str
    bytes_written: int


@dataclass
class ParsedTitle:
    url: str
    file_path: str
    title: str


class GreetingConfig(Config):
    name: str = "Dagster"
    excited: bool = True


class FakeSourceConfig(Config):
    page_count: int = 3


daily_partitions = DailyPartitionsDefinition(start_date="2026-07-01")


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
def greeting_file(
    context: AssetExecutionContext,
    configurable_greeting: str,
    learning_storage: LearningStorageResource,
) -> str:
    output_path = learning_storage.path_for("greeting.txt")
    output_path.write_text(configurable_greeting, encoding="utf-8")

    context.log.info(f"Wrote greeting to {output_path}")

    context.add_output_metadata(
        {
            "output_path": str(output_path),
            "bytes_written": output_path.stat().st_size,
        }
    )

    return str(output_path)


@asset(group_name="learning")
def fake_source_urls(
    context: AssetExecutionContext, config: FakeSourceConfig
) -> list[str]:
    urls = [
        f"https://example.com/law/{page_number}"
        for page_number in range(1, config.page_count + 1)
    ]

    context.add_output_metadata({"page_count": config.page_count, "urls": urls})

    return urls


@asset(group_name="learning")
def fake_raw_pages(
    context: AssetExecutionContext,
    fake_source_urls: list[str],
) -> list[dict[str, str]]:
    pages = []

    for url in fake_source_urls:
        page = {
            "url": url,
            "html": f"<html><title>Page for {url}</title><body>Some legal text</body></html>",
        }
        pages.append(page)

    context.add_output_metadata(
        {
            "page_count": len(pages),
            "urls": fake_source_urls,
        }
    )

    return pages


@asset(group_name="learning")
def parsed_page_titles(
    context: AssetExecutionContext,
    fake_raw_pages: list[dict[str, str]],
) -> list[dict[str, str]]:
    titles = []

    for page in fake_raw_pages:
        html = page["html"]
        title = html.split("<title>")[1].split("</title>")[0]

        titles.append(
            {
                "url": page["url"],
                "title": title,
            }
        )

    context.add_output_metadata(
        {
            "title_count": len(titles),
        }
    )

    return titles


@asset(group_name="learning")
def page_summary(
    context: AssetExecutionContext,
    parsed_page_titles: list[dict[str, str]],
) -> dict[str, int]:
    summary = {
        "pages_seen": len(parsed_page_titles),
        "titles_seen": sum(1 for page in parsed_page_titles if page["title"]),
    }

    context.add_output_metadata(summary)

    return summary


@asset(group_name="learning")
def fake_raw_page_files(
    context: AssetExecutionContext,
    fake_source_urls: list[str],
    learning_storage: LearningStorageResource,
) -> list[SavedPage]:
    saved_pages = []

    for index, url in enumerate(fake_source_urls, start=1):
        html = f"<html><title>Page for {url}</title><body>Some legal text</body></html>"
        output_path = learning_storage.path_for(f"raw_pages/page_{index}.html")

        output_path.write_text(html, encoding="utf-8")

        saved_pages.append(
            SavedPage(
                url=url,
                file_path=str(output_path),
                bytes_written=output_path.stat().st_size,
            )
        )

    context.add_output_metadata(
        {
            "file_count": len(saved_pages),
            "total_bytes": sum(page.bytes_written for page in saved_pages),
        }
    )

    return saved_pages


@asset(group_name="learning")
def parsed_titles_from_files(
    context: AssetExecutionContext,
    fake_raw_page_files: list[SavedPage],
) -> list[ParsedTitle]:
    titles = []

    for saved_page in fake_raw_page_files:
        file_path = saved_page.file_path
        html = Path(file_path).read_text(encoding="utf-8")

        if "<title>" not in html or "</title>" not in html:
            context.log.warning(f"Missing title in {file_path}")
            continue

        title = html.split("<title>")[1].split("</title>")[0]

        titles.append(
            ParsedTitle(
                url=str(saved_page.url),
                file_path=file_path,
                title=title,
            )
        )

    context.add_output_metadata(
        {
            "title_count": len(titles),
            "input_file_count": len(fake_raw_page_files),
        }
    )

    return titles


@asset_check(asset=parsed_titles_from_files)
def parsed_titles_are_not_empty(
    parsed_titles_from_files: list[ParsedTitle],
) -> AssetCheckResult:
    title_count = len(parsed_titles_from_files)

    return AssetCheckResult(
        passed=title_count > 0,
        metadata={
            "title_count": title_count,
        },
    )


@asset(group_name="learning", partitions_def=daily_partitions)
def daily_learning_note(
    context: AssetExecutionContext,
    learning_storage: LearningStorageResource,
) -> str:
    partition_date = context.partition_key
    output_path = learning_storage.path_for(f"daily_notes/{partition_date}.txt")

    message = f"This asset ran for partition {partition_date}"
    output_path.write_text(message, encoding="utf-8")

    context.add_output_metadata(
        {
            "partition_date": partition_date,
            "output_path": str(output_path),
        }
    )

    return str(output_path)


@asset(group_name="learning", partitions_def=daily_partitions)
def daily_fake_source_urls(context: AssetExecutionContext) -> list[str]:
    partition_date = context.partition_key

    urls = [
        f"https://example.com/{partition_date}/law/1",
        f"https://example.com/{partition_date}/law/2",
        f"https://example.com/{partition_date}/law/3",
    ]

    context.add_output_metadata(
        {
            "partition_date": partition_date,
            "url_count": len(urls),
        }
    )

    return urls


@asset(group_name="learning", partitions_def=daily_partitions)
def daily_fake_raw_page_files(
    context: AssetExecutionContext,
    daily_fake_source_urls: list[str],
    learning_storage: LearningStorageResource,
) -> list[SavedPage]:
    partition_date = context.partition_key
    saved_pages = []

    for index, url in enumerate(daily_fake_source_urls, start=1):
        html = f"<html><title>Page for {url}</title><body>Some legal text</body></html>"
        output_path = learning_storage.path_for(
            f"daily_raw_pages/{partition_date}/page_{index}.html"
        )

        output_path.write_text(html, encoding="utf-8")

        saved_pages.append(
            SavedPage(
                url=url,
                file_path=str(output_path),
                bytes_written=output_path.stat().st_size,
            )
        )

    context.add_output_metadata(
        {
            "partition_date": partition_date,
            "file_count": len(saved_pages),
            "total_bytes": sum(page.bytes_written for page in saved_pages),
        }
    )

    return saved_pages


@asset(group_name="learning", partitions_def=daily_partitions)
def daily_parsed_titles_from_files(
    context: AssetExecutionContext,
    daily_fake_raw_page_files: list[SavedPage],
) -> list[ParsedTitle]:
    titles = []

    for saved_page in daily_fake_raw_page_files:
        html = Path(saved_page.file_path).read_text(encoding="utf-8")

        if "<title>" not in html or "</title>" not in html:
            context.log.warning(f"Missing title in {saved_page.file_path}")
            continue

        title = html.split("<title>")[1].split("</title>")[0]

        titles.append(
            ParsedTitle(
                url=saved_page.url,
                file_path=saved_page.file_path,
                title=title,
            )
        )

    context.add_output_metadata(
        {
            "partition_date": context.partition_key,
            "title_count": len(titles),
        }
    )

    return titles
