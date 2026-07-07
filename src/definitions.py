from dagster import Definitions

from learning import (
    configurable_greeting,
    excited_hello,
    fake_raw_pages,
    fake_source_urls,
    greeting_file,
    hello_dagster,
    page_summary,
    parsed_page_titles,
)
from src.resources import LearningStorageResource

defs = Definitions(
    assets=[
        hello_dagster,
        excited_hello,
        configurable_greeting,
        greeting_file,
        fake_source_urls,
        fake_raw_pages,
        parsed_page_titles,
        page_summary,
    ],
    resources={"learning_storage": LearningStorageResource()},
)
