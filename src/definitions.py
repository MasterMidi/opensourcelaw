from dagster import Definitions, ScheduleDefinition, define_asset_job

from learning import (
    configurable_greeting,
    excited_hello,
    fake_raw_page_files,
    fake_raw_pages,
    fake_source_urls,
    greeting_file,
    hello_dagster,
    page_summary,
    parsed_page_titles,
    parsed_titles_are_not_empty,
    parsed_titles_from_files,
)
from src.resources import LearningStorageResource

learning_file_pipeline_job = define_asset_job(
    name="learning_file_pipeline", selection="*parsed_titles_from_files"
)

learning_file_pipeline_schedule = ScheduleDefinition(
    job=learning_file_pipeline_job, cron_schedule="* * * * *"
)

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
        fake_raw_page_files,
        parsed_titles_from_files,
    ],
    asset_checks=[parsed_titles_are_not_empty],
    jobs=[learning_file_pipeline_job],
    schedules=[learning_file_pipeline_schedule],
    resources={"learning_storage": LearningStorageResource()},
)
