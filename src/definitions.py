from dagster import (
    Definitions,
    PoolMetadataValue,
    ScheduleDefinition,
    define_asset_job,
)

from learning import (
    configurable_greeting,
    daily_fake_raw_page_files,
    daily_fake_source_urls,
    daily_learning_note,
    daily_parsed_titles_from_files,
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
    yearly_fake_raw_page_files,
    yearly_fake_source_urls,
    yearly_parsed_titles_from_files,
)
from src.assets.retsinformation.document import retsinfo_documents
from src.assets.retsinformation.sitemap_pages import retsinfo_sitemap_page
from src.assets.retsinformation.sitemap_index import retsinfo_sitemap_index
from src.resources import (
    DotnetScriptResource,
    LearningStorageResource,
    RetsinformationHttpResource,
)

learning_file_pipeline_job = define_asset_job(
    name="learning_file_pipeline", selection="*parsed_titles_from_files"
)

learning_file_pipeline_schedule = ScheduleDefinition(
    job=learning_file_pipeline_job, cron_schedule="30 * * * *"
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
        daily_learning_note,
        daily_fake_source_urls,
        daily_fake_raw_page_files,
        daily_parsed_titles_from_files,
        yearly_fake_source_urls,
        yearly_fake_raw_page_files,
        yearly_parsed_titles_from_files,
        retsinfo_sitemap_index,
        retsinfo_sitemap_page,
        retsinfo_documents,
    ],
    asset_checks=[parsed_titles_are_not_empty],
    jobs=[learning_file_pipeline_job],
    schedules=[learning_file_pipeline_schedule],
    resources={
        "dotnet_script": DotnetScriptResource(),
        "learning_storage": LearningStorageResource(),
        "retsinformation_http": RetsinformationHttpResource(),
    },
)
