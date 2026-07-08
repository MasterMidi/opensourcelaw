from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from learning import (
    parsed_titles_are_not_empty,
)
from src.assets.retsinformation.document import retsinfo_documents
from src.assets.retsinformation.sitemap_index import retsinfo_sitemap_index
from src.assets.retsinformation.sitemap_pages import retsinfo_sitemap_pages
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
        retsinfo_sitemap_index,
        retsinfo_sitemap_pages,
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
