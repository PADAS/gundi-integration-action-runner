import pydantic
from typing import List
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions
from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str = pydantic.Field(..., title = "Username", description = "Username for reading Discourse topics")
    apikey: pydantic.SecretStr = pydantic.Field(..., title = "API Key", 
                                description = "API Key for reading Discourse topics", format="password")
                                
    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "username",
            "apikey",
        ],
    )

class ReadDiscourseTopicsConfig(PullActionConfiguration):
    discourse_feed_url: pydantic.HttpUrl = pydantic.Field('https://community.earthranger.com/latest.json', title = "Discourse Feed", description = "URL to the Discourse feed where we'll read topics and posts")
    include_these_tags: List[str] = pydantic.Field(['er-notify'], title = "Read these tags", description = "Tags to filter the topics by")
    storage_bucket: str = pydantic.Field('er_notifications', title = "Storage Bucket", description = "Google Cloud Storage bucket to store the topics")
    storage_blob: str = pydantic.Field('topic_feeds.json', title = "Output file name", description = "Output file name in the Google Storage bucket")
    
    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "discourse_feed_url",
            "include_these_tags",
            "storage_bucket",
            "storage_blob",
        ],
    )