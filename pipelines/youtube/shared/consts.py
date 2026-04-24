import os
from dotenv import load_dotenv

load_dotenv()

INPUT_PATH = "s3://de-youtubedata-raw-useast1-dev/youtube/raw_statistics/"
OUTPUT_PATH = "s3://de-youtubedata-cleansed-useast1-dev/youtube/raw_statistics/"

PREDICATE = "region in ('ca','gb','us')"

CSV_OPTIONS = {
    "quoteChar": '"',
    "withHeader": True,
    "separator": ",",
    "optimizePerformance": False,
}

MAPPINGS = [
    ("video_id", "string", "video_id", "string"),
    ("trending_date", "string", "trending_date", "string"),
    ("title", "string", "title", "string"),
    ("channel_title", "string", "channel_title", "string"),
    ("category_id", "bigint", "category_id", "bigint"),
    ("publish_time", "string", "publish_time", "string"),
    ("tags", "string", "tags", "string"),
    ("views", "bigint", "views", "bigint"),
    ("likes", "bigint", "likes", "bigint"),
    ("dislikes", "bigint", "dislikes", "bigint"),
    ("comment_count", "bigint", "comment_count", "bigint"),
    ("thumbnail_link", "string", "thumbnail_link", "string"),
    ("comments_disabled", "boolean", "comments_disabled", "boolean"),
    ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
    ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
    ("description", "string", "description", "string"),
]

S3_CLEANSED_PATH = os.getenv("S3_CLEANSED_LAYER")
GLUE_DB = os.getenv("GLUE_CATALOG_DB_NAME")
GLUE_TABLE = os.getenv("GLUE_CATALOG_TABLE_NAME")
WRITE_MODE = os.getenv("WRITE_DATA_OP", "append")