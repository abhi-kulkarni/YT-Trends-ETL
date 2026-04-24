
from typing import Tuple
import urllib
import awswrangler as wr
import pandas as pd
from pipelines.youtube.shared.consts import S3_CLEANSED_PATH, GLUE_DB, GLUE_TABLE, WRITE_MODE
import structlog

logger = structlog.get_logger(__name__)

def parse_s3_event(event: dict) -> Tuple[str, str]:
    """Extract bucket and key from S3 event"""
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        record["s3"]["object"]["key"], encoding="utf-8"
    )
    return bucket, key


def read_json_from_s3(bucket: str, key: str) -> pd.DataFrame:
    path = f"s3://{bucket}/{key}"
    logger.info(f"Reading JSON from {path}")
    return wr.s3.read_json(path)


def transform(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize nested JSON"""
    if "items" not in df_raw:
        raise ValueError("Missing 'items' key in input JSON")

    return pd.json_normalize(df_raw["items"])


def write_to_parquet(df: pd.DataFrame):
    logger.info("Writing data to S3 (parquet dataset)")
    return wr.s3.to_parquet(
        df=df,
        path=S3_CLEANSED_PATH,
        dataset=True,
        database=GLUE_DB,
        table=GLUE_TABLE,
        mode=WRITE_MODE,
    )
