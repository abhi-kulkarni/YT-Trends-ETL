from pipelines.youtube.shared.utils import parse_s3_event, read_json_from_s3, transform, write_to_parquet
import structlog

logger = structlog.get_logger(__name__)

def lambda_handler(event, context):
    try:
        bucket, key = parse_s3_event(event)

        df_raw = read_json_from_s3(bucket, key)
        df = transform(df_raw)

        response = write_to_parquet(df)

        logger.info("Job completed successfully")
        return {
            "statusCode": 200,
            "body": response,
        }

    except Exception as e:
        logger.exception(f"Failed processing S3 object")
        raise