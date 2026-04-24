import sys
from awsglue.transforms import ApplyMapping, ResolveChoice, DropNullFields
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pipelines.youtube.shared.consts import ARGS, CSV_OPTIONS, MAPPINGS, PREDICATE, INPUT_PATH, OUTPUT_PATH
import structlog

logger = structlog.get_logger(__name__)

ARGS = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Init
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session

job = Job(glue_ctx)
job.init(ARGS["JOB_NAME"], ARGS)

# Extract
def read_raw_data():
    return glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options=CSV_OPTIONS,
        connection_options={
            "paths": [INPUT_PATH],
            "recurse": True,
        },
        push_down_predicate=PREDICATE,
    )

# Transform
def transform(df):
    df = ApplyMapping.apply(frame=df, mappings=MAPPINGS)
    df = ResolveChoice.apply(frame=df, choice="make_struct")
    df = DropNullFields.apply(frame=df)
    return df

# Load
def write_output(df):
    # Avoid coalesce(1) unless absolutely required (can hurt performance)
    return glue_ctx.write_dynamic_frame.from_options(
        frame=df,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": OUTPUT_PATH,
            "partitionKeys": ["region"],
        },
    )

# Pipeline
raw_df = read_raw_data()
processed_df = transform(raw_df)

write_output(processed_df)

job.commit()
