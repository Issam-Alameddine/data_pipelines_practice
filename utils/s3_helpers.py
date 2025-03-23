import pandas as pd
import boto3
import io
import logging

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


def object_exists(bucket: str, key: str) -> bool:
    """Check if an object exists in S3."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        raise  # Propagate other errors


def save_df_to_s3_parquet(df: pd.DataFrame, bucket: str, key: str):
    """Save a DataFrame to S3 in Parquet format."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Saved to s3://{bucket}/{key}")

