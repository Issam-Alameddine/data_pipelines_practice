import pandas as pd
import boto3
import io
import logging

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")

def save_df_to_s3_parquet(df: pd.DataFrame, bucket: str, key: str):
    """Save a DataFrame to S3 in Parquet format."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Saved to s3://{bucket}/{key}")

def save_ohlcv_to_s3(ticker: str, df: pd.DataFrame, s3_bucket: str, ohlcv_folder: str):
    """
    Partition and save OHLCV data by hour based on timestamp column.
    """
    df["datetime"] = pd.to_datetime(df["timestamp"])
    df["hour_partition"] = df["datetime"].dt.strftime("%Y-%m-%d-%H")

    for hour_str in df["hour_partition"].unique():
        hourly_df = df[df["hour_partition"] == hour_str].drop(columns=["hour_partition"])
        key = f"{ohlcv_folder}{ticker}/{hour_str}.parquet"
        save_df_to_s3_parquet(hourly_df, s3_bucket, key)
