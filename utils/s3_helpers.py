import pandas as pd
import logging
import io
logger = logging.getLogger(__name__)
def save_df_to_s3_parquet(df: pd.DataFrame, S3_BUCKET:str, S3_KEY: str):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow")
    boto3.client("s3").put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=buffer.getvalue())
    logger.info(f"Saved to s3://{S3_BUCKET}/{S3_KEY}")

def save_ohlcv_to_s3(ticker: str, df: pd.DataFrame):
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"{OHLCV_FOLDER}{ticker}/{date_str}.parquet"
    save_df_to_s3_parquet(df, key)