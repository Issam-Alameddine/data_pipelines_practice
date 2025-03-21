import pandas as pd
import boto3
import logging
from datetime import datetime, timedelta
from config import S3_BUCKET, OHLCV_FOLDER
from data.fetch_data import fetch_ticker_data, fetch_aggs
from utils.s3_helpers import save_df_to_s3_parquet
from io import BytesIO

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")

def list_existing_keys(ticker: str, prefix: str):
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{prefix}{ticker}/")
    keys = {obj["Key"]: obj["Size"] for obj in result.get("Contents", [])}
    return keys

def run_backfill(start_date: str, end_date: str):
    tickers_data = fetch_ticker_data()
    tickers_df = pd.DataFrame(tickers_data)
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    hours = []
    while start <= end:
        for h in range(24):
            hours.append(start.replace(hour=h).strftime("%Y-%m-%d-%H"))
        start += timedelta(days=1)

    for ticker in tickers_df["ticker"]:
        logger.info(f"Checking hourly gaps for {ticker}")
        existing_keys = list_existing_keys(ticker, OHLCV_FOLDER)
        missing_hours = []

        for hour in hours:
            key = f"{OHLCV_FOLDER}{ticker}/{hour}.parquet"
            if key not in existing_keys:
                missing_hours.append(hour)
            elif existing_keys[key] < 1024:  # Heuristic: < 1 KB probably means incomplete
                logger.warning(f"⚠Incomplete file detected for {ticker} {hour}, will replace.")
                missing_hours.append(hour)

        if not missing_hours:
            logger.info(f"All data present for {ticker}")
            continue

        for hour in missing_hours:
            ts = datetime.strptime(hour, "%Y-%m-%d-%H")
            ts_start = ts.strftime("%Y-%m-%dT%H:00:00")
            ts_end = (ts + timedelta(hours=1)).strftime("%Y-%m-%dT%H:00:00")

            try:
                aggs = fetch_aggs(ticker, ts_start, ts_end, 5, "minute")
                df = pd.DataFrame([vars(a) for a in aggs])
                if not df.empty:
                    df["timestamp"] = pd.to_datetime(df["t"], unit="ms")
                    df = df.rename(columns={
                        "o": "open", "h": "high", "l": "low", "c": "close",
                        "v": "volume", "vw": "vwap", "n": "transactions"
                    })
                    df = df[["timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]]
                    key = f"{OHLCV_FOLDER}{ticker}/{hour}.parquet"
                    save_df_to_s3_parquet(df, S3_BUCKET, key)
            except Exception as e:
                logger.warning(f"Failed to backfill {ticker} {hour}: {e}")
