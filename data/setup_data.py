import logging
import pandas as pd
import time
from tqdm import tqdm
from datetime import datetime, timedelta
from config import S3_BUCKET, TICKERS_PARQUET_KEY, DETAILS_PARQUET_KEY, OHLCV_FOLDER
from data.fetch_data import fetch_ticker_data, fetch_ticker_details, fetch_aggs
from utils.s3_helpers import object_exists, save_df_to_s3_parquet, save_ohlcv_to_s3

logger = logging.getLogger(__name__)

YEARS_BACK = 1
CANDLE_SIZE = 5
TIME_PERIOD = "minute"
def run_setup(start_date, end_date, candle_size, time_period):

    #------------------- Fetch ticker metadata -------------------#
    logger.info('Fetching ticker metadata...')
    ticker_data = fetch_ticker_data()
    tickers_df = pd.DataFrame(ticker_data)

    #-#-#-#-#-#-#-#-#-#-#-#-
    # tickers_df = pd.read_csv("tickers.csv")
    #-#-#-#-#-#-#-#-#-#-#-#-

    logger.info(f'Retrieved {tickers_df.shape[0]} tickers.')
    save_df_to_s3_parquet(tickers_df, S3_BUCKET, TICKERS_PARQUET_KEY)

    # # ------------------- Fetch ticker details -------------------#
    from tqdm import tqdm

    logger.info('Fetching ticker details...')
    details_list = []

    def flatten_detail(detail):
        flat = {}
        for k, v in vars(detail).items():
            if hasattr(v, '__dict__'):
                flat[k] = vars(v)  # Convert nested objects like address
            else:
                flat[k] = v
        return flat

    for t in tqdm(tickers_df['ticker'], desc="Tickers", unit="tickers"):
        try:
            detail = fetch_ticker_details(t)
            flat_detail = flatten_detail(detail)
            details_list.append(flat_detail)
        except Exception as e:
            logger.warning(f'{t}: Failed to fetch details ({e})')
        time.sleep(0.2)

    details_df = pd.DataFrame(details_list)
    logger.info(f"Retrieved ticker details for {len(details_list)} out of {len(tickers_df)} tickers.")
    save_df_to_s3_parquet(details_df, S3_BUCKET, DETAILS_PARQUET_KEY)
    #-#-#-#-#-#-#-
    # details_df = pd.read_parquet(f"s3://{S3_BUCKET}/{DETAILS_PARQUET_KEY}")
    #-#-#-#-#-#-#-
    # ------------------- Fetch historical OHLCV -------------------#
    logger.info("Fetching historical OHLCV...")


    # Filter out tickers that IPO'd after the requested start_date
    details_df["list_date"] = pd.to_datetime(details_df["list_date"], errors="coerce")
    filtered_tickers_df = details_df[
        (details_df["list_date"].notna()) &
        (details_df["list_date"] <= pd.to_datetime(start_date))
        ].copy()

    tickers = filtered_tickers_df["ticker"].tolist()
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1

    ticker_bar = tqdm(tickers, desc="Tickers", unit="ticker")

    for ticker in ticker_bar:
        current = start
        day_idx = 0

        while current <= end:
            day_str = current.strftime("%Y-%m-%d")
            print(ticker, day_str, (current + timedelta(days=1)).strftime("%Y-%m-%d"), candle_size, time_period)
            try:
                aggs = fetch_aggs(
                    ticker=ticker,
                    start_date=day_str,
                    end_date=(current + timedelta(days=1)).strftime("%Y-%m-%d"),
                    candle_size=candle_size,
                    time_period=time_period,
                )
                df = pd.DataFrame([vars(a) for a in aggs])
                if not df.empty:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                    df["hour_partition"] = df["timestamp"].dt.strftime("%Y-%m-%d-%H")

                    for hour_str in df["hour_partition"].unique():
                        hourly_df = df[df["hour_partition"] == hour_str].drop(columns="hour_partition")

                        dt = pd.to_datetime(hour_str, format="%Y-%m-%d-%H")
                        year = dt.strftime("%Y")
                        month = dt.strftime("%m")
                        day = dt.strftime("%d")
                        hour = dt.strftime("%H")

                        # Example: historical_ohlcv/AAPL/year=2025/month=03/day=21/hour=14/AAPL_2025032114.parquet
                        key = (
                            f"historical_ohlcv/ticker={ticker}/"
                            f"year={year}/month={month}/day={day}/hour={hour}/"
                            f"{ticker}_{year}{month}{day}{hour}.parquet"
                        )

                        if not object_exists(S3_BUCKET, key):
                            save_df_to_s3_parquet(hourly_df, S3_BUCKET, key)

            except Exception as e:
                logger.warning(f"{ticker} {day_str}: failed to fetch OHLCV ({e})")

            current += timedelta(days=1)
            day_idx += 1
            ticker_bar.set_postfix_str(f"{ticker} - Day {day_idx}/{total_days}")
            time.sleep(0.5)

    logger.info("Finished fetching and saving historical OHLCV data partitioned by hour.")