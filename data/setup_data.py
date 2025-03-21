import logging
import pandas as pd
import time
from datetime import datetime, timedelta

from config import S3_BUCKET, TICKERS_PARQUET_KEY, DETAILS_PARQUET_KEY, OHLCV_FOLDER
from data.fetch_data import fetch_ticker_data, fetch_ticker_details, fetch_aggs
from utils.s3_helpers import save_df_to_s3_parquet, save_ohlcv_to_s3

logger = logging.getLogger(__name__)

YEARS_BACK = 1
CANDLE_SIZE = 5
TIME_PERIOD = "minute"
def run_setup():

    #------------------- Fetch ticker metadata -------------------#
    logger.info('Fetching ticker metadata...')
    ticker_data = fetch_ticker_data()
    tickers_df = pd.DataFrame(ticker_data)
    logger.info(f'Retrieved {tickers_df.shape[0]} tickers.')
    save_df_to_s3_parquet(tickers_df, S3_BUCKET, S3_KEY)

    # ------------------- Fetch ticker details -------------------#
    logger.info('Fetching ticker details...')
    details_list = []
    for i, t in enumerate(tickers_df['ticker']):
        try:
            detail = fetch_ticker_details(t)
            details_list.append(vars(detail))

        except Exception as e:
            logger.warning(f'{t}: Failed to fetch details ({e})')
        time.sleep(0.2)

    details_df = pd.DataFrame(details_list)
    logger.info(f'Retrieved details for {details_df.shape[0]} tickers.')
    save_df_to_s3_parquet(details_df, DETAILS_PARQUET_KEY)

    # ------------------- Fetch historical OHLCV -------------------#
    logger.info("Fetching historical OHLCV...")
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = datetime.today() - timedelta(days = 365 * YEARS_BACK).strftime('%Y-%m-%d')

    for i, t in enumerate(tickers_df['ticker']):
        try:
            aggs = fetch_aggs(t, start_date, end_date, CANDLE_SIZE, TIME_PERIOD)
            df = pd.DataFrame([vars(a) for a in aggs])
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close",
                                        "v": "volume", "vw": "vwap", "n": "transactions"})
                df = df[["timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]]
                save_ohlcv_to_s3(t, df)
        except Exception as e:
            logger.warning(f"{t}: failed to fetch OHLCV data ({e})")
        time.sleep(1)

    logger.info(f"Finished fetching details and OHLCV. One-time setup complete")