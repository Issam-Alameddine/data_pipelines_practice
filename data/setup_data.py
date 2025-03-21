import logging
import pandas as pd
import time
from tqdm import tqdm

from config import S3_BUCKET, TICKERS_PARQUET_KEY, DETAILS_PARQUET_KEY, OHLCV_FOLDER
from data.fetch_data import fetch_ticker_data, fetch_ticker_details, fetch_aggs
from utils.s3_helpers import save_df_to_s3_parquet, save_ohlcv_to_s3

logger = logging.getLogger(__name__)

YEARS_BACK = 1
CANDLE_SIZE = 5
TIME_PERIOD = "minute"
def run_setup(start_date, end_date, candle_size, time_period):

    #------------------- Fetch ticker metadata -------------------#
    logger.info('Fetching ticker metadata...')
    ticker_data = fetch_ticker_data()
    tickers_df = pd.DataFrame(ticker_data)
    logger.info(f'Retrieved {tickers_df.shape[0]} tickers.')
    save_df_to_s3_parquet(tickers_df, S3_BUCKET, TICKERS_PARQUET_KEY)

    # ------------------- Fetch ticker details -------------------#
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

    # ------------------- Fetch historical OHLCV -------------------#
    logger.info("Fetching historical OHLCV...")

    for i, t in enumerate(tickers_df['ticker']):
        try:
            aggs = fetch_aggs(ticker = t, start_date = start_date, end_date = end_date, candle_size = candle_size, time_period= time_period)
            df = pd.DataFrame([vars(a) for a in aggs])
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                df = df.rename(columns={
                    "o": "open", "h": "high", "l": "low", "c": "close",
                    "v": "volume", "vw": "vwap", "n": "transactions"
                })
                df = df[["timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]]
                save_ohlcv_to_s3(t, df, S3_BUCKET, OHLCV_FOLDER)
        except Exception as e:
            logger.warning(f"{t}: failed to fetch OHLCV data ({e})")
        time.sleep(1)

    logger.info("Finished fetching details and OHLCV. One-time setup complete.")
