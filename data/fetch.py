import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from polygon import RESTClient
from tqdm import tqdm
from utils.config import *
from utils.s3_helpers import object_exists, save_df_to_s3_parquet
import os
import pandas as pd

load_dotenv()
API_KEY = os.getenv('POLYGON_API_KEY')
client = RESTClient(API_KEY)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------#


def fetch_ticker_data(ticker_list="", market="stocks", exchange="XNAS", active=True):
    """
    Fetch data on tickers
    :param ticker_list: All tickers by default
    :param market: stocks by default (other options: crypto, fx, otc, indices)
    :param exchange: NASDAQ (XNAS) by default
    :param active: TRUE by default
    :return:
    """
    tickers = []
    if ticker_list:
        for x in ticker_list:
            for t in client.list_tickers(ticker=x, market=market, exchange=exchange, active=active, order="asc", sort="ticker"):
                tickers.append(t)
    else:
        for t in client.list_tickers(ticker="", market=market, exchange=exchange, active=active, order="asc", sort="ticker"):
            tickers.append(t)

    data_df = pd.DataFrame(tickers)
    save_df_to_s3_parquet(data_df, S3_BUCKET, TICKERS_PARQUET_KEY)

    # -------- #
    logger.info(f"Saved details to {S3_BUCKET}/{TICKERS_PARQUET_KEY}")
    # -------- #


def fetch_ticker_details(tickers):
    """
    Fetch detailed information about a given stock ticker. Check https://polygon.io/docs/rest/stocks/tickers/ticker-overview for details on response fields.
    :param tickers: Keep empty for all tickers
    :return: save details to s3
    """
    details_list = []
    for ticker in tickers:
        ticker_details = client.get_ticker_details(ticker=ticker)
        vars(ticker_details).pop('address', None)
        details_list.append(vars(ticker_details))

    ticker_details = pd.DataFrame(details_list)
    save_df_to_s3_parquet(ticker_details, bucket=S3_BUCKET, key=DETAILS_PARQUET_KEY)

    # -------- #
    logger.info(f"Saved details to {S3_BUCKET}/{DETAILS_PARQUET_KEY}")
    # -------- #


def fetch_candidate_tickers(window):
    """
    :param window: window size, in days, used to calculate the volume average
    Fetches top 10 best average volume for tickers between $1 and $2")
    :return: A list of tickers
    """
    tickers_list = pd.read_parquet(f"s3://{S3_BUCKET}/{DETAILS_PARQUET_KEY}")['ticker'].tolist()
    ticker_avg_volume = []
    for ticker in tickers_list:
        volume, price, i = 0, 0, 0
        moving_date = datetime.today().date() + timedelta(days=-window)
        while moving_date < datetime.today().date():
            try:
                r = client.get_daily_open_close_agg(ticker=ticker, date=moving_date, adjusted=True)
                volume += vars(r)['volume']
                price += vars(r)['close']
                i += 1
                moving_date += timedelta(days=1)
            except:
                moving_date += timedelta(days=1)

        if i >= 20 and 1 <= price/i <= 2:
            ticker_avg_volume.append((ticker, round(volume / i), price/i))
            print(ticker, round(volume / i), price/i)
    best_tickers = sorted(ticker_avg_volume, key=lambda x: x[1], reverse=True)[:10]
    pd.DataFrame(best_tickers, columns=['ticker', 'avg_volume', 'avg_price']).to_csv('data/best_tickers.csv', index=False)


def fetch_ohlcv(start_date, end_date, tickers=None, candle_size=1, time_period='minute'):
    """
    Fetch Historic OHLCV
    :param start_date: start_date (YYYY-MM-DD)
    :param end_date:   end_date (YYYY-MM-DD)
    :param tickers: list of tickers
    :param candle_size: value of candle size
    :param time_period: time period of candle size
    :return: saves results in parquet format in s3 partitioned by ticker, year, month, day
    """
    # ---------------------------- #
    logger.info("Fetching OHLCV...")
    # ---------------------------- #

    details_df = pd.read_parquet(f"s3://{S3_BUCKET}/{DETAILS_PARQUET_KEY}")
    details_df["list_date"] = pd.to_datetime(details_df["list_date"], errors="coerce")
    start_date, end_date = datetime.strptime(start_date, "%Y-%m-%d"), datetime.strptime(end_date, "%Y-%m-%d")

    if not tickers:
        tickers_list = details_df.query('list_date!= list_date | list_date <= @start_date')['ticker'].tolist()
    else:
        tickers_list = details_df.query('ticker == @tickers')['ticker'].tolist()

    for ticker in tqdm(tickers_list, desc='Tickers', unit='ticker'):
        current_date = start_date

        while current_date <= end_date:
            candles = []

            for a in client.list_aggs(ticker=ticker, multiplier=candle_size, timespan=time_period, from_=datetime.strftime(current_date, "%Y-%m-%d"), to=datetime.strftime(current_date, "%Y-%m-%d"), limit=50000):
                candles.append(a)

            candles_df = pd.DataFrame(candles)

            if not candles_df.empty:
                candles_df['timestamp'] = pd.to_datetime(candles_df['timestamp'], unit='ms')
                year = current_date.strftime("%Y")
                month = current_date.strftime("%m")
                day = current_date.strftime("%d")
                key = f"{OHLCV_FOLDER}/ticker={ticker}/year={year}/month={month}/day={day}"
                if not object_exists(S3_BUCKET, key):
                    save_df_to_s3_parquet(candles_df, S3_BUCKET, key)
                    # ---------------------------- #
                    logger.info(f"Saved Historic OHLCV for {ticker} on {current_date}...")
                    # ---------------------------- #
                else:
                    logger.info(f"Skipped, object already exists")
            else:
                logger.info(f"No data for {ticker} on {current_date}")

            current_date = current_date + timedelta(days=1)
