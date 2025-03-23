import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from polygon import RESTClient
import pandas_market_calendars as mcal
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


from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

def fetch_candidate_tickers(window=20):
    """
    Fetches top 20 tickers by average notional volume over the last 'window' NYSE trading days.
    Notional volume = price * volume.
    Skips tickers with any day < 1M volume.
    """
    # Get last N trading days (excluding today)
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date="2020-01-01", end_date=datetime.today().strftime('%Y-%m-%d'))
    trading_days = sorted([d.date() for d in schedule.index if d.date() < datetime.today().date()])
    last_days = trading_days[-window:]

    tickers_list = pd.read_parquet(f"s3://{S3_BUCKET}/{DETAILS_PARQUET_KEY}")['ticker'].tolist()
    ticker_notional_volume = []

    for ticker in tqdm(tickers_list, desc="Scanning tickers"):
        notional = 0
        bail = False

        for day in last_days:
            try:
                r = client.get_daily_open_close_agg(ticker=ticker, date=day, adjusted=True)
                if r.volume < 1_000_000:
                    bail = True
                    break
                notional += r.volume * r.close
            except:
                continue

        if bail:
            continue

        avg_notional = round(notional / window)
        ticker_notional_volume.append((ticker, avg_notional))
        print(f"{ticker}: avg notional ${avg_notional:,}")

    best = sorted(ticker_notional_volume, key=lambda x: x[1], reverse=True)[:20]
    df = pd.DataFrame(best, columns=['ticker', 'avg_notional_volume'])
    df.to_csv("data/top_notional_tickers.csv", index=False)
    return df['ticker'].tolist()



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
