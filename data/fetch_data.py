import pandas as pd
from dotenv import load_dotenv
import os
from polygon import RESTClient

load_dotenv()
API_KEY = os.getenv('POLYGON_API_KEY')
client = RESTClient(API_KEY)
# ------------------------------------------------------------------------#

def fetch_ticker_data(ticker = "", type = "", market = "stocks", exchange = "XNAS", active = True):
    """
    Fetch data on tickers
    :param ticker: All tickers by default
    :param type: All types by default
    :param market: stocks by default (other options: crypto, fx, otc, indices)
    :param exchange: NASDAQ (XNAS) by default
    :param active: TRUE by default
    :return:
    """
    tickers = client.list_tickers(ticker = ticker, type = type, market=market, exchange=exchange, active=active)
    tickers_data = [vars(ticker) for ticker in tickers]

    return tickers_data
def fetch_ticker_details(ticker):
    """
    Fetch detailed information about a given stock ticker. Check https://polygon.io/docs/rest/stocks/tickers/ticker-overview for details on response fields.
    :param ticker:
    :return:
    """
    return client.get_ticker_details(ticker = ticker)
def fetch_aggs(ticker, start_date, end_date, candle_size=5, time_period='minute', limit=50000):
    """
    Fetch historical OHLCV data from Polygon.

    Args:
    - ticker (str): Stock ticker symbol (e.g., 'AAPL').
    - start_date (str): Start date in 'YYYY-MM-DD' format.
    - end_date (str): End date in 'YYYY-MM-DD' format.
    - candle_size (int): Interval size (e.g., 5 for 5-minute candles).
    - time_period (str): Timeframe ('minute', 'hour', 'day', etc.).
    - limit (int): Max records per request (default: 50,000).

    Returns:
    - List of aggregated OHLCV data.
    """

    aggs = []
    for a in client.list_aggs(
            ticker=ticker,
            multiplier=candle_size,
            timespan=time_period,
            from_=start_date,
            to=end_date,
            limit=limit
    ):
        aggs.append(a)

    return aggs

