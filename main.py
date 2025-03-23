
import logging.config
import yaml

with open("logs/logging.yaml", "r") as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger(__name__)

from data.fetch import *
from data.backfill import missing_dates_check

if __name__ == "__main__":

    # fetch_ticker_details()
    # fetch_ticker_data()
    # fetch_ohlcv()

    # can make this run once a week for example
    # best_tickers = pd.read_csv('./data/best_tickers.csv')['ticker'].tolist()
    # fetch_ohlcv(start_date='2025-03-01', end_date='2025-03-20', tickers=best_tickers)
    # ----------------------------------------#
    # for backfilling:
    # missing_dates_check(True)
    pass