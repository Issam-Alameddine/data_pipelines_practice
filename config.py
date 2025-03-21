# AWS S3 Config

S3_BUCKET = "your-s3-bucket-name"
TICKERS_PARQUET_KEY = "nasdaq/tickers_metadata.parquet"
DETAILS_PARQUET_KEY = "nasdaq/tickers_details.parquet"
OHLCV_FOLDER = "nasdaq/historical_ohlcv/"  # partitioned by ticker
YEARS_BACK = 1
CANDLE_SIZE = 5
TIME_PERIOD = "minute"