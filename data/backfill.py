import boto3
import pandas_market_calendars as mcal
import re
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import pandas as pd
import os

from utils.config import *
from data.fetch_stocks import fetch_ohlcv

logger = logging.getLogger(__name__)

def missing_dates_check(auto_backfill: bool = False):
    bucket_name = f'{S3_BUCKET}'
    prefix = f'{OHLCV_FOLDER}/'

    # --- NYSE trading calendar ---
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date='2025-01-01', end_date=datetime.today().strftime('%Y-%m-%d'))
    valid_trading_days = set(schedule.index.date)
    latest_trading_day = max(d for d in valid_trading_days if d < datetime.today().date())

    # --- Load previous backfill report (if exists) ---
    previous_checked = {}
    previous_missing = defaultdict(set)

    if os.path.exists("logs/backfill_report.csv"):
        old_df = pd.read_csv("logs/backfill_report.csv")
        for _, row in old_df.iterrows():
            ticker = row['ticker']
            try:
                last_checked = datetime.strptime(str(row['last_checked']), "%Y-%m-%d").date()
                previous_checked[ticker] = last_checked
            except Exception as e:
                logger.warning(f"Couldn't parse last_checked for {ticker}: {e}")
            try:
                if pd.notna(row['missing_dates']):
                    for d in str(row['missing_dates']).split(','):
                        previous_missing[ticker].add(datetime.strptime(d.strip(), "%Y-%m-%d").date())
            except Exception as e:
                logger.warning(f"Couldn't parse missing_dates for {ticker}: {e}")

    # --- Scan S3 for partitioned dates ---
    s3 = boto3.client('s3')
    pattern = re.compile(r'ticker=(?P<ticker>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})')
    partitions = defaultdict(set)

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            match = pattern.search(key)
            if match:
                ticker = match.group('ticker')
                date_str = f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
                partitions[ticker].add(date_str)

    results = []
    tickers_to_backfill = []

    for ticker, date_strs in partitions.items():
        date_objs = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in date_strs)
        if not date_objs:
            continue

        data_min_date = date_objs[0]

        previously_missing = previous_missing.get(ticker, set())

        last_checked = previous_checked.get(ticker)

        if last_checked:
            post_checked_days = [d for d in valid_trading_days if d > last_checked and d <= latest_trading_day]
            logger.info(f"Checking {ticker} from last_checked={last_checked} and {len(previously_missing)} previously missing days")
        else:
            post_checked_days = [d for d in valid_trading_days if d >= data_min_date and d <= latest_trading_day]
            logger.info(f"Checking {ticker} from beginning ({data_min_date})")

        expected_days = sorted(set(post_checked_days).union(previously_missing))
        actual_days = set(date_objs)
        missing_days = sorted(d for d in expected_days if d not in actual_days)

        results.append({
            'ticker': ticker,
            'last_checked': latest_trading_day.strftime("%Y-%m-%d"),
            'missing_count': len(missing_days),
            'missing_dates': ', '.join(d.strftime("%Y-%m-%d") for d in missing_days) if missing_days else ''
        })

        if auto_backfill and missing_days:
            for missing_date in missing_days:
                tickers_to_backfill.append((ticker, missing_date))

    # --- Save updated backfill report ---
    df = pd.DataFrame(results)
    df = df.sort_values(by='missing_count', ascending=False)
    df.to_csv("logs/backfill_report.csv", index=False)
    logger.info("Saved updated backfill_report.csv")

    # --- Backfill missing days one-by-one ---
    if auto_backfill:
        for ticker, missing_date in tickers_to_backfill:
            logger.info(f"Backfilling {ticker} on {missing_date}")
            fetch_ohlcv(
                start_date=missing_date.strftime("%Y-%m-%d"),
                end_date=missing_date.strftime("%Y-%m-%d"),
                tickers=[ticker],
                candle_size=1,
                time_period='minute'
            )

