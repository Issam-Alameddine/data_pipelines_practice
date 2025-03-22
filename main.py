import argparse
import logging.config
import yaml
from datetime import datetime

# --- Load logging config ---
with open("logging.yaml", "r") as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Stock data pipeline runner")
    parser.add_argument("--task", choices=["setup", "backfill", "live", "daily"], required=True)
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--candle-size", help="Default is 5")
    parser.add_argument("--time-period", help="Default is minute")
    args = parser.parse_args()

    if args.task == "setup":
        if not args.start_date or not args.end_date or not args.candle_size or not args.time_period:
            parser.error("setup requires --start-date  --end-date --candle-size and --time-period")
        from data.setup_data import run_setup
        logger.info(f"Running setup from {args.start_date} to {args.end_date}...")
        run_setup(start_date=args.start_date, end_date=args.end_date, candle_size=args.candle_size, time_period=args.time_period)

    elif args.task == "backfill":
        from data.backfill import run_backfill
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        logger.info(f"Running backfill up to {end_date}...")
        run_backfill(end_date=end_date)

    elif args.task == "live":
        logger.info("Live data streaming not yet implemented.")

    elif args.task == "daily":
        logger.info("Daily task not yet implemented.")


if __name__ == "__main__":
    main()
