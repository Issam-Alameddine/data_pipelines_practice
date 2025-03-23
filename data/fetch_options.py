import logging
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient
from tqdm import tqdm
import os
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv('POLYGON_API_KEY')
client = RESTClient(API_KEY)


def tag_runners_with_options(csv_path="data/historical_runners.csv", save_path="data/tagged_runners_with_options.csv"):
    df = pd.read_csv(csv_path, parse_dates=["date"])
    tagged_rows = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Tagging options"):
        ticker = row['ticker']
        spike_date = row['date'].date()
        open_price = row['open']
        expiry_cutoff = spike_date + timedelta(days=14)

        try:
            contracts = client.get_option_contracts(ticker=ticker, as_of=spike_date.isoformat(), limit=1000)
        except Exception as e:
            logger.info(f"Failed to get contracts for {ticker} on {spike_date}: {e}")
            continue

        total_vol = 0
        put_vol = 0
        call_vol = 0
        strike_diffs = []
        max_contract_vol = 0
        contract_count = 0

        for contract in contracts.results:
            try:
                exp_date = datetime.strptime(contract.expiration_date, "%Y-%m-%d").date()
                if exp_date > expiry_cutoff:
                    continue

                aggs = client.get_aggs(
                    ticker=contract.ticker,
                    multiplier=1,
                    timespan="day",
                    from_=spike_date.isoformat(),
                    to=spike_date.isoformat(),
                    limit=1
                )
                if not aggs:
                    continue

                vol = aggs[0].volume or 0
                strike = contract.strike_price or 0
                contract_type = contract.type

                if contract_type == "put":
                    put_vol += vol
                elif contract_type == "call":
                    call_vol += vol

                total_vol += vol
                contract_count += 1
                max_contract_vol = max(max_contract_vol, vol)

                if open_price > 0:
                    strike_diff_pct = abs(strike - open_price) / open_price * 100
                    strike_diffs.append(strike_diff_pct)

            except Exception:
                continue

        avg_strike_distance = round(sum(strike_diffs) / len(strike_diffs), 2) if strike_diffs else None
        call_put_ratio = round(call_vol / put_vol, 2) if put_vol > 0 else None

        tagged_rows.append({
            **row,
            "total_options_volume": total_vol,
            "call_put_ratio": call_put_ratio,
            "num_contracts": contract_count,
            "avg_strike_distance_pct": avg_strike_distance,
            "max_contract_volume": max_contract_vol
        })

    tagged_df = pd.DataFrame(tagged_rows)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    tagged_df.to_csv(save_path, index=False)
    print(f"Saved {len(tagged_df)} tagged runners to {save_path}")
    return tagged_df
