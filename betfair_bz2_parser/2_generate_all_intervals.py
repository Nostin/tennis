import pandas as pd
import numpy as np
import os

YEARS = list(range(2016, 2025))
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

for year in YEARS:
    for month in MONTHS:
        input_file = f"betfair_streaming_ltp_{year}_{month}.csv"
        output_file = f"betfair_intervals_{year}_{month}.csv"

        if not os.path.exists(input_file):
            print(f"⏭️ Skipping missing file: {input_file}")
            continue

        print(f"⚙️ Processing {input_file}...")

        df = pd.read_csv(input_file, parse_dates=["timestamp", "market_time", "inplay_start"])
        df.dropna(subset=["ltp", "timestamp", "market_time"], inplace=True)
        df.sort_values(["market_id", "selection_id", "timestamp"], inplace=True)

        records = []
        grouped = df.groupby(["market_id", "selection_id"])

        for (market_id, selection_id), group in grouped:
            group = group.copy()
            group = group.sort_values("timestamp").reset_index(drop=True)

            if group.empty:
                continue

            market_time = group["market_time"].iloc[0]
            first_timestamp = group["timestamp"].iloc[0]
            duration = (market_time - first_timestamp).total_seconds()

            if duration <= 0 or duration != duration:  # second check guards against NaT math producing NaN
                if len(group) == 1:
                    row = group.iloc[0]
                    records.append({
                        "market_id": market_id,
                        "selection_id": selection_id,
                        "runner_name": row.get("runner_name"),
                        "event_name": row.get("event_name"),
                        "interval_pct": 0,
                        "timestamp": row["timestamp"],
                        "ltp": row["ltp"],
                        "market_time": row["market_time"],
                        "inplay_start": row.get("inplay_start"),
                    })
                    print(f"⚠️ Rescued single-row market: {market_id}, {selection_id}")
                else:
                    print(f"❌ Skipped zero-duration but multi-row market: {market_id}, {selection_id}")
                continue

            group["elapsed_pct"] = ((group["timestamp"] - first_timestamp).dt.total_seconds() / duration * 100).clip(0, 100)
            group["bucket"] = np.floor(group["elapsed_pct"] / 10) * 10
            group["bucket"] = group["bucket"].clip(0, 100).astype(int)

            bucketed = group.drop_duplicates(subset=["bucket"], keep="last")

            if bucketed.empty:
                print(f"❌ No valid buckets: {market_id}, {selection_id}")
                continue

            for _, row in bucketed.iterrows():
                records.append({
                    "market_id": market_id,
                    "selection_id": selection_id,
                    "runner_name": row.get("runner_name"),
                    "event_name": row.get("event_name"),
                    "interval_pct": row["bucket"],
                    "timestamp": row["timestamp"],
                    "ltp": row["ltp"],
                    "market_time": row["market_time"],
                    "inplay_start": row.get("inplay_start"),
                })

        interval_df = pd.DataFrame(records)
        interval_df.to_csv(output_file, index=False)

        print(f"✅ Saved {len(interval_df)} interval rows to {output_file}")
