import os
import bz2
import json
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from dateutil import parser

# 👇 This version processes all year/month folders from 2015 to 2024
years = [str(y) for y in range(2015, 2025)]
months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def parse_file(filepath, market_meta):
    local_rows = []
    with bz2.open(filepath, "rt") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if obj.get("op") != "mcm":
                    continue

                pt = obj.get("pt")
                timestamp = datetime.utcfromtimestamp(pt / 1000.0)

                for mc in obj.get("mc", []):
                    market_id = mc.get("id")
                    if not market_id:
                        continue

                    md = mc.get("marketDefinition")
                    if md:
                        if market_id not in market_meta:
                            market_meta[market_id] = {
                                "market_time": None,
                                "inplay_start": None,
                                "event_name": None,
                                "runner_map": {}
                            }
                        if md.get("marketTime"):
                            market_meta[market_id]["market_time"] = parser.isoparse(md["marketTime"]).replace(tzinfo=None)
                        if md.get("eventName"):
                            market_meta[market_id]["event_name"] = md["eventName"]
                        if "runners" in md:
                            market_meta[market_id]["runner_map"] = {
                                r["id"]: r["name"] for r in md["runners"]
                            }
                        if md.get("inPlay") and market_meta[market_id]["inplay_start"] is None:
                            market_meta[market_id]["inplay_start"] = timestamp

                    for rc in mc.get("rc", []):
                        if "ltp" in rc:
                            selection_id = rc["id"]
                            ltp = rc["ltp"]
                            meta = market_meta.get(market_id, {})
                            market_time = meta.get("market_time")
                            inplay_start = meta.get("inplay_start")
                            event_name = meta.get("event_name")
                            runner_name = meta.get("runner_map", {}).get(selection_id)

                            keep = False
                            if market_time and timestamp < market_time:
                                keep = True
                            elif inplay_start and timestamp < inplay_start:
                                keep = True

                            if keep:
                                local_rows.append({
                                    "market_id": market_id,
                                    "selection_id": selection_id,
                                    "runner_name": runner_name,
                                    "event_name": event_name,
                                    "timestamp": timestamp,
                                    "ltp": ltp,
                                    "market_time": market_time,
                                    "inplay_start": inplay_start
                                })
            except Exception as e:
                print(f"⚠️ Error parsing {filepath}: {e}")
    return local_rows

def process_month(year, month):
    root_dir = f"./{year}/{month}"
    output_file = f"betfair_streaming_ltp_{year}_{month}.csv"
    all_rows = []
    market_meta = {}

    for root, _, files in os.walk(root_dir):
        for file in tqdm(files, desc=f"Parsing {year}-{month}"):
            if file.endswith(".bz2"):
                path = os.path.join(root, file)
                all_rows.extend(parse_file(path, market_meta))

    if not all_rows:
        print(f"🛑 No data found for {year}-{month}")
        return

    df = pd.DataFrame(all_rows)
    df.sort_values(["market_id", "selection_id", "timestamp"], inplace=True)
    df.to_csv(output_file, index=False)
    print(f"✅ Saved {len(df)} pre-match rows to {output_file}")

if __name__ == "__main__":
    for year in years:
        for month in months:
            process_month(year, month)
