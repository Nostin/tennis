import os
import bz2
import json
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Base directory where the year folders exist
BASE_DIRECTORY = "."  # Change if needed
OUTPUT_FILE = "betfair_opening_closing_odds.csv"

# Set the date range to extract (Example: March 24-31, 2023)
START_DATE = datetime(2023, 3, 24)
END_DATE = datetime(2023, 3, 31)

def extract_match_odds(file_path):
    """Extract MATCH_ODDS data from a given .bz2 file, tracking odds at specific time intervals."""
    market_data = {}
    market_definition_seen = False

    try:
        with bz2.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if "mc" not in data:
                        continue

                    # Get the published time from the stream data
                    published_time = datetime.fromtimestamp(data.get("pt", 0)/1000)

                    for market in data["mc"]:
                        market_id = market["id"]

                        # Extract market definition (contains event details)
                        if "marketDefinition" in market:
                            market_def = market["marketDefinition"]
                            
                            # Check if this is a tennis match market
                            if market_def.get("marketType") != "MATCH_ODDS":
                                continue

                            event_id = market_def["eventId"]
                            event_name = market_def.get("eventName", "Unknown")
                            market_time = datetime.strptime(market_def["marketTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
                            inplay = market_def.get("inPlay", False)

                            # Check if match date is within the desired range
                            if not (START_DATE <= market_time <= END_DATE):
                                continue

                            # Convert runner IDs to strings and store runner information
                            # Sort runners by name to ensure consistent runner1/runner2 assignment
                            runners = sorted([
                                {"id": str(runner["id"]), "name": runner["name"]}
                                for runner in market_def["runners"]
                            ], key=lambda x: x["name"])

                            if len(runners) != 2:
                                logging.warning(f"Unexpected number of runners ({len(runners)}) in market {market_id}")
                                continue

                            market_definition_seen = True

                            # Initialize market storage if not exists
                            if market_id not in market_data:
                                market_data[market_id] = {
                                    "event_id": event_id,
                                    "event_name": event_name,
                                    "market_time": market_time,
                                    "runnerid1": runners[0]["id"],
                                    "runnername1": runners[0]["name"],
                                    "runnerid2": runners[1]["id"],
                                    "runnername2": runners[1]["name"],
                                    "odds": {},
                                    "inplay_started": False,
                                    "market_start_time": None,
                                    "market_end_time": None
                                }
                            
                            # Update inplay status
                            market_data[market_id]["inplay_started"] = market_data[market_id]["inplay_started"] or inplay

                        # Process odds updates
                        if "rc" in market and market_id in market_data:
                            # Skip if we've already seen inplay for this market
                            if market_data[market_id]["inplay_started"]:
                                continue

                            for odds_entry in market["rc"]:
                                runner_id = str(odds_entry["id"])
                                odds = odds_entry.get("ltp")

                                if odds and runner_id in [market_data[market_id]["runnerid1"], market_data[market_id]["runnerid2"]]:
                                    # Initialize runner data if not exists
                                    if runner_id not in market_data[market_id]["odds"]:
                                        market_data[market_id]["odds"][runner_id] = {
                                            "opening": {"odds": odds, "time": published_time},
                                            "closing": {"odds": odds, "time": published_time},
                                            "all_updates": []  # Track all odds updates with timestamps
                                        }
                                        # Set market start time on first odds update
                                        if market_data[market_id]["market_start_time"] is None:
                                            market_data[market_id]["market_start_time"] = published_time
                                    else:
                                        current_odds = market_data[market_id]["odds"][runner_id]
                                        current_odds["all_updates"].append({
                                            "time": published_time,
                                            "odds": odds
                                        })
                                        
                                        # Update opening odds if this is earlier than current opening
                                        if published_time < current_odds["opening"]["time"]:
                                            current_odds["opening"] = {"odds": odds, "time": published_time}
                                        
                                        # Update closing odds if this is later than current closing
                                        if published_time > current_odds["closing"]["time"]:
                                            current_odds["closing"] = {"odds": odds, "time": published_time}
                                            market_data[market_id]["market_end_time"] = published_time

                except json.JSONDecodeError as e:
                    logging.warning(f"JSON decode error in file {file_path}: {str(e)}")
                    continue
                except Exception as e:
                    logging.error(f"Error processing line in {file_path}: {str(e)}")
                    continue

    except Exception as e:
        logging.error(f"Error opening file {file_path}: {str(e)}")
        return {}

    if not market_definition_seen:
        logging.warning(f"No valid market definition found in {file_path}")
        return {}

    return market_data

def get_odds_at_time_percentage(updates, start_time, end_time, percentage):
    """Get the odds closest to a specific percentage of the market duration."""
    if not updates or not start_time or not end_time:
        return None
    
    total_duration = (end_time - start_time).total_seconds()
    target_time = start_time + pd.Timedelta(seconds=total_duration * percentage)
    
    # Find the update closest to the target time
    closest_update = min(updates, key=lambda x: abs((x["time"] - target_time).total_seconds()))
    return closest_update["odds"]

def process_one_week(base_directory):
    """Walk through the directory and process one week's worth of .bz2 files."""
    all_odds_records = []
    processed_files = 0
    skipped_files = 0

    for year in os.listdir(base_directory):
        year_path = os.path.join(base_directory, year)
        if not os.path.isdir(year_path) or not year.isdigit():
            continue

        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue

            for day in os.listdir(month_path):
                day_path = os.path.join(month_path, day)
                if not os.path.isdir(day_path):
                    continue

                try:
                    folder_date = datetime(int(year), int(datetime.strptime(month, "%b").month), int(day))
                except ValueError:
                    continue

                if START_DATE <= folder_date <= END_DATE:
                    for match_id in os.listdir(day_path):
                        match_path = os.path.join(day_path, match_id)
                        if not os.path.isdir(match_path):
                            continue

                        for filename in os.listdir(match_path):
                            if filename.endswith(".bz2"):
                                file_path = os.path.join(match_path, filename)
                                logging.info(f"Processing: {file_path}")
                                
                                market_data = extract_match_odds(file_path)
                                
                                if market_data:
                                    processed_files += 1
                                    for market_id, data in market_data.items():
                                        # Create base record with common data
                                        base_record = {
                                            "event_id": data["event_id"],
                                            "event_name": data["event_name"],
                                            "date": data["market_time"].strftime("%Y-%m-%d"),
                                            "time_utc": data["market_time"].strftime("%H:%M:%S"),
                                            "runnerid1": data["runnerid1"],
                                            "runnername1": data["runnername1"],
                                            "runnerid2": data["runnerid2"],
                                            "runnername2": data["runnername2"]
                                        }

                                        # Process odds for each runner
                                        for runner_id, odds in data["odds"].items():
                                            record = base_record.copy()
                                            is_runner1 = runner_id == data["runnerid1"]
                                            record["runner"] = data["runnername1"] if is_runner1 else data["runnername2"]
                                            record["runner_id"] = runner_id
                                            record["is_runner1"] = is_runner1  # Add flag to show which runner these odds are for

                                            # Get odds at different time points
                                            odds_25pct = get_odds_at_time_percentage(
                                                odds["all_updates"],
                                                data["market_start_time"],
                                                data["market_end_time"],
                                                0.25
                                            )
                                            odds_50pct = get_odds_at_time_percentage(
                                                odds["all_updates"],
                                                data["market_start_time"],
                                                data["market_end_time"],
                                                0.50
                                            )
                                            odds_75pct = get_odds_at_time_percentage(
                                                odds["all_updates"],
                                                data["market_start_time"],
                                                data["market_end_time"],
                                                0.75
                                            )

                                            record.update({
                                                "opening_odds": round(odds["opening"]["odds"], 3),
                                                "odds_25pct": round(odds_25pct, 3) if odds_25pct else None,
                                                "odds_50pct": round(odds_50pct, 3) if odds_50pct else None,
                                                "odds_75pct": round(odds_75pct, 3) if odds_75pct else None,
                                                "closing_odds": round(odds["closing"]["odds"], 3),
                                                "num_updates": len(odds["all_updates"])
                                            })
                                            all_odds_records.append(record)
                                else:
                                    skipped_files += 1

    logging.info(f"Processed {processed_files} files successfully")
    logging.info(f"Skipped {skipped_files} files")

    # Save extracted data to a CSV
    if all_odds_records:
        df = pd.DataFrame(all_odds_records)
        df.to_csv(OUTPUT_FILE, index=False)
        logging.info(f"Saved {len(all_odds_records)} records to {OUTPUT_FILE}")
    else:
        logging.warning("No records were extracted")

# Run extraction for one week
process_one_week(BASE_DIRECTORY)
