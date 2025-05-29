import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

engine = get_engine()

with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS betfair_odds;"))

CSV_DIR = "spreadsheet_raw"  # change if needed
YEARS = range(2016, 2025)
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

all_dataframes = []
total_rows = 0

for year in YEARS:
    for month in MONTHS:
        file_path = os.path.join(CSV_DIR, f"betfair_intervals_{year}_{month}.csv")
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            continue

        print(f"📂 Reading: {file_path}")
        df = pd.read_csv(file_path, parse_dates=["timestamp", "market_time", "inplay_start"])
        df["record_id"] = df["market_id"].astype(str) + "_" + df["selection_id"].astype(str)

        df_pivot = df.pivot_table(
            index=["record_id", "market_id", "selection_id", "runner_name", "event_name", "market_time"],
            columns="interval_pct",
            values="ltp"
        ).reset_index()

        df_pivot.columns.name = None
        df_pivot.rename(columns={i: f"odds_{i:02}pct" for i in range(0, 101, 10) if i in df_pivot.columns}, inplace=True)

        for i in range(0, 101, 10):
            col = f"odds_{i:02}pct"
            if col not in df_pivot.columns:
                df_pivot[col] = None

        df_pivot["event_id"] = df_pivot["market_id"]
        df_pivot["date"] = pd.to_datetime(df_pivot["market_time"]).dt.date
        df_pivot["time_utc"] = pd.to_datetime(df_pivot["market_time"]).dt.time
        df_pivot["odds_player_name"] = df_pivot["runner_name"]
        df_pivot["odds_player_id"] = df_pivot["selection_id"]
        df_pivot["num_updates"] = 11

        for col in [
            "player1_name", "player1_name_normalised", "player1_id",
            "player2_name", "player2_name_normalised", "player2_id",
            "odds_player_name_normalised"
        ]:
            df_pivot[col] = None

        all_dataframes.append(df_pivot)
        total_rows += len(df_pivot)
        print(f"✅ Pivoted {len(df_pivot)} rows from {file_path}")

print(f"\n🔄 Merging {len(all_dataframes)} dataframes ({total_rows:,} rows)...")
full_df = pd.concat(all_dataframes)
merged_df = full_df.groupby("record_id").agg(lambda x: x.ffill().bfill().iloc[0] if x.notnull().any() else None)
merged_df.reset_index(inplace=True)

ordered_cols = [
    "record_id", "event_id", "event_name", "date", "time_utc",
    "player1_name", "player1_name_normalised", "player1_id",
    "player2_name", "player2_name_normalised", "player2_id",
    "odds_player_name", "odds_player_name_normalised", "odds_player_id"
] + [f"odds_{i:02}pct" for i in range(0, 101, 10)] + ["num_updates"]

print(f"📝 Writing {len(merged_df):,} rows to database...")
merged_df[ordered_cols].to_sql("betfair_odds", engine, if_exists="append", index=False)

print("✅ Done importing all CSVs.")

print("🔄 Updating player1/player2 names in database...")
with engine.begin() as conn:
    conn.execute(text("""
        UPDATE betfair_odds SET
            player1_name = NULL,
            player2_name = NULL,
            player1_id = NULL,
            player2_id = NULL;
    """))

    conn.execute(text("""
        WITH ranked_players AS (
            SELECT event_id, odds_player_name, odds_player_id,
                   ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY odds_player_id) AS rn
            FROM betfair_odds
            GROUP BY event_id, odds_player_name, odds_player_id
        )
        UPDATE betfair_odds AS b
        SET
            player1_name = CASE WHEN r.rn = 1 THEN r.odds_player_name ELSE b.player1_name END,
            player1_id   = CASE WHEN r.rn = 1 THEN r.odds_player_id::text ELSE b.player1_id END,
            player2_name = CASE WHEN r.rn = 2 THEN r.odds_player_name ELSE b.player2_name END,
            player2_id   = CASE WHEN r.rn = 2 THEN r.odds_player_id::text ELSE b.player2_id END
        FROM ranked_players r
        WHERE b.event_id = r.event_id AND b.odds_player_id = r.odds_player_id;
    """))

    conn.execute(text("""
        UPDATE betfair_odds AS b
        SET
            player1_name = COALESCE(b.player1_name, o.player1_name),
            player1_id   = COALESCE(b.player1_id, o.player1_id),
            player2_name = COALESCE(b.player2_name, o.player2_name),
            player2_id   = COALESCE(b.player2_id, o.player2_id)
        FROM (
            SELECT event_id,
                   MAX(player1_name) AS player1_name,
                   MAX(player1_id) AS player1_id,
                   MAX(player2_name) AS player2_name,
                   MAX(player2_id) AS player2_id
            FROM betfair_odds
            GROUP BY event_id
        ) o
        WHERE b.event_id = o.event_id
          AND (b.player1_name IS DISTINCT FROM o.player1_name OR b.player2_name IS DISTINCT FROM o.player2_name);
    """))

print("✅ Player names updated.")