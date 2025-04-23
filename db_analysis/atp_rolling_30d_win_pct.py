import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from datetime import timedelta
from sqlalchemy import text
from db_connect import get_engine

# Config
engine = get_engine()
TABLE_NAME = "matched_atp_records"
WINDOW_DAYS = 30

# Load match data
print("🔍 Loading match data...")
with engine.connect() as conn:
    df = pd.read_sql(text(f"""
        SELECT matchid, date, winner_name, loser_name
        FROM {TABLE_NAME}
        WHERE comment IS NULL OR comment != 'Walkover'
        ORDER BY date ASC
    """), conn)

df["date"] = pd.to_datetime(df["date"])
player_history = {}

# Function to calculate 30-day win %
def calculate_win_pct(player, match_date):
    history = player_history.get(player, [])
    recent = [outcome for d, outcome in history if (match_date - d).days <= WINDOW_DAYS]
    if not recent:
        return 0.0
    return round(100 * sum(recent) / len(recent), 2)

results = []

# Iterate matches
print("📊 Calculating 30-day win percentages...")
for _, row in df.iterrows():
    matchid = row["matchid"]
    date = row["date"]
    winner = row["winner_name"]
    loser = row["loser_name"]

    w_pct_30d = calculate_win_pct(winner, date)
    l_pct_30d = calculate_win_pct(loser, date)

    results.append((matchid, w_pct_30d, l_pct_30d))

    # Update history AFTER calculation
    player_history.setdefault(winner, []).append((date, 1))
    player_history.setdefault(loser, []).append((date, 0))

# Update DB
print("📝 Writing results to DB...")
with engine.begin() as conn:
    conn.execute(text(f"""
        ALTER TABLE {TABLE_NAME}
        DROP COLUMN IF EXISTS f_winner_win_pct_30d,
        DROP COLUMN IF EXISTS f_loser_win_pct_30d;
    """))

    conn.execute(text(f"""
        ALTER TABLE {TABLE_NAME}
        ADD COLUMN IF NOT EXISTS f_winner_win_pct_30d NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_loser_win_pct_30d NUMERIC(5, 2);
    """))

    for matchid, w_pct_30d, l_pct_30d in results:
        conn.execute(text(f"""
            UPDATE {TABLE_NAME}
            SET f_winner_win_pct_30d = :w,
                f_loser_win_pct_30d = :l
            WHERE matchid = :id
        """), {"id": matchid, "w": w_pct_30d, "l": l_pct_30d})

print("✅ Done. Rolling 30-day win percentages updated.")
