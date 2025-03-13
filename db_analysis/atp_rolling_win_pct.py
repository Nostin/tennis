import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from datetime import timedelta
from db_connect import get_engine

# Get the database engine
engine = get_engine()

# -------------------------
# CONFIGURATION
# -------------------------
TABLE_NAME = "matched_atp_records"
WINDOW_3M = 90  # 3 months in days
WINDOW_6M = 180  # 6 months in days

# Load match data from the database
with engine.connect() as conn:
    df = pd.read_sql(f'SELECT matchid, date, winner_name, loser_name FROM {TABLE_NAME}', conn)

# Convert Date column to datetime and sort matches chronologically
df['date'] = pd.to_datetime(df['date'], dayfirst=True)
df = df.sort_values(by="date")

# Dictionary to track player match history with dates and outcomes (1 = win, 0 = loss)
player_history = {}

# Dictionary to track the last match date for each player
last_match_date = {}

# -------------------------
# CALCULATE ROLLING WIN PERCENTAGE
# -------------------------
print("Calculating rolling win percentages...")

def calculate_win_pct(player, current_date, history, window_days):
    """Calculate win percentage over the specified window."""
    if player not in history or not history[player]:
        return 0.0  # Default to 0% if no history
    
    matches = [(d, outcome) for d, outcome in history[player] if (current_date - d).days <= window_days]
    if not matches:
        return 0.0
    
    wins = sum(outcome for _, outcome in matches)
    total = len(matches)
    return round((wins / total) * 100, 2)  # Return as percentage (0-100)

updated_rows = []

for index, row in df.iterrows():
    match_date = row["date"]
    winner = row["winner_name"]
    loser = row["loser_name"]
    matchid = row["matchid"]

    # Validate data
    if pd.isna(winner) or pd.isna(loser) or pd.isna(matchid):
        print(f"Skipping row {index}: Missing winner, loser, or matchid")
        continue

    # Initialize player history if not present
    if winner not in player_history:
        player_history[winner] = []
    if loser not in player_history:
        player_history[loser] = []

    # Calculate pre-match win percentages for 3 and 6 months
    winner_win_pct_3m = calculate_win_pct(winner, match_date, player_history, WINDOW_3M)
    winner_win_pct_6m = calculate_win_pct(winner, match_date, player_history, WINDOW_6M)
    loser_win_pct_3m = calculate_win_pct(loser, match_date, player_history, WINDOW_3M)
    loser_win_pct_6m = calculate_win_pct(loser, match_date, player_history, WINDOW_6M)

    # Calculate days since last match
    winner_days_since_last = (match_date - last_match_date.get(winner, match_date)).days
    loser_days_since_last = (match_date - last_match_date.get(loser, match_date)).days

    # Store last match date for next iterations
    last_match_date[winner] = match_date
    last_match_date[loser] = match_date

    # Store for database update
    updated_rows.append((matchid, winner_win_pct_3m, winner_win_pct_6m, loser_win_pct_3m, loser_win_pct_6m, winner_days_since_last, loser_days_since_last))

    # Update player history with current match (1 for winner, 0 for loser)
    player_history[winner].append((match_date, 1))
    player_history[loser].append((match_date, 0))

# -------------------------
# UPDATE DATABASE WITH WIN PERCENTAGES
# -------------------------
print("Updating database with rolling win percentages...")
try:
    with engine.connect() as conn:
        for matchid, w_pct_3m, w_pct_6m, l_pct_3m, l_pct_6m, w_days_since_last, l_days_since_last in updated_rows:
            update_query = text(f"""
                UPDATE {TABLE_NAME}
                SET winner_win_pct_3m = :w_pct_3m, winner_win_pct_6m = :w_pct_6m,
                    loser_win_pct_3m = :l_pct_3m, loser_win_pct_6m = :l_pct_6m,
                    winner_days_since_last = :w_days_since_last,
                    loser_days_since_last = :l_days_since_last
                WHERE matchid = :matchid
            """)
            conn.execute(update_query, {
                "matchid": matchid,
                "w_pct_3m": w_pct_3m,
                "w_pct_6m": w_pct_6m,
                "l_pct_3m": l_pct_3m,
                "l_pct_6m": l_pct_6m,
                "w_days_since_last": w_days_since_last,
                "l_days_since_last": l_days_since_last
            })
        conn.commit()
    print("✅ Rolling win percentages successfully updated in the database!")
except Exception as e:
    print(f"Error updating database: {e}")
    raise