import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from datetime import timedelta
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

TABLE_NAME = "matched_atp_records"

DECAY_FACTOR = 0.9  # Fatigue decays 10% per day
FATIGUE_WINDOW_DAYS = 14  # Look back 14 days
MATCH_WEIGHT = 5  # Base fatigue per match
RECOVERY_RATE = 0.8  # 20% fatigue recovery for 3+ rest days
LONG_MATCH_CUMULATIVE_MINUTES = 300  # Cumulative threshold for back-to-back penalty
BACK_TO_BACK_PENALTY = 10  # Extra fatigue for cumulative long matches

# Tournament Multipliers
TOURNAMENT_MULTIPLIERS = {
    "Grand Slam": 1.5,
    "Masters 1000": 1.3,
    "ATP 500": 1.2,
    "ATP 250": 1.0,
    "Challenger": 0.8
}

# Load match data
with engine.connect() as conn:
    df = pd.read_sql(f'SELECT matchid, date, winner_name, loser_name, minutes, comment, series FROM {TABLE_NAME}', conn)

df['date'] = pd.to_datetime(df['date'], dayfirst=True)
df = df.sort_values(by="date")

player_fatigue = {}

# -------------------------
# CALCULATE ENHANCED FATIGUE
# -------------------------
print("Calculating fatigue factors...")

def calculate_fatigue(player, current_date):
    """Calculate fatigue based on past matches within the window, incorporating recovery and tournament importance."""
    if player not in player_fatigue:
        return 0.0

    fatigue = 0.0
    updated_history = []
    recent_minutes = 0  # Track cumulative minutes within 5 days

    for past_date, minutes, tournament in player_fatigue[player]:
        days_ago = (current_date - past_date).days
        if days_ago > FATIGUE_WINDOW_DAYS:
            continue  # Skip matches outside fatigue window

        # Base fatigue calculation with decay
        tournament_factor = TOURNAMENT_MULTIPLIERS.get(tournament, 1.0)  # Default to 1.0 if missing
        match_fatigue = (minutes * tournament_factor) * (DECAY_FACTOR ** days_ago) + (MATCH_WEIGHT * tournament_factor)
        fatigue += match_fatigue

        # Recovery: If 3+ rest days, apply recovery rate to this match's fatigue
        if days_ago >= 3:
            fatigue -= match_fatigue * (1 - RECOVERY_RATE)  # Subtract the non-recovered portion

        # Track cumulative minutes for back-to-back penalty
        if days_ago <= 5:
            recent_minutes += minutes

        updated_history.append((past_date, minutes, tournament))

    # Back-to-back penalty for cumulative minutes
    if recent_minutes > LONG_MATCH_CUMULATIVE_MINUTES:
        fatigue += BACK_TO_BACK_PENALTY

    # Update player history
    player_fatigue[player] = updated_history

    return round(fatigue, 2)

updates = []

for index, row in df.iterrows():
    match_date = row["date"]
    winner = row["winner_name"]
    loser = row["loser_name"]
    matchid = row["matchid"]
    series = row["series"] if pd.notna(row["series"]) else "ATP 250"
    comment = str(row["comment"]).strip() if pd.notna(row["comment"]) else ""

    # Ignore Walkovers & Retirements (<30 min)
    if comment == "Walkover" or (comment == "Retired" and (pd.isna(row["minutes"]) or row["minutes"] < 30)):
        minutes = 0
    else:
        minutes = row["minutes"] if pd.notna(row["minutes"]) else 110  # Default match length

    if pd.isna(winner) or pd.isna(loser) or pd.isna(matchid):
        print(f"Skipping row {index}: Missing winner, loser, or matchid")
        continue

    # Calculate pre-match fatigue
    winner_fatigue = calculate_fatigue(winner, match_date)
    loser_fatigue = calculate_fatigue(loser, match_date)

    # Store for batch update
    updates.append((matchid, winner_fatigue, loser_fatigue))

    # Update player fatigue history
    if winner not in player_fatigue:
        player_fatigue[winner] = []
    if loser not in player_fatigue:
        player_fatigue[loser] = []

    player_fatigue[winner].append((match_date, minutes, series))
    player_fatigue[loser].append((match_date, minutes, series))

# -------------------------
# BULK UPDATE DATABASE WITH FATIGUE FACTORS
# -------------------------
print("Updating database with enhanced fatigue factors...")
try:
    with engine.connect() as conn:
        values_clause = ",".join([f"(:matchid_{i}, :w_fatigue_{i}, :l_fatigue_{i})" for i in range(len(updates))])
        query = text(f"""
            CREATE TEMP TABLE temp_fatigue_updates AS 
            SELECT matchid, winner_fatigue, loser_fatigue 
            FROM (VALUES {values_clause}) AS temp_table (matchid, winner_fatigue, loser_fatigue)
        """)
        params = {}
        for i, (matchid, w_fatigue, l_fatigue) in enumerate(updates):
            params[f"matchid_{i}"] = matchid
            params[f"w_fatigue_{i}"] = w_fatigue
            params[f"l_fatigue_{i}"] = l_fatigue

        conn.execute(query, params)

        conn.execute(
            text(f"""
                UPDATE {TABLE_NAME}
                SET winner_fatigue = temp_fatigue_updates.winner_fatigue,
                    loser_fatigue = temp_fatigue_updates.loser_fatigue
                FROM temp_fatigue_updates
                WHERE {TABLE_NAME}.matchid = temp_fatigue_updates.matchid
            """)
        )
        conn.commit()
    print("✅ Enhanced fatigue factors updated!")
except Exception as e:
    print(f"Error updating database with enhanced fatigue: {e}")
    raise

# -------------------------
# SCALE FATIGUE TO 0–100 (95TH PERCENTILE)
# -------------------------
print("Scaling fatigue to 0–100...")
try:
    with engine.connect() as conn:
        # Use percentile-based scaling
        result = conn.execute(text("""
            SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY winner_fatigue) AS max_fatigue
            FROM matched_atp_records
        """))
        MAX_FATIGUE_OBSERVED = result.fetchone()[0] or 100  # Default to 100 if empty
        print(f"Max fatigue observed (95th percentile): {MAX_FATIGUE_OBSERVED}")

        # Scale fatigue
        conn.execute(
            text(f"""
                UPDATE {TABLE_NAME}
                SET winner_fatigue = (winner_fatigue / :max_fatigue) * 100,
                    loser_fatigue = (loser_fatigue / :max_fatigue) * 100
            """),
            {"max_fatigue": MAX_FATIGUE_OBSERVED}
        )
        conn.commit()
    print("✅ Fatigue scaled to 0–100!")
except Exception as e:
    print(f"Error scaling fatigue: {e}")
    raise

# -------------------------
# VERIFY RESULTS
# -------------------------
print("Verifying scaled fatigue...")
with engine.connect() as conn:
    df_verify = pd.read_sql(f"""
        SELECT matchid, winner_name, winner_fatigue, loser_fatigue
        FROM {TABLE_NAME}
        ORDER BY winner_fatigue DESC
        LIMIT 5
    """, conn)
    print("Top 5 fatigue values:")
    print(df_verify)

    df_stats = pd.read_sql(f"""
        SELECT AVG(winner_fatigue) as avg_fatigue, MAX(winner_fatigue) as max_fatigue, MIN(winner_fatigue) as min_fatigue
        FROM {TABLE_NAME}
    """, conn)
    print("\nFatigue statistics:")
    print(df_stats)