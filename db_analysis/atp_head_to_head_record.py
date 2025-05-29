import sys
import os
import pandas as pd

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import text
from db_connect import get_engine

engine = get_engine()
TABLE_NAME = "matched_atp_records"

# -------------------------
# Reset the feature columns
# -------------------------
with engine.begin() as conn:
    conn.execute(text("""
        ALTER TABLE matched_atp_records
        DROP COLUMN IF EXISTS f_w_h2h_wins,
        DROP COLUMN IF EXISTS f_l_h2h_wins,          
        DROP COLUMN IF EXISTS f_w_h2h_wins_clay,
        DROP COLUMN IF EXISTS f_l_h2h_wins_clay,
        DROP COLUMN IF EXISTS f_w_h2h_wins_grass,
        DROP COLUMN IF EXISTS f_l_h2h_wins_grass,
        DROP COLUMN IF EXISTS f_w_h2h_wins_hard,
        DROP COLUMN IF EXISTS f_l_h2h_wins_hard,
        DROP COLUMN IF EXISTS f_winner_recent_matches_30d,
        DROP COLUMN IF EXISTS f_loser_recent_matches_30d;
    """))

    conn.execute(text("""
        ALTER TABLE matched_atp_records
        ADD COLUMN IF NOT EXISTS f_w_h2h_wins INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_h2h_wins INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_h2h_wins_clay INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_h2h_wins_clay INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_h2h_wins_grass INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_h2h_wins_grass INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_h2h_wins_hard INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_h2h_wins_hard INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_winner_recent_matches_30d INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_loser_recent_matches_30d INT DEFAULT 0;
    """))

# -------------------------
# Load match data (excluding walkovers)
# -------------------------
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT matchid, date, winner_name, loser_name, surface
        FROM {TABLE_NAME}
        WHERE comment IS NULL OR comment != 'Walkover'
        ORDER BY date ASC
    """, conn)

df["date"] = pd.to_datetime(df["date"])

# -------------------------
# State tracking dictionaries
# -------------------------
h2h_wins = {}  # frozenset(player1, player2) -> {wins: {p1: x, p2: y}}
h2h_surface_wins = {"Hard": {}, "Clay": {}, "Grass": {}}  # same structure
player_match_history = {}  # player -> list of (date, matchid)

# -------------------------
# Process and update
# -------------------------
print("Processing head-to-head data...")

try:
    with engine.begin() as conn:
        for _, row in df.iterrows():
            matchid = row["matchid"]
            match_date = row["date"]
            winner = row["winner_name"]
            loser = row["loser_name"]
            surface = row["surface"]

            if surface not in h2h_surface_wins:
                print(f"Skipping match {matchid} due to unknown surface: {surface}")
                continue

            # Init player history
            for player in [winner, loser]:
                player_match_history.setdefault(player, [])

            # Get 30-day match count
            winner_recent = len([d for d, _ in player_match_history[winner] if (match_date - d).days <= 30])
            loser_recent = len([d for d, _ in player_match_history[loser] if (match_date - d).days <= 30])

            # H2H keys (total and surface)
            h2h_key = frozenset([winner, loser])
            h2h_record = h2h_wins.setdefault(h2h_key, {winner: 0, loser: 0})
            
            # Get surface-specific records
            surf_record = h2h_surface_wins[surface].setdefault(h2h_key, {winner: 0, loser: 0})
            hard_record = h2h_surface_wins["Hard"].setdefault(h2h_key, {winner: 0, loser: 0})
            clay_record = h2h_surface_wins["Clay"].setdefault(h2h_key, {winner: 0, loser: 0})
            grass_record = h2h_surface_wins["Grass"].setdefault(h2h_key, {winner: 0, loser: 0})

            # Pre-match counts
            w_h2h = h2h_record[winner]
            l_h2h = h2h_record[loser]
            w_h2h_hard = hard_record[winner]
            l_h2h_hard = hard_record[loser]
            w_h2h_clay = clay_record[winner]
            l_h2h_clay = clay_record[loser]
            w_h2h_grass = grass_record[winner]
            l_h2h_grass = grass_record[loser]

            # Update the DB row with pre-match stats
            conn.execute(text(f"""
                UPDATE {TABLE_NAME}
                SET
                    f_w_h2h_wins = :w_h2h,
                    f_l_h2h_wins = :l_h2h,
                    f_w_h2h_wins_hard = :w_h2h_hard,
                    f_l_h2h_wins_hard = :l_h2h_hard,
                    f_w_h2h_wins_clay = :w_h2h_clay,
                    f_l_h2h_wins_clay = :l_h2h_clay,
                    f_w_h2h_wins_grass = :w_h2h_grass,
                    f_l_h2h_wins_grass = :l_h2h_grass,
                    f_winner_recent_matches_30d = :w_recent,
                    f_loser_recent_matches_30d = :l_recent
                WHERE matchid = :matchid
            """), {
                "matchid": matchid,
                "w_h2h": w_h2h,
                "l_h2h": l_h2h,
                "w_h2h_hard": w_h2h_hard,
                "l_h2h_hard": l_h2h_hard,
                "w_h2h_clay": w_h2h_clay,
                "l_h2h_clay": l_h2h_clay,
                "w_h2h_grass": w_h2h_grass,
                "l_h2h_grass": l_h2h_grass,
                "w_recent": winner_recent,
                "l_recent": loser_recent
            })

            # Post-match updates
            h2h_record[winner] += 1
            surf_record[winner] += 1
            player_match_history[winner].append((match_date, matchid))
            player_match_history[loser].append((match_date, matchid))

    print("✅ H2H and recent match features updated!")
except Exception as e:
    print(f"❌ Error updating head-to-head data: {e}")
    raise
