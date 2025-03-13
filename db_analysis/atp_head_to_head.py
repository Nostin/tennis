import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

TABLE_NAME = "matched_atp_records"

# Load match data, excluding walkovers
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT matchid, date, winner_name, loser_name, surface, score
        FROM {TABLE_NAME}
        WHERE comment != 'Walkover' OR comment IS NULL
    """, conn)

# Convert Date column to datetime and sort matches in chronological order
df['date'] = pd.to_datetime(df['date'], dayfirst=True)
df = df.sort_values(by="date")

# Dictionaries to track head-to-head wins, match history, and tie-breaks
h2h_wins = {}  # Overall H2H: (winner, loser) -> count
h2h_surface_wins = {"Hard": {}, "Clay": {}, "Grass": {}}  # Surface-specific H2H
player_match_history = {}  # Player -> [(date, matchid)] for recent match count
player_tb_history = {}  # Player -> [(won, total)] for tie-break win %

# -------------------------
# ITERATE THROUGH MATCHES & UPDATE DATABASE
# -------------------------
print("Processing matches row by row...")

def parse_tiebreaks(score):
    """Parse score for tie-breaks: returns (winner_tb_won, loser_tb_won)."""
    if pd.isna(score):
        return 0, 0
    sets = score.split()
    winner_tb = sum(1 for s in sets if "7-6" in s)  # Winner won tie-break
    loser_tb = sum(1 for s in sets if "6-7" in s)   # Loser won tie-break
    return winner_tb, loser_tb

def calc_tb_win_pct(history):
    """Calculate rolling tie-break win % from last 10 tie-breaks."""
    if not history:
        return 0.0
    recent = history[-10:]  # Last 10 tie-breaks
    won = sum(w for w, _ in recent)
    total = sum(t for _, t in recent)
    return round((won / total * 100) if total > 0 else 0.0, 2)

try:
    with engine.connect() as conn:
        for index, row in df.iterrows():
            winner = row["winner_name"]
            loser = row["loser_name"]
            matchid = row["matchid"]
            match_date = row["date"]
            surface = row["surface"]
            score = row["score"]

            # Validate data
            if pd.isna(winner) or pd.isna(loser) or pd.isna(matchid) or pd.isna(surface) or surface not in ["Hard", "Clay", "Grass"]:
                print(f"Skipping row {index}: Missing or invalid data")
                continue

            # Initialize player history
            for player in [winner, loser]:
                if player not in player_match_history:
                    player_match_history[player] = []
                if player not in player_tb_history:
                    player_tb_history[player] = []

            # Calculate recent match counts (last 30 days)
            winner_recent = len([d for d, _ in player_match_history[winner] if (match_date - d).days <= 30])
            loser_recent = len([d for d, _ in player_match_history[loser] if (match_date - d).days <= 30])

            # Calculate tie-break win % (before this match)
            winner_tb_pct = calc_tb_win_pct(player_tb_history[winner])
            loser_tb_pct = calc_tb_win_pct(player_tb_history[loser])

            # Get current H2H wins (before this match)
            w_h2h_before = h2h_wins.get((winner, loser), 0)
            l_h2h_before = h2h_wins.get((loser, winner), 0)
            w_h2h_surface_before = h2h_surface_wins[surface].get((winner, loser), 0)
            l_h2h_surface_before = h2h_surface_wins[surface].get((loser, winner), 0)

            # Update the database
            update_query = text(f"""
                UPDATE {TABLE_NAME}
                SET w_h2h_wins = :w_h2h_wins,
                    l_h2h_wins = :l_h2h_wins,
                    w_h2h_wins_{surface.lower()} = :w_h2h_surface,
                    l_h2h_wins_{surface.lower()} = :l_h2h_surface,
                    winner_recent_matches_30d = :w_recent,
                    loser_recent_matches_30d = :l_recent,
                    winner_tb_win_pct = :w_tb_pct,
                    loser_tb_win_pct = :l_tb_pct
                WHERE matchid = :matchid
            """)
            conn.execute(update_query, {
                "w_h2h_wins": w_h2h_before,
                "l_h2h_wins": l_h2h_before,
                "w_h2h_surface": w_h2h_surface_before,
                "l_h2h_surface": l_h2h_surface_before,
                "w_recent": winner_recent,
                "l_recent": loser_recent,
                "w_tb_pct": winner_tb_pct,
                "l_tb_pct": loser_tb_pct,
                "matchid": matchid
            })

            # Update histories after the match
            h2h_wins[(winner, loser)] = w_h2h_before + 1
            h2h_wins[(loser, winner)] = h2h_wins.get((loser, winner), 0)
            h2h_surface_wins[surface][(winner, loser)] = w_h2h_surface_before + 1
            h2h_surface_wins[surface][(loser, winner)] = h2h_surface_wins[surface].get((loser, winner), 0)
            player_match_history[winner].append((match_date, matchid))
            player_match_history[loser].append((match_date, matchid))
            
            # Update tie-break history
            w_tb_won, l_tb_won = parse_tiebreaks(score)
            player_tb_history[winner].append((w_tb_won, w_tb_won + l_tb_won))
            player_tb_history[loser].append((l_tb_won, w_tb_won + l_tb_won))

        conn.commit()
    print("✅ Head-to-head, surface H2H, recent matches, and tie-break performance updated (walkovers excluded)!")
except Exception as e:
    print(f"Error updating database: {e}")
    raise