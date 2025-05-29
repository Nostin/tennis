import sys
import os
import pandas as pd
from sqlalchemy import text

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_connect import get_engine
engine = get_engine()

# Pull predictions + soft odds
query = """
SELECT
    p.matchid,
    p.player1_name,
    p.player2_name,
    p.winner_name,
    p.loser_name,
    p.model_prob_p1,
    p.model_prob_p2,
    p.predicted_winner,
    mar.avgw AS soft_odds_p1,
    mar.avgl AS soft_odds_p2,
    mar.surface,
    mar.f_winner_total_matches as winner_total_matches,
    mar.f_loser_total_matches as loser_total_matches,
    mar.f_winner_total_surface_matches as winner_total_surface_matches,
    mar.f_loser_total_surface_matches as loser_total_surface_matches,
    mar.f_winner_recent_matches_30d as winner_recent_matches_30d,
    mar.f_loser_recent_matches_30d as loser_recent_matches_30d
FROM xgboost_predictions p
JOIN matched_atp_records mar ON p.matchid::text = mar.matchid::text
WHERE mar.avgw IS NOT NULL AND mar.avgl IS NOT NULL
"""

with engine.connect() as conn:
    df = pd.read_sql(query, conn)

# Normalize softbook odds to probabilities
df["soft_prob_p1"] = 1 / df["soft_odds_p1"]
df["soft_prob_p2"] = 1 / df["soft_odds_p2"]
df["total_prob"] = df["soft_prob_p1"] + df["soft_prob_p2"]
df["soft_prob_p1"] /= df["total_prob"]
df["soft_prob_p2"] /= df["total_prob"]

# Calculate model value over softbook implied
df["value_p1"] = df["model_prob_p1"] - df["soft_prob_p1"]
df["value_p2"] = df["model_prob_p2"] - df["soft_prob_p2"]

# Flag bets with positive edge
value_threshold = 0.05
value_bets = df[(df["value_p1"] > value_threshold) | (df["value_p2"] > value_threshold)].copy()

# Save value bets to table and CSV
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS value_bet_opportunities"))
    value_bets.to_sql("value_bet_opportunities", conn, index=False)

value_bets.to_csv("value_bet_opportunities.csv", index=False)
print("✅ Saved value bets to DB and CSV.")
