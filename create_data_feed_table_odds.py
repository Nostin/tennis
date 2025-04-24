import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import numpy as np
from sqlalchemy import text
from db_connect import get_engine

# Connect to DB
engine = get_engine()

SOURCE_TABLE = "matched_atp_records"
TARGET_TABLE = "xgboost_odds_data_feed"

# Create table (unchanged)
create_table_query = f"""
DROP TABLE IF EXISTS {TARGET_TABLE};
CREATE TABLE {TARGET_TABLE} (
    matchid TEXT PRIMARY KEY,
    date DATE,
    surface TEXT,
    player1_name TEXT,
    player2_name TEXT,
    target INTEGER,
    elo_diff NUMERIC(8,2),
    surface_elo_diff NUMERIC(8,2),
    glicko_diff NUMERIC(8,2),
    glicko_surface_diff NUMERIC(8,2),
    avg_elo_faced_diff NUMERIC(8,2),
    avg_surface_elo_faced_diff NUMERIC(8,2),
    tournament_fatigue_diff INTEGER,
    h2h_wins_diff INTEGER,
    h2h_surface_wins_diff INTEGER,
    win_pct_last_30d_diff NUMERIC(5,2),
    recent_matches_30d_diff INTEGER,
    tournament_strength INTEGER,
    hold_pct_diff NUMERIC(5,2),
    hold_surface_pct_diff NUMERIC(5,2),
    break_pct_diff NUMERIC(5,2),
    break_surface_pct_diff NUMERIC(5,2),
    break_point_conversion_diff NUMERIC(5, 2),
    break_point_surface_conversion_diff NUMERIC(5, 2),
    tiebreak_rate_diff NUMERIC(5, 2),
    tiebreak_win_diff NUMERIC(5, 2),
    tiebreak_surface_rate_diff NUMERIC(5, 2),
    tiebreak_surface_win_diff NUMERIC(5, 2),
    home_adv_diff INT,
    p1_overall_rd FLOAT,
    p2_overall_rd FLOAT,
    p1_surface_rd FLOAT,
    p2_surface_rd FLOAT,
    p1_odds_shape_flat INT,
    p1_odds_shape_u INT,
    p1_odds_shape_inv_u INT,
    p1_odds_shape_mixed INT,
    p1_odds_shape_short INT,
    p1_odds_shape_drift INT,
    p1_odds_shape_unknown INT,
    p1_odds_shape_strength FLOAT,
    p1_odds_shortened_fav INT,
    p1_odds_late_money INT,
    p2_odds_shape_flat INT,
    p2_odds_shape_u INT,
    p2_odds_shape_inv_u INT,
    p2_odds_shape_mixed INT,
    p2_odds_shape_short INT,
    p2_odds_shape_drift INT,
    p2_odds_shape_unknown INT,
    p2_odds_shape_strength FLOAT,
    p2_odds_shortened_fav INT,
    p2_odds_late_money INT
);
"""
with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()
print(f"✅ Created table {TARGET_TABLE}")

# Load data
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT 
            matchid, date, surface, series,
            winner_name, loser_name,
            f_winner_home_adv, f_loser_home_adv,
            f_winner_overall_elo, f_winner_surface_elo, f_winner_avg_elo_faced, f_winner_avg_surface_elo_faced,
            f_loser_overall_elo, f_loser_surface_elo, f_loser_avg_elo_faced, f_loser_avg_surface_elo_faced,
            f_winner_overall_glicko, f_winner_surface_glicko, f_loser_overall_glicko, f_loser_surface_glicko,
            f_winner_tourney_minutes, f_loser_tourney_minutes,
            f_winner_win_pct_30d, f_loser_win_pct_30d,
            f_winner_recent_matches_30d, f_loser_recent_matches_30d,
            f_w_hold_pct, f_w_break_pct, f_w_bp_conversion_pct,
            f_l_hold_pct, f_l_break_pct, f_l_bp_conversion_pct,
            f_w_hold_pct_clay, f_w_hold_pct_hard, f_w_hold_pct_grass,
            f_l_hold_pct_clay, f_l_hold_pct_hard, f_l_hold_pct_grass,
            f_w_break_pct_clay, f_w_break_pct_hard, f_w_break_pct_grass,
            f_l_break_pct_clay, f_l_break_pct_hard, f_l_break_pct_grass,
            f_w_bp_conversion_pct_clay, f_w_bp_conversion_pct_hard, f_w_bp_conversion_pct_grass,
            f_l_bp_conversion_pct_clay, f_l_bp_conversion_pct_hard, f_l_bp_conversion_pct_grass,
            f_w_tb_rate, f_l_tb_rate, f_w_tb_win_pct, f_l_tb_win_pct,
            f_w_tb_rate_hard, f_w_tb_rate_clay, f_w_tb_rate_grass,
            f_l_tb_rate_hard, f_l_tb_rate_clay, f_l_tb_rate_grass,
            f_w_tb_win_pct_hard, f_w_tb_win_pct_clay, f_w_tb_win_pct_grass,
            f_l_tb_win_pct_hard, f_l_tb_win_pct_clay, f_l_tb_win_pct_grass,
            f_w_h2h_wins, f_w_h2h_wins_clay, f_w_h2h_wins_grass, f_w_h2h_wins_hard,
            f_l_h2h_wins, f_l_h2h_wins_clay, f_l_h2h_wins_grass, f_l_h2h_wins_hard,
            f_winner_overall_rd, f_winner_surface_rd, f_loser_overall_rd, f_loser_surface_rd,
            f_winner_odds_shape, f_winner_odds_strength, f_winner_shortened_fav, f_winner_late_money,
            f_loser_odds_shape, f_loser_odds_strength, f_loser_shortened_fav, f_loser_late_money
        FROM {SOURCE_TABLE}
        WHERE "comment" != 'Walkover'
        AND "date" > '2015-06-01'
        AND f_winner_total_matches > 9
        AND f_loser_total_matches > 9
        AND f_winner_overall_elo IS NOT NULL 
        AND f_loser_overall_elo IS NOT NULL
        AND f_winner_odds_shape IS NOT NULL
        AND f_loser_odds_shape IS NOT NULL
        ORDER BY date ASC
    """, conn)

df["randomise_flag"] = np.random.rand(len(df)) < 0.5
df["target"] = np.where(df["randomise_flag"], 1, 0)

# Assign p1/p2 stats based on winner/loser
df["p1_overall_elo"] = np.where(df["randomise_flag"], df["f_winner_overall_elo"], df["f_loser_overall_elo"])
df["p2_overall_elo"] = np.where(df["randomise_flag"], df["f_loser_overall_elo"], df["f_winner_overall_elo"])
df["p1_surface_elo"] = np.where(df["randomise_flag"], df["f_winner_surface_elo"], df["f_loser_surface_elo"])
df["p2_surface_elo"] = np.where(df["randomise_flag"], df["f_loser_surface_elo"], df["f_winner_surface_elo"])
df["p1_avg_elo_faced"] = np.where(df["randomise_flag"], df["f_winner_avg_elo_faced"], df["f_loser_avg_elo_faced"])
df["p2_avg_elo_faced"] = np.where(df["randomise_flag"], df["f_loser_avg_elo_faced"], df["f_winner_avg_elo_faced"])
df["p1_surface_avg_elo_faced"] = np.where(df["randomise_flag"], df["f_winner_avg_surface_elo_faced"], df["f_loser_avg_surface_elo_faced"])
df["p2_surface_avg_elo_faced"] = np.where(df["randomise_flag"], df["f_loser_avg_surface_elo_faced"], df["f_winner_avg_surface_elo_faced"])
df["p1_fatigue"] = np.where(df["randomise_flag"], df["f_winner_tourney_minutes"], df["f_loser_tourney_minutes"])
df["p2_fatigue"] = np.where(df["randomise_flag"], df["f_loser_tourney_minutes"], df["f_winner_tourney_minutes"])
df["p1_h2h_wins"] = np.where(df["randomise_flag"], df["f_w_h2h_wins"], df["f_l_h2h_wins"])
df["p2_h2h_wins"] = np.where(df["randomise_flag"], df["f_l_h2h_wins"], df["f_w_h2h_wins"])
df["p1_overall_glicko"] = np.where(df["randomise_flag"], df["f_winner_overall_glicko"], df["f_loser_overall_glicko"])
df["p2_overall_glicko"] = np.where(df["randomise_flag"], df["f_loser_overall_glicko"], df["f_winner_overall_glicko"])
df["p1_surface_glicko"] = np.where(df["randomise_flag"], df["f_winner_surface_glicko"], df["f_loser_surface_glicko"])
df["p2_surface_glicko"] = np.where(df["randomise_flag"], df["f_loser_surface_glicko"], df["f_winner_surface_glicko"])
df["p1_overall_rd"] = np.where(df["randomise_flag"], df["f_winner_overall_rd"], df["f_loser_overall_rd"])
df["p2_overall_rd"] = np.where(df["randomise_flag"], df["f_loser_overall_rd"], df["f_winner_overall_rd"])
df["p1_surface_rd"] = np.where(df["randomise_flag"], df["f_winner_surface_rd"], df["f_loser_surface_rd"])
df["p2_surface_rd"] = np.where(df["randomise_flag"], df["f_loser_surface_rd"], df["f_winner_surface_rd"])

# Conditions for surface and randomization
conditions = [
    (df["surface"] == "Clay") & df["randomise_flag"],
    (df["surface"] == "Clay") & ~df["randomise_flag"],
    (df["surface"] == "Grass") & df["randomise_flag"],
    (df["surface"] == "Grass") & ~df["randomise_flag"],
    (df["surface"] == "Hard") & df["randomise_flag"],
    (df["surface"] == "Hard") & ~df["randomise_flag"],
]

# H2H Choices for Player 1
p1_h2h_choices = [
    df["f_w_h2h_wins_clay"],
    df["f_l_h2h_wins_clay"],
    df["f_w_h2h_wins_grass"],
    df["f_l_h2h_wins_grass"],
    df["f_w_h2h_wins_hard"],
    df["f_l_h2h_wins_hard"],
]

# H2H Choices for Player 2 (flipped)
p2_h2h_choices = [
    df["f_l_h2h_wins_clay"],
    df["f_w_h2h_wins_clay"],
    df["f_l_h2h_wins_grass"],
    df["f_w_h2h_wins_grass"],
    df["f_l_h2h_wins_hard"],
    df["f_w_h2h_wins_hard"],
]

# Assign to DataFrame
df["p1_h2h_surface_wins"] = np.select(conditions, p1_h2h_choices, default=0)
df["p2_h2h_surface_wins"] = np.select(conditions, p2_h2h_choices, default=0)
df["p1_30d_match_count"] = np.where(df["randomise_flag"], df["f_winner_recent_matches_30d"], df["f_loser_recent_matches_30d"])
df["p2_30d_match_count"] = np.where(df["randomise_flag"], df["f_loser_recent_matches_30d"], df["f_winner_recent_matches_30d"])
df["p1_30d_win_rate"] = np.where(df["randomise_flag"], df["f_winner_win_pct_30d"], df["f_loser_win_pct_30d"])
df["p2_30d_win_rate"] = np.where(df["randomise_flag"], df["f_loser_win_pct_30d"], df["f_winner_win_pct_30d"])
df["p1_hold_pct"] = np.where(df["randomise_flag"], df["f_w_hold_pct"], df["f_l_hold_pct"])
df["p2_hold_pct"] = np.where(df["randomise_flag"], df["f_l_hold_pct"], df["f_w_hold_pct"])
df["p1_break_pct"] = np.where(df["randomise_flag"], df["f_w_break_pct"], df["f_l_break_pct"])
df["p2_break_pct"] = np.where(df["randomise_flag"], df["f_l_break_pct"], df["f_w_break_pct"])
df["p1_break_conv_pct"] = np.where(df["randomise_flag"], df["f_w_bp_conversion_pct"], df["f_l_bp_conversion_pct"])
df["p2_break_conv_pct"] = np.where(df["randomise_flag"], df["f_l_bp_conversion_pct"], df["f_w_bp_conversion_pct"])

# hold Choices for Player 1
p1_hold_choices = [
    df["f_w_hold_pct_clay"],
    df["f_l_hold_pct_clay"],
    df["f_w_hold_pct_grass"],
    df["f_l_hold_pct_grass"],
    df["f_w_hold_pct_hard"],
    df["f_l_hold_pct_hard"],
]

# hold Choices for Player 2 (flipped)
p2_hold_choices = [
    df["f_l_hold_pct_clay"],
    df["f_w_hold_pct_clay"],
    df["f_l_hold_pct_grass"],
    df["f_w_hold_pct_grass"],
    df["f_l_hold_pct_hard"],
    df["f_w_hold_pct_hard"],
]

df["p1_hold_surface_pct"] = np.select(conditions, p1_hold_choices, default=0)
df["p2_hold_surface_pct"] = np.select(conditions, p2_hold_choices, default=0)

# break Choices for Player 1
p1_break_choices = [
    df["f_w_break_pct_clay"],
    df["f_l_break_pct_clay"],
    df["f_w_break_pct_grass"],
    df["f_l_break_pct_grass"],
    df["f_w_break_pct_hard"],
    df["f_l_break_pct_hard"],
]

# break Choices for Player 2 (flipped)
p2_break_choices = [
    df["f_l_break_pct_clay"],
    df["f_w_break_pct_clay"],
    df["f_l_break_pct_grass"],
    df["f_w_break_pct_grass"],
    df["f_l_break_pct_hard"],
    df["f_w_break_pct_hard"],
]

df["p1_break_surface_pct"] = np.select(conditions, p1_break_choices, default=0)
df["p2_break_surface_pct"] = np.select(conditions, p2_break_choices, default=0)

# bp conv Choices for Player 1
p1_bp_conv_choices = [
    df["f_w_bp_conversion_pct_clay"],
    df["f_l_bp_conversion_pct_clay"],
    df["f_w_bp_conversion_pct_grass"],
    df["f_l_bp_conversion_pct_grass"],
    df["f_w_bp_conversion_pct_hard"],
    df["f_l_bp_conversion_pct_hard"],
]

# bp conv Choices for Player 2 (flipped)
p2_bp_conv_choices = [
    df["f_l_bp_conversion_pct_clay"],
    df["f_w_bp_conversion_pct_clay"],
    df["f_l_bp_conversion_pct_grass"],
    df["f_w_bp_conversion_pct_grass"],
    df["f_l_bp_conversion_pct_hard"],
    df["f_w_bp_conversion_pct_hard"],
]

df["p1_break_point_surface_conv"] = np.select(conditions, p1_bp_conv_choices, default=0)
df["p2_break_point_surface_conv"] = np.select(conditions, p2_bp_conv_choices, default=0)

df["p1_tiebreak_rate"] = np.where(df["randomise_flag"], df["f_w_tb_rate"], df["f_l_tb_rate"])
df["p2_tiebreak_rate"] = np.where(df["randomise_flag"], df["f_l_tb_rate"], df["f_w_tb_rate"])
df["p1_tiebreak_win_pct"] = np.where(df["randomise_flag"], df["f_w_tb_win_pct"], df["f_l_tb_win_pct"])
df["p2_tiebreak_win_pct"] = np.where(df["randomise_flag"], df["f_l_tb_win_pct"], df["f_w_tb_win_pct"])
df["p1_home_adv"] = np.where(df["randomise_flag"], df["f_winner_home_adv"], df["f_loser_home_adv"])
df["p2_home_adv"] = np.where(df["randomise_flag"], df["f_loser_home_adv"], df["f_winner_home_adv"])

# tie break rate Choices for Player 1
p1_tb_rate_choices = [
    df["f_w_tb_rate_clay"],
    df["f_l_tb_rate_clay"],
    df["f_w_tb_rate_grass"],
    df["f_l_tb_rate_grass"],
    df["f_w_tb_rate_hard"],
    df["f_l_tb_rate_hard"],
]

# tie break rate Choices for Player 2 (flipped)
p2_tb_rate_choices = [
    df["f_l_tb_rate_clay"],
    df["f_w_tb_rate_clay"],
    df["f_l_tb_rate_grass"],
    df["f_w_tb_rate_grass"],
    df["f_l_tb_rate_hard"],
    df["f_w_tb_rate_hard"],
]

df["p1_tiebreak_surface_rate"] = np.select(conditions, p1_tb_rate_choices, default=0)
df["p2_tiebreak_surface_rate"] = np.select(conditions, p2_tb_rate_choices, default=0)

# tie break win Choices for Player 1
p1_tb_win_choices = [
    df["f_w_tb_win_pct_clay"],
    df["f_l_tb_win_pct_clay"],
    df["f_w_tb_win_pct_grass"],
    df["f_l_tb_win_pct_grass"],
    df["f_w_tb_win_pct_hard"],
    df["f_l_tb_win_pct_hard"],
]

# tie break win Choices for Player 2 (flipped)
p2_tb_win_choices = [
    df["f_l_tb_win_pct_clay"],
    df["f_w_tb_win_pct_clay"],
    df["f_l_tb_win_pct_grass"],
    df["f_w_tb_win_pct_grass"],
    df["f_l_tb_win_pct_hard"],
    df["f_w_tb_win_pct_hard"],
]

df["p1_tiebreak_surface_win"] = np.select(conditions, p1_tb_win_choices, default=0)
df["p2_tiebreak_surface_win"] = np.select(conditions, p2_tb_win_choices, default=0)

# odds shit
df["p1_odds_shape_flat"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_odds_shape"] == "Flat", 1, 0),
    np.where(df["f_loser_odds_shape"] == "Flat", 1, 0)
)

df["p2_odds_shape_flat"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_odds_shape"] == "Flat", 1, 0),
    np.where(df["f_winner_odds_shape"] == "Flat", 1, 0)
)

df["p1_odds_shape_u"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_odds_shape"] == "U-shape", 1, 0),
    np.where(df["f_loser_odds_shape"] == "U-shape", 1, 0)
)

df["p2_odds_shape_u"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_odds_shape"] == "U-shape", 1, 0),
    np.where(df["f_winner_odds_shape"] == "U-shape", 1, 0)
)

df["p1_odds_shape_inv_u"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_odds_shape"] == "Inverse U", 1, 0),
    np.where(df["f_loser_odds_shape"] == "Inverse U", 1, 0)
)

df["p2_odds_shape_inv_u"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_odds_shape"] == "Inverse U", 1, 0),
    np.where(df["f_winner_odds_shape"] == "Inverse U", 1, 0)
)

df["p1_odds_shape_mixed"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_odds_shape"] == "Mixed", 1, 0),
    np.where(df["f_loser_odds_shape"] == "Mixed", 1, 0)
)

df["p2_odds_shape_mixed"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_odds_shape"] == "Mixed", 1, 0),
    np.where(df["f_winner_odds_shape"] == "Mixed", 1, 0)
)

df["p1_odds_shape_short"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_odds_shape"] == "Shortener", 1, 0),
    np.where(df["f_loser_odds_shape"] == "Shortener", 1, 0)
)

df["p2_odds_shape_short"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_odds_shape"] == "Shortener", 1, 0),
    np.where(df["f_winner_odds_shape"] == "Shortener", 1, 0)
)

df["p1_odds_shape_drift"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_odds_shape"] == "Drifter", 1, 0),
    np.where(df["f_loser_odds_shape"] == "Drifter", 1, 0)
)

df["p2_odds_shape_drift"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_odds_shape"] == "Drifter", 1, 0),
    np.where(df["f_winner_odds_shape"] == "Drifter", 1, 0)
)

df["p1_odds_shape_unknown"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_odds_shape"] == "Unknown", 1, 0),
    np.where(df["f_loser_odds_shape"] == "Unknown", 1, 0)
)

df["p2_odds_shape_unknown"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_odds_shape"] == "Unknown", 1, 0),
    np.where(df["f_winner_odds_shape"] == "Unknown", 1, 0)
)

df["p1_odds_shape_strength"] = np.where(df["randomise_flag"], df["f_winner_odds_strength"], df["f_loser_odds_strength"])
df["p2_odds_shape_strength"] = np.where(df["randomise_flag"], df["f_loser_odds_strength"], df["f_winner_odds_strength"])

df["p1_odds_shortened_fav"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_shortened_fav"] == True, 1, 0),
    np.where(df["f_loser_shortened_fav"] == True, 1, 0)
)

df["p2_odds_shortened_fav"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_shortened_fav"] == True, 1, 0),
    np.where(df["f_winner_shortened_fav"] == True, 1, 0)
)

df["p1_odds_late_money"] = np.where(
    df["randomise_flag"],
    np.where(df["f_winner_late_money"] == True, 1, 0),
    np.where(df["f_loser_late_money"] == True, 1, 0)
)

df["p2_odds_late_money"] = np.where(
    df["randomise_flag"],
    np.where(df["f_loser_late_money"] == True, 1, 0),
    np.where(df["f_winner_late_money"] == True, 1, 0)
)

# Feature Engineering (p1 - p2)
df["elo_diff"] = df["p1_overall_elo"] - df["p2_overall_elo"]
df["surface_elo_diff"] = df["p1_surface_elo"] - df["p2_surface_elo"]
df["avg_elo_faced_diff"] = df["p1_avg_elo_faced"] - df["p2_avg_elo_faced"]
df["avg_surface_elo_faced_diff"] = df["p1_surface_avg_elo_faced"] - df["p2_surface_avg_elo_faced"]
df["glicko_diff"] = df["p1_overall_glicko"] - df["p2_overall_glicko"]
df["glicko_surface_diff"] = df["p1_surface_glicko"] - df["p2_surface_glicko"]
df["tournament_fatigue_diff"] = df["p1_fatigue"] - df["p2_fatigue"]
df["h2h_wins_diff"] = df["p1_h2h_wins"] - df["p2_h2h_wins"]
df["h2h_surface_wins_diff"] = df["p1_h2h_surface_wins"] - df["p2_h2h_surface_wins"]
df["recent_matches_30d_diff"] = df["p1_30d_match_count"] - df["p2_30d_match_count"]
df["win_pct_last_30d_diff"] = df["p1_30d_win_rate"] - df["p2_30d_win_rate"]
df["hold_pct_diff"] = df["p1_hold_pct"] - df["p2_hold_pct"]
df["hold_surface_pct_diff"] = df["p1_hold_surface_pct"] - df["p2_hold_surface_pct"]
df["break_pct_diff"] = df["p1_break_pct"] - df["p2_break_pct"]
df["break_surface_pct_diff"] = df["p1_break_surface_pct"] - df["p2_break_surface_pct"]
df["break_point_conversion_diff"] = df["p1_break_conv_pct"] - df["p2_break_conv_pct"]
df["break_point_surface_conversion_diff"] = df["p1_break_point_surface_conv"] - df["p2_break_point_surface_conv"]
df["tiebreak_rate_diff"] = df["p1_tiebreak_rate"] - df["p2_tiebreak_rate"]
df["tiebreak_win_diff"] = df["p1_tiebreak_win_pct"] - df["p2_tiebreak_win_pct"]
df["home_adv_diff"] = df["p1_home_adv"] - df["p2_home_adv"]
df["tiebreak_surface_rate_diff"] = df["p1_tiebreak_surface_rate"] - df["p2_tiebreak_surface_rate"]
df["tiebreak_surface_win_diff"] = df["p1_tiebreak_surface_win"] - df["p2_tiebreak_surface_win"]
# Tournament Strength
tournament_strength_map = {"Grand Slam": 5, "Masters 1000": 4, "ATP500": 3, "ATP250": 2, "Masters Cup": 1}
df["tournament_strength"] = df["series"].map(tournament_strength_map).fillna(0).astype(int)

# write to new database table
# Final feature DataFrame
output_df = pd.DataFrame({
    "matchid": df["matchid"],
    "date": df["date"],
    "surface": df["surface"],
    "player1_name": np.where(df["randomise_flag"], df["winner_name"], df["loser_name"]),
    "player2_name": np.where(df["randomise_flag"], df["loser_name"], df["winner_name"]),
    "target": df["target"],
    "elo_diff": df["elo_diff"],
    "surface_elo_diff": df["surface_elo_diff"],
    "avg_elo_faced_diff": df["avg_elo_faced_diff"],
    "avg_surface_elo_faced_diff": df["avg_surface_elo_faced_diff"],
    "glicko_diff": df["glicko_diff"],
    "glicko_surface_diff": df["glicko_surface_diff"],
    "tournament_fatigue_diff": df["tournament_fatigue_diff"],
    "h2h_wins_diff": df["h2h_wins_diff"],
    "h2h_surface_wins_diff": df["h2h_surface_wins_diff"],
    "recent_matches_30d_diff": df["recent_matches_30d_diff"],
    "win_pct_last_30d_diff": df["win_pct_last_30d_diff"],
    "hold_pct_diff": df["hold_pct_diff"],
    "hold_surface_pct_diff": df["hold_surface_pct_diff"],
    "break_pct_diff": df["break_pct_diff"],
    "break_surface_pct_diff": df["break_surface_pct_diff"],
    "break_point_conversion_diff": df["break_point_conversion_diff"],
    "break_point_surface_conversion_diff": df["break_point_surface_conversion_diff"],
    "tiebreak_rate_diff": df["tiebreak_rate_diff"],
    "tiebreak_win_diff": df["tiebreak_win_diff"],
    "home_adv_diff": df["home_adv_diff"],
    "tiebreak_surface_rate_diff": df["tiebreak_surface_rate_diff"],
    "tiebreak_surface_win_diff": df["tiebreak_surface_win_diff"],
    "tournament_strength": df["tournament_strength"],
    "p1_overall_rd": df["p1_overall_rd"],
    "p2_overall_rd": df["p2_overall_rd"],
    "p1_surface_rd": df["p1_surface_rd"],
    "p2_surface_rd": df["p2_surface_rd"],
    "p1_odds_shape_flat": df["p1_odds_shape_flat"],
    "p2_odds_shape_flat": df["p2_odds_shape_flat"],
    "p1_odds_shape_u": df["p1_odds_shape_u"],
    "p2_odds_shape_u": df["p2_odds_shape_u"],
    "p1_odds_shape_inv_u": df["p1_odds_shape_inv_u"],
    "p2_odds_shape_inv_u": df["p2_odds_shape_inv_u"],
    "p1_odds_shape_mixed": df["p1_odds_shape_mixed"],
    "p2_odds_shape_mixed": df["p2_odds_shape_mixed"],
    "p1_odds_shape_short": df["p1_odds_shape_short"],
    "p2_odds_shape_short": df["p2_odds_shape_short"],
    "p1_odds_shape_drift": df["p1_odds_shape_drift"],
    "p2_odds_shape_drift": df["p2_odds_shape_drift"],
    "p1_odds_shape_unknown": df["p1_odds_shape_unknown"],
    "p2_odds_shape_unknown": df["p2_odds_shape_unknown"],
    "p1_odds_shape_strength": df["p1_odds_shape_strength"],
    "p2_odds_shape_strength": df["p2_odds_shape_strength"],
    "p1_odds_shortened_fav": df["p1_odds_shortened_fav"],
    "p2_odds_shortened_fav": df["p2_odds_shortened_fav"],
    "p1_odds_late_money": df["p1_odds_late_money"],
    "p2_odds_late_money": df["p2_odds_late_money"]
})

# Write to DB
print("💾 Inserting into database...")
output_df.to_sql(TARGET_TABLE, engine, if_exists="append", index=False, method="multi")
print(f"✅ Wrote {len(output_df)} rows to {TARGET_TABLE}")
