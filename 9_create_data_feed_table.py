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
TARGET_TABLE = "xgboost_data_feed"

# Create table (unchanged)
create_table_query = f"""
DROP TABLE IF EXISTS {TARGET_TABLE};
CREATE TABLE {TARGET_TABLE} (
    matchid TEXT PRIMARY KEY,
    winner_name TEXT,
    loser_name TEXT,
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
    p2_odds_late_money INT,
    p1_was_fav INT,
    p2_was_fav INT,
    p1_was_fav_closing INT,
    p2_was_fav_closing INT,
    p1_became_fav INT,
    p2_became_fav INT,
    p1_lost_fav INT,
    p2_lost_fav INT,
    p1_stayed_fav INT,
    p2_stayed_fav INT,
    odds_volatility_diff FLOAT,
    odds_trend_slope_diff FLOAT,
    odds_max_swing_diff FLOAT,
    odds_early_move_diff FLOAT,
    odds_late_move_diff FLOAT,
    odds_vol_ratio_diff FLOAT,
    odds_net_move_diff FLOAT,
    odds_curvature_diff FLOAT,
    deltap_1_diff FLOAT,
    deltap_2_diff FLOAT,
    deltap_3_diff FLOAT,
    deltap_4_diff FLOAT,
    deltap_5_diff FLOAT,
    deltap_6_diff FLOAT,
    deltap_7_diff FLOAT,
    deltap_8_diff FLOAT,
    deltap_9_diff FLOAT,
    deltap_10_diff FLOAT
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
            f_loser_odds_shape, f_loser_odds_strength, f_loser_shortened_fav, f_loser_late_money,
            f_winner_odds_volatility, f_loser_odds_volatility,
            f_winner_odds_trend_slope, f_loser_odds_trend_slope,
            f_winner_odds_max_swing, f_loser_odds_max_swing,
            f_winner_odds_early_move, f_loser_odds_early_move,
            f_winner_odds_late_move, f_loser_odds_late_move,
            f_winner_odds_vol_ratio, f_loser_odds_vol_ratio,
            f_winner_odds_net_move, f_loser_odds_net_move,
            f_winner_odds_curvature, f_loser_odds_curvature,
            f_winner_was_fav, f_winner_was_fav_closing, f_winner_became_fav, f_winner_lost_fav, f_winner_stayed_fav,
            f_loser_was_fav, f_loser_was_fav_closing, f_loser_became_fav, f_loser_lost_fav, f_loser_stayed_fav,
            f_winner_odds_deltap_1, f_winner_odds_deltap_2, f_winner_odds_deltap_3, f_winner_odds_deltap_4,
            f_winner_odds_deltap_5, f_winner_odds_deltap_6, f_winner_odds_deltap_7, f_winner_odds_deltap_8,
            f_winner_odds_deltap_9, f_winner_odds_deltap_10,
            f_loser_odds_deltap_1, f_loser_odds_deltap_2, f_loser_odds_deltap_3, f_loser_odds_deltap_4,
            f_loser_odds_deltap_5, f_loser_odds_deltap_6, f_loser_odds_deltap_7, f_loser_odds_deltap_8,
            f_loser_odds_deltap_9, f_loser_odds_deltap_10
        FROM {SOURCE_TABLE}
        WHERE "comment" != 'Walkover'
        AND "date" > '2015-06-01'
        AND f_winner_total_matches > 9
        AND f_loser_total_matches > 9
        AND f_winner_overall_elo IS NOT NULL 
        AND f_loser_overall_elo IS NOT NULL
        ORDER BY date ASC
    """, conn)

df["randomise_flag"] = np.random.rand(len(df)) < 0.5
df["target"] = np.where(df["randomise_flag"], 1, 0)

# Create a dictionary to store all the new columns
new_columns = {}

# Basic stats
new_columns.update({
    "p1_overall_elo": np.where(df["randomise_flag"], df["f_winner_overall_elo"], df["f_loser_overall_elo"]),
    "p2_overall_elo": np.where(df["randomise_flag"], df["f_loser_overall_elo"], df["f_winner_overall_elo"]),
    "p1_surface_elo": np.where(df["randomise_flag"], df["f_winner_surface_elo"], df["f_loser_surface_elo"]),
    "p2_surface_elo": np.where(df["randomise_flag"], df["f_loser_surface_elo"], df["f_winner_surface_elo"]),
    "p1_avg_elo_faced": np.where(df["randomise_flag"], df["f_winner_avg_elo_faced"], df["f_loser_avg_elo_faced"]),
    "p2_avg_elo_faced": np.where(df["randomise_flag"], df["f_loser_avg_elo_faced"], df["f_winner_avg_elo_faced"]),
    "p1_surface_avg_elo_faced": np.where(df["randomise_flag"], df["f_winner_avg_surface_elo_faced"], df["f_loser_avg_surface_elo_faced"]),
    "p2_surface_avg_elo_faced": np.where(df["randomise_flag"], df["f_loser_avg_surface_elo_faced"], df["f_winner_avg_surface_elo_faced"]),
    "p1_fatigue": np.where(df["randomise_flag"], df["f_winner_tourney_minutes"], df["f_loser_tourney_minutes"]),
    "p2_fatigue": np.where(df["randomise_flag"], df["f_loser_tourney_minutes"], df["f_winner_tourney_minutes"]),
    "p1_h2h_wins": np.where(df["randomise_flag"], df["f_w_h2h_wins"], df["f_l_h2h_wins"]),
    "p2_h2h_wins": np.where(df["randomise_flag"], df["f_l_h2h_wins"], df["f_w_h2h_wins"]),
    "p1_overall_glicko": np.where(df["randomise_flag"], df["f_winner_overall_glicko"], df["f_loser_overall_glicko"]),
    "p2_overall_glicko": np.where(df["randomise_flag"], df["f_loser_overall_glicko"], df["f_winner_overall_glicko"]),
    "p1_surface_glicko": np.where(df["randomise_flag"], df["f_winner_surface_glicko"], df["f_loser_surface_glicko"]),
    "p2_surface_glicko": np.where(df["randomise_flag"], df["f_loser_surface_glicko"], df["f_winner_surface_glicko"]),
    "p1_overall_rd": np.where(df["randomise_flag"], df["f_winner_overall_rd"], df["f_loser_overall_rd"]),
    "p2_overall_rd": np.where(df["randomise_flag"], df["f_loser_overall_rd"], df["f_winner_overall_rd"]),
    "p1_surface_rd": np.where(df["randomise_flag"], df["f_winner_surface_rd"], df["f_loser_surface_rd"]),
    "p2_surface_rd": np.where(df["randomise_flag"], df["f_loser_surface_rd"], df["f_winner_surface_rd"])
})

# Surface-specific stats
conditions = [
    (df["surface"] == "Clay") & df["randomise_flag"],
    (df["surface"] == "Clay") & ~df["randomise_flag"],
    (df["surface"] == "Grass") & df["randomise_flag"],
    (df["surface"] == "Grass") & ~df["randomise_flag"],
    (df["surface"] == "Hard") & df["randomise_flag"],
    (df["surface"] == "Hard") & ~df["randomise_flag"],
]

# H2H surface choices
p1_h2h_choices = [
    df["f_w_h2h_wins_clay"],
    df["f_l_h2h_wins_clay"],
    df["f_w_h2h_wins_grass"],
    df["f_l_h2h_wins_grass"],
    df["f_w_h2h_wins_hard"],
    df["f_l_h2h_wins_hard"],
]

p2_h2h_choices = [
    df["f_l_h2h_wins_clay"],
    df["f_w_h2h_wins_clay"],
    df["f_l_h2h_wins_grass"],
    df["f_w_h2h_wins_grass"],
    df["f_l_h2h_wins_hard"],
    df["f_w_h2h_wins_hard"],
]

new_columns.update({
    "p1_h2h_surface_wins": np.select(conditions, p1_h2h_choices, default=0),
    "p2_h2h_surface_wins": np.select(conditions, p2_h2h_choices, default=0),
    "p1_30d_match_count": np.where(df["randomise_flag"], df["f_winner_recent_matches_30d"], df["f_loser_recent_matches_30d"]),
    "p2_30d_match_count": np.where(df["randomise_flag"], df["f_loser_recent_matches_30d"], df["f_winner_recent_matches_30d"]),
    "p1_30d_win_rate": np.where(df["randomise_flag"], df["f_winner_win_pct_30d"], df["f_loser_win_pct_30d"]),
    "p2_30d_win_rate": np.where(df["randomise_flag"], df["f_loser_win_pct_30d"], df["f_winner_win_pct_30d"])
})

# Hold and break stats
new_columns.update({
    "p1_hold_pct": np.where(df["randomise_flag"], df["f_w_hold_pct"], df["f_l_hold_pct"]),
    "p2_hold_pct": np.where(df["randomise_flag"], df["f_l_hold_pct"], df["f_w_hold_pct"]),
    "p1_break_pct": np.where(df["randomise_flag"], df["f_w_break_pct"], df["f_l_break_pct"]),
    "p2_break_pct": np.where(df["randomise_flag"], df["f_l_break_pct"], df["f_w_break_pct"]),
    "p1_break_conv_pct": np.where(df["randomise_flag"], df["f_w_bp_conversion_pct"], df["f_l_bp_conversion_pct"]),
    "p2_break_conv_pct": np.where(df["randomise_flag"], df["f_l_bp_conversion_pct"], df["f_w_bp_conversion_pct"])
})

# Surface-specific hold and break stats
p1_hold_choices = [
    df["f_w_hold_pct_clay"],
    df["f_l_hold_pct_clay"],
    df["f_w_hold_pct_grass"],
    df["f_l_hold_pct_grass"],
    df["f_w_hold_pct_hard"],
    df["f_l_hold_pct_hard"],
]

p2_hold_choices = [
    df["f_l_hold_pct_clay"],
    df["f_w_hold_pct_clay"],
    df["f_l_hold_pct_grass"],
    df["f_w_hold_pct_grass"],
    df["f_l_hold_pct_hard"],
    df["f_w_hold_pct_hard"],
]

new_columns.update({
    "p1_hold_surface_pct": np.select(conditions, p1_hold_choices, default=0),
    "p2_hold_surface_pct": np.select(conditions, p2_hold_choices, default=0)
})

# Break point conversion stats
p1_bp_conv_choices = [
    df["f_w_bp_conversion_pct_clay"],
    df["f_l_bp_conversion_pct_clay"],
    df["f_w_bp_conversion_pct_grass"],
    df["f_l_bp_conversion_pct_grass"],
    df["f_w_bp_conversion_pct_hard"],
    df["f_l_bp_conversion_pct_hard"],
]

p2_bp_conv_choices = [
    df["f_l_bp_conversion_pct_clay"],
    df["f_w_bp_conversion_pct_clay"],
    df["f_l_bp_conversion_pct_grass"],
    df["f_w_bp_conversion_pct_grass"],
    df["f_l_bp_conversion_pct_hard"],
    df["f_w_bp_conversion_pct_hard"],
]

# Break surface percentage choices
p1_break_choices = [
    df["f_w_break_pct_clay"],
    df["f_l_break_pct_clay"],
    df["f_w_break_pct_grass"],
    df["f_l_break_pct_grass"],
    df["f_w_break_pct_hard"],
    df["f_l_break_pct_hard"],
]

p2_break_choices = [
    df["f_l_break_pct_clay"],
    df["f_w_break_pct_clay"],
    df["f_l_break_pct_grass"],
    df["f_w_break_pct_grass"],
    df["f_l_break_pct_hard"],
    df["f_w_break_pct_hard"],
]

new_columns.update({
    "p1_break_point_surface_conv": np.select(conditions, p1_bp_conv_choices, default=0),
    "p2_break_point_surface_conv": np.select(conditions, p2_bp_conv_choices, default=0),
    "p1_break_surface_pct": np.select(conditions, p1_break_choices, default=0),
    "p2_break_surface_pct": np.select(conditions, p2_break_choices, default=0)
})

# Tiebreak stats
new_columns.update({
    "p1_tiebreak_rate": np.where(df["randomise_flag"], df["f_w_tb_rate"], df["f_l_tb_rate"]),
    "p2_tiebreak_rate": np.where(df["randomise_flag"], df["f_l_tb_rate"], df["f_w_tb_rate"]),
    "p1_tiebreak_win_pct": np.where(df["randomise_flag"], df["f_w_tb_win_pct"], df["f_l_tb_win_pct"]),
    "p2_tiebreak_win_pct": np.where(df["randomise_flag"], df["f_l_tb_win_pct"], df["f_w_tb_win_pct"]),
    "p1_home_adv": np.where(df["randomise_flag"], df["f_winner_home_adv"], df["f_loser_home_adv"]),
    "p2_home_adv": np.where(df["randomise_flag"], df["f_loser_home_adv"], df["f_winner_home_adv"])
})

# Surface-specific tiebreak stats
p1_tb_rate_choices = [
    df["f_w_tb_rate_clay"],
    df["f_l_tb_rate_clay"],
    df["f_w_tb_rate_grass"],
    df["f_l_tb_rate_grass"],
    df["f_w_tb_rate_hard"],
    df["f_l_tb_rate_hard"],
]

p2_tb_rate_choices = [
    df["f_l_tb_rate_clay"],
    df["f_w_tb_rate_clay"],
    df["f_l_tb_rate_grass"],
    df["f_w_tb_rate_grass"],
    df["f_l_tb_rate_hard"],
    df["f_w_tb_rate_hard"],
]

# Surface-specific tiebreak win percentage choices
p1_tb_win_choices = [
    df["f_w_tb_win_pct_clay"],
    df["f_l_tb_win_pct_clay"],
    df["f_w_tb_win_pct_grass"],
    df["f_l_tb_win_pct_grass"],
    df["f_w_tb_win_pct_hard"],
    df["f_l_tb_win_pct_hard"],
]

p2_tb_win_choices = [
    df["f_l_tb_win_pct_clay"],
    df["f_w_tb_win_pct_clay"],
    df["f_l_tb_win_pct_grass"],
    df["f_w_tb_win_pct_grass"],
    df["f_l_tb_win_pct_hard"],
    df["f_w_tb_win_pct_hard"],
]

new_columns.update({
    "p1_tiebreak_surface_rate": np.select(conditions, p1_tb_rate_choices, default=0),
    "p2_tiebreak_surface_rate": np.select(conditions, p2_tb_rate_choices, default=0),
    "p1_tiebreak_surface_win": np.select(conditions, p1_tb_win_choices, default=0),
    "p2_tiebreak_surface_win": np.select(conditions, p2_tb_win_choices, default=0)
})

# Odds shape stats
odds_shape_columns = {
    "flat": "Flat",
    "u": "U-shape",
    "inv_u": "Inverse U",
    "mixed": "Mixed",
    "short": "Shortener",
    "drift": "Drifter",
    "unknown": "Unknown"
}

for shape, shape_value in odds_shape_columns.items():
    new_columns.update({
        f"p1_odds_shape_{shape}": np.where(
            df["randomise_flag"],
            np.where(df["f_winner_odds_shape"] == shape_value, 1, 0),
            np.where(df["f_loser_odds_shape"] == shape_value, 1, 0)
        ),
        f"p2_odds_shape_{shape}": np.where(
            df["randomise_flag"],
            np.where(df["f_loser_odds_shape"] == shape_value, 1, 0),
            np.where(df["f_winner_odds_shape"] == shape_value, 1, 0)
        )
    })

# Other odds stats
new_columns.update({
    "p1_odds_shape_strength": np.where(df["randomise_flag"], df["f_winner_odds_strength"], df["f_loser_odds_strength"]),
    "p2_odds_shape_strength": np.where(df["randomise_flag"], df["f_loser_odds_strength"], df["f_winner_odds_strength"]),
    "p1_odds_shortened_fav": np.where(
        df["randomise_flag"],
        np.where(df["f_winner_shortened_fav"] == True, 1, 0),
        np.where(df["f_loser_shortened_fav"] == True, 1, 0)
    ),
    "p2_odds_shortened_fav": np.where(
        df["randomise_flag"],
        np.where(df["f_loser_shortened_fav"] == True, 1, 0),
        np.where(df["f_winner_shortened_fav"] == True, 1, 0)
    ),
    "p1_odds_late_money": np.where(
        df["randomise_flag"],
        np.where(df["f_winner_late_money"] == True, 1, 0),
        np.where(df["f_loser_late_money"] == True, 1, 0)
    ),
    "p2_odds_late_money": np.where(
        df["randomise_flag"],
        np.where(df["f_loser_late_money"] == True, 1, 0),
        np.where(df["f_winner_late_money"] == True, 1, 0)
    )
})

# Favorite status stats
favorite_stats = [
    "was_fav", "was_fav_closing", "became_fav", "lost_fav", "stayed_fav"
]

for stat in favorite_stats:
    new_columns.update({
        f"p1_{stat}": np.where(
            df["randomise_flag"],
            np.where(df[f"f_winner_{stat}"].isna(), 0, df[f"f_winner_{stat}"]).astype(int),
            np.where(df[f"f_loser_{stat}"].isna(), 0, df[f"f_loser_{stat}"]).astype(int)
        ),
        f"p2_{stat}": np.where(
            df["randomise_flag"],
            np.where(df[f"f_loser_{stat}"].isna(), 0, df[f"f_loser_{stat}"]).astype(int),
            np.where(df[f"f_winner_{stat}"].isna(), 0, df[f"f_winner_{stat}"]).astype(int)
        )
    })

# Odds movement stats
odds_movement_stats = [
    "odds_volatility", "odds_trend_slope", "odds_max_swing",
    "odds_early_move", "odds_late_move", "odds_vol_ratio",
    "odds_net_move", "odds_curvature"
]

for stat in odds_movement_stats:
    new_columns.update({
        f"{stat}_diff": np.where(df["randomise_flag"], df[f"f_winner_{stat}"], df[f"f_loser_{stat}"])
    })

# Delta p stats
for i in range(1, 11):
    new_columns.update({
        f"deltap_{i}_diff": np.where(df["randomise_flag"], df[f"f_winner_odds_deltap_{i}"], df[f"f_loser_odds_deltap_{i}"])
    })

# Tournament strength
tournament_strength_map = {"Grand Slam": 5, "Masters 1000": 4, "ATP500": 3, "ATP250": 2, "Masters Cup": 1}
new_columns["tournament_strength"] = df["series"].map(tournament_strength_map).fillna(0).astype(int)

# Create a new DataFrame with all the columns at once
new_df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)

# Calculate differences
diff_columns = {
    "elo_diff": new_df["p1_overall_elo"] - new_df["p2_overall_elo"],
    "surface_elo_diff": new_df["p1_surface_elo"] - new_df["p2_surface_elo"],
    "avg_elo_faced_diff": new_df["p1_avg_elo_faced"] - new_df["p2_avg_elo_faced"],
    "avg_surface_elo_faced_diff": new_df["p1_surface_avg_elo_faced"] - new_df["p2_surface_avg_elo_faced"],
    "glicko_diff": new_df["p1_overall_glicko"] - new_df["p2_overall_glicko"],
    "glicko_surface_diff": new_df["p1_surface_glicko"] - new_df["p2_surface_glicko"],
    "tournament_fatigue_diff": new_df["p1_fatigue"] - new_df["p2_fatigue"],
    "h2h_wins_diff": new_df["p1_h2h_wins"] - new_df["p2_h2h_wins"],
    "h2h_surface_wins_diff": new_df["p1_h2h_surface_wins"] - new_df["p2_h2h_surface_wins"],
    "recent_matches_30d_diff": new_df["p1_30d_match_count"] - new_df["p2_30d_match_count"],
    "win_pct_last_30d_diff": new_df["p1_30d_win_rate"] - new_df["p2_30d_win_rate"],
    "hold_pct_diff": new_df["p1_hold_pct"] - new_df["p2_hold_pct"],
    "hold_surface_pct_diff": new_df["p1_hold_surface_pct"] - new_df["p2_hold_surface_pct"],
    "break_pct_diff": new_df["p1_break_pct"] - new_df["p2_break_pct"],
    "break_surface_pct_diff": new_df["p1_break_surface_pct"] - new_df["p2_break_surface_pct"],
    "break_point_conversion_diff": new_df["p1_break_conv_pct"] - new_df["p2_break_conv_pct"],
    "break_point_surface_conversion_diff": new_df["p1_break_point_surface_conv"] - new_df["p2_break_point_surface_conv"],
    "tiebreak_rate_diff": new_df["p1_tiebreak_rate"] - new_df["p2_tiebreak_rate"],
    "tiebreak_win_diff": new_df["p1_tiebreak_win_pct"] - new_df["p2_tiebreak_win_pct"],
    "home_adv_diff": new_df["p1_home_adv"] - new_df["p2_home_adv"],
    "tiebreak_surface_rate_diff": new_df["p1_tiebreak_surface_rate"] - new_df["p2_tiebreak_surface_rate"],
    "tiebreak_surface_win_diff": new_df["p1_tiebreak_surface_win"] - new_df["p2_tiebreak_surface_win"]
}

# Add difference columns to the DataFrame
new_df = pd.concat([new_df, pd.DataFrame(diff_columns)], axis=1)

# Create final output DataFrame
output_df = pd.DataFrame({
    "matchid": new_df["matchid"],
    "winner_name": new_df["winner_name"],
    "loser_name": new_df["loser_name"],
    "date": new_df["date"],
    "surface": new_df["surface"],
    "player1_name": np.where(new_df["randomise_flag"], new_df["winner_name"], new_df["loser_name"]),
    "player2_name": np.where(new_df["randomise_flag"], new_df["loser_name"], new_df["winner_name"]),
    "target": new_df["target"],
    "elo_diff": new_df["elo_diff"],
    "surface_elo_diff": new_df["surface_elo_diff"],
    "avg_elo_faced_diff": new_df["avg_elo_faced_diff"],
    "avg_surface_elo_faced_diff": new_df["avg_surface_elo_faced_diff"],
    "glicko_diff": new_df["glicko_diff"],
    "glicko_surface_diff": new_df["glicko_surface_diff"],
    "tournament_fatigue_diff": new_df["tournament_fatigue_diff"],
    "h2h_wins_diff": new_df["h2h_wins_diff"],
    "h2h_surface_wins_diff": new_df["h2h_surface_wins_diff"],
    "recent_matches_30d_diff": new_df["recent_matches_30d_diff"],
    "win_pct_last_30d_diff": new_df["win_pct_last_30d_diff"],
    "hold_pct_diff": new_df["hold_pct_diff"],
    "hold_surface_pct_diff": new_df["hold_surface_pct_diff"],
    "break_pct_diff": new_df["break_pct_diff"],
    "break_surface_pct_diff": new_df["break_surface_pct_diff"],
    "break_point_conversion_diff": new_df["break_point_conversion_diff"],
    "break_point_surface_conversion_diff": new_df["break_point_surface_conversion_diff"],
    "tiebreak_rate_diff": new_df["tiebreak_rate_diff"],
    "tiebreak_win_diff": new_df["tiebreak_win_diff"],
    "home_adv_diff": new_df["home_adv_diff"],
    "tiebreak_surface_rate_diff": new_df["tiebreak_surface_rate_diff"],
    "tiebreak_surface_win_diff": new_df["tiebreak_surface_win_diff"],
    "tournament_strength": new_df["tournament_strength"],
    "p1_overall_rd": new_df["p1_overall_rd"],
    "p2_overall_rd": new_df["p2_overall_rd"],
    "p1_surface_rd": new_df["p1_surface_rd"],
    "p2_surface_rd": new_df["p2_surface_rd"],
    "p1_odds_shape_flat": new_df["p1_odds_shape_flat"],
    "p2_odds_shape_flat": new_df["p2_odds_shape_flat"],
    "p1_odds_shape_u": new_df["p1_odds_shape_u"],
    "p2_odds_shape_u": new_df["p2_odds_shape_u"],
    "p1_odds_shape_inv_u": new_df["p1_odds_shape_inv_u"],
    "p2_odds_shape_inv_u": new_df["p2_odds_shape_inv_u"],
    "p1_odds_shape_mixed": new_df["p1_odds_shape_mixed"],
    "p2_odds_shape_mixed": new_df["p2_odds_shape_mixed"],
    "p1_odds_shape_short": new_df["p1_odds_shape_short"],
    "p2_odds_shape_short": new_df["p2_odds_shape_short"],
    "p1_odds_shape_drift": new_df["p1_odds_shape_drift"],
    "p2_odds_shape_drift": new_df["p2_odds_shape_drift"],
    "p1_odds_shape_unknown": new_df["p1_odds_shape_unknown"],
    "p2_odds_shape_unknown": new_df["p2_odds_shape_unknown"],
    "p1_odds_shape_strength": new_df["p1_odds_shape_strength"],
    "p2_odds_shape_strength": new_df["p2_odds_shape_strength"],
    "p1_odds_shortened_fav": new_df["p1_odds_shortened_fav"],
    "p2_odds_shortened_fav": new_df["p2_odds_shortened_fav"],
    "p1_odds_late_money": new_df["p1_odds_late_money"],
    "p2_odds_late_money": new_df["p2_odds_late_money"],
    "p1_was_fav": new_df["p1_was_fav"],
    "p2_was_fav": new_df["p2_was_fav"],
    "p1_was_fav_closing": new_df["p1_was_fav_closing"],
    "p2_was_fav_closing": new_df["p2_was_fav_closing"],
    "p1_became_fav": new_df["p1_became_fav"],
    "p2_became_fav": new_df["p2_became_fav"],
    "p1_lost_fav": new_df["p1_lost_fav"],
    "p2_lost_fav": new_df["p2_lost_fav"],
    "p1_stayed_fav": new_df["p1_stayed_fav"],
    "p2_stayed_fav": new_df["p2_stayed_fav"],
    "odds_volatility_diff": new_df["odds_volatility_diff"],
    "odds_trend_slope_diff": new_df["odds_trend_slope_diff"],
    "odds_max_swing_diff": new_df["odds_max_swing_diff"],
    "odds_early_move_diff": new_df["odds_early_move_diff"],
    "odds_late_move_diff": new_df["odds_late_move_diff"],
    "odds_vol_ratio_diff": new_df["odds_vol_ratio_diff"],
    "odds_net_move_diff": new_df["odds_net_move_diff"],
    "odds_curvature_diff": new_df["odds_curvature_diff"],
    "deltap_1_diff": new_df["deltap_1_diff"],
    "deltap_2_diff": new_df["deltap_2_diff"],
    "deltap_3_diff": new_df["deltap_3_diff"],
    "deltap_4_diff": new_df["deltap_4_diff"],
    "deltap_5_diff": new_df["deltap_5_diff"],
    "deltap_6_diff": new_df["deltap_6_diff"],
    "deltap_7_diff": new_df["deltap_7_diff"],
    "deltap_8_diff": new_df["deltap_8_diff"],
    "deltap_9_diff": new_df["deltap_9_diff"],
    "deltap_10_diff": new_df["deltap_10_diff"]
})

# Write to DB
print("💾 Inserting into database...")
output_df.to_sql(TARGET_TABLE, engine, if_exists="append", index=False, method="multi")
print(f"✅ Wrote {len(output_df)} rows to {TARGET_TABLE}")
